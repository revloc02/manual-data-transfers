package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.S3Operations.s3Delete;
import static forest.colver.datatransfer.aws.S3Operations.s3Head;
import static forest.colver.datatransfer.aws.S3Operations.s3List;
import static forest.colver.datatransfer.aws.S3Operations.s3Put;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDelete;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDepth;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS1;
import static forest.colver.datatransfer.aws.Utils.S3_INTERNAL;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStampFormatted;
import static forest.colver.datatransfer.config.Utils.readFile;
import static forest.colver.datatransfer.hybrid.SqsAndS3.copyOneSqsToS3;
import static forest.colver.datatransfer.hybrid.SqsAndS3.moveOneSqsToS3;
import static forest.colver.datatransfer.hybrid.SqsAndS3.moveS3ObjectToSqs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class HybridSqsAndS3IntTests {

  private static final Logger LOG = LoggerFactory.getLogger(HybridSqsAndS3IntTests.class);
  private static final String SQS1 = EMX_SANDBOX_TEST_SQS1;

  @Test
  public void testMoveOneSqsToS3() {
    // place a message on SQS
    LOG.info("Interacting with: sqs={}", SQS1);
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    sqsSend(creds, SQS1, payload);

    // check that it arrived
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isEqualTo(1));

    // move it to S3
    var objectKey = "revloc02/source/test/test.txt";
    moveOneSqsToS3(creds, SQS1, S3_INTERNAL, objectKey);

    // check that it arrived
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(1);
    assertThat(objects.get(0).key()).isEqualTo(objectKey);

    // assert the SQS was cleared
    var messages = sqsReadOneMessage(creds, SQS1);
    assertThat(messages.hasMessages()).isFalse();

    // cleanup S3
    s3Delete(creds, S3_INTERNAL, objectKey);
    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(0);
  }

  @Test
  public void testMoveOneWithPropertiesSqsToS3() {
    // place a message on SQS
    LOG.info("Interacting with: sqs={}", SQS1);
    var creds = getEmxSbCreds();
    var messageProps = Map.of("timestamp", getTimeStampFormatted(), "key2", "value2", "key3",
        "value3");
    var payload = getDefaultPayload();
    sqsSend(creds, SQS1, payload, messageProps);

    // check that it arrived
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isEqualTo(1));

    // move it to S3
    var objectKey = "revloc02/source/test/test.txt";
    moveOneSqsToS3(creds, SQS1, S3_INTERNAL, objectKey);

    // check that it arrived
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(1);
    assertThat(objects.get(0).key()).isEqualTo(objectKey);

    // use head to check metadata on S3 object
    var headObjectResponse = s3Head(creds, S3_INTERNAL, objectKey);
    assertThat(headObjectResponse.metadata().get("key2")).isEqualTo("value2");
    assertThat(headObjectResponse.metadata().get("key3")).isEqualTo("value3");

    // assert the SQS was cleared
    var messages = sqsReadOneMessage(creds, SQS1);
    assertThat(messages.hasMessages()).isFalse();

    // cleanup S3
    s3Delete(creds, S3_INTERNAL, objectKey);
    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(0);
  }

  @Test
  public void testCopyOneSqsToS3() {
    // place a message on SQS
    LOG.info("Interacting with: sqs={}", SQS1);
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    sqsSend(creds, SQS1, payload);

    // check that it arrived
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isEqualTo(1));

    // copy it to S3
    var objectKey = "revloc02/source/test/test.txt";
    copyOneSqsToS3(creds, SQS1, S3_INTERNAL, objectKey);

    // check that it arrived
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(1);
    assertThat(objects.get(0).key()).isEqualTo(objectKey);

    // cleanup SQS
    var messages = sqsReadOneMessage(creds, SQS1);
    sqsDelete(creds, messages, SQS1);

    // cleanup S3
    s3Delete(creds, S3_INTERNAL, objectKey);
    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(0);
  }

  @Test
  public void testMoveS3ObjectToSqs() throws IOException {
    // put a file
    var objectKey = "revloc02/source/test/test.txt";
    var creds = getEmxSbCreds();
    var putObjectRequest = PutObjectRequest.builder()
        .bucket(S3_INTERNAL)
        .key(objectKey)
        .build();
    s3Put(creds, getDefaultPayload(), putObjectRequest);

    // verify the file is there
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(1);
    assertThat(objects.get(0).key()).isEqualTo(objectKey);

    // move the file to SQS
    moveS3ObjectToSqs(creds, S3_INTERNAL, objectKey, SQS1);

    // check that it arrived
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .until(() -> sqsDepth(creds, SQS1) >= 1);

    // check the payload
    var response = sqsReadOneMessage(creds, SQS1);
    assertThat(response.messages().get(0).body()).isEqualTo(getDefaultPayload());

    // verify the S3 object is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(0);

    // cleanup
    sqsDelete(creds, response, SQS1);
  }

  @Test
  public void testMoveS3ObjectTooBigToSqs() throws IOException {
    // put a file
    var objectKey = "revloc02/source/test/BoMx1.txt";
    var contents = readFile("src/test/resources/BoMx1.txt", StandardCharsets.UTF_8);
    var creds = getEmxSbCreds();
    var putObjectRequest = PutObjectRequest.builder()
        .bucket(S3_INTERNAL)
        .key(objectKey)
        .build();
    s3Put(creds, contents, putObjectRequest);

    // verify the file is there
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(1);
    assertThat(objects.get(0).key()).isEqualTo(objectKey);

    // move the file to SQS, should log an error because the file is too big
    moveS3ObjectToSqs(creds, S3_INTERNAL, objectKey, SQS1);

    // check that the SQS is, in fact, empty
    assertThat(sqsDepth(creds, SQS1)).isZero();

    // cleanup S3
    s3Delete(creds, S3_INTERNAL, objectKey);
    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(0);
  }
}
