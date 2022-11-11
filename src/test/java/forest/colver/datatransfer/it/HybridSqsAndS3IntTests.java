package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.S3Operations.s3Delete;
import static forest.colver.datatransfer.aws.S3Operations.s3List;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDepth;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS1;
import static forest.colver.datatransfer.aws.Utils.S3_INTERNAL;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStampFormatted;
import static forest.colver.datatransfer.hybrid.SqsAndS3.moveOneSqsToS3;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Map;
import javax.jms.JMSException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HybridSqsAndS3IntTests {

  private static final Logger LOG = LoggerFactory.getLogger(HybridSqsAndS3IntTests.class);
  private static final String SQS1 = EMX_SANDBOX_TEST_SQS1;

  // todo: run this
  @Test
  public void testMoveOneSqsToS3() throws JMSException {
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

    // cleanup
    s3Delete(creds, S3_INTERNAL, objectKey);
    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(0);
  }

  // todo: this
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

    // assert the SQS was cleared
    var messages = sqsReadOneMessage(creds, SQS1);
    assertThat(messages.hasMessages()).isFalse();

    // cleanup
    s3Delete(creds, S3_INTERNAL, objectKey);
    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(0);

  }
}
