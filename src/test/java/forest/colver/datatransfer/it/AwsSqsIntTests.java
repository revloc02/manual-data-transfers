package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.SqsOperations.sqsCopy;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDelete;
import static forest.colver.datatransfer.aws.SqsOperations.sqsGet;
import static forest.colver.datatransfer.aws.SqsOperations.sqsGetQueueAttributes;
import static forest.colver.datatransfer.aws.SqsOperations.sqsMove;
import static forest.colver.datatransfer.aws.SqsOperations.sqsPurge;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS1;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS2;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStamp;
import static forest.colver.datatransfer.config.Utils.readFile;
import static forest.colver.datatransfer.config.Utils.pause;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration Tests for AWS SQS
 */
public class AwsSqsIntTests {

  private static final Logger LOG = LoggerFactory.getLogger(AwsSqsIntTests.class);
  private static final String SQS1 = EMX_SANDBOX_TEST_SQS1;
  private static final String SQS2 = EMX_SANDBOX_TEST_SQS2;

  @Test
  public void testSqsPurge() {
    LOG.info("Interacting with: sqs={}", SQS1);
    // place some messages
    var creds = getEmxSbCreds();
    for (var i = 0; i < 5; i++) {
      sqsSend(
          creds,
          SQS1,
          readFile("src/test/resources/1test.txt", StandardCharsets.UTF_8));
    }
    pause(1);

    // check that the messages are where we think they are
    var attributes = sqsGetQueueAttributes(creds, SQS1);
    sqsPurge(creds,
        SQS1); // purge before asserting depth in case it's wrong, thus a rerun will work
    assertThat(attributes.attributesAsStrings().get("ApproximateNumberOfMessages")).isEqualTo("5");

    // assert the queue was cleared
    var messages = sqsGet(creds, SQS1);
    assertThat(messages.hasMessages()).isFalse();
  }

  @Test
  public void testSqsCopy() {
    LOG.info("Interacting with: sqs={}; sqs={}", SQS1, SQS2);
    // put message on queue
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    sqsSend(creds, SQS1, payload);

    // verify message is on the queue
    var fromQResponse = sqsGet(creds, SQS1);
    var body = fromQResponse.messages().get(0).body();
    assertThat(body).isEqualTo(payload);

    // copy the message
    pause(4); // waiting for the visibility timeout from the sqsGet()
    sqsCopy(creds, SQS1, SQS2);

    // remove message from source queue
    sqsDelete(creds, fromQResponse, SQS1);

    // verify the message is on the other queue
    var toQResponse = sqsGet(creds, SQS2);
    body = toQResponse.messages().get(0).body();
    assertThat(body).isEqualTo(payload);

    // cleanup
    sqsDelete(creds, toQResponse, SQS2);
  }

  @Test
  public void testSqsSend() {
    LOG.info("Interacting with: sqs={}", SQS1);
    // send some stuff
    var creds = getEmxSbCreds();
    var payload = "message with payload only, no headers";
    sqsSend(creds, SQS1, payload);
    // check that it arrived
    var response = sqsGet(creds, SQS1);
    assertThat(response.messages().get(0).body()).isEqualTo(payload);
    // cleanup
    sqsDelete(creds, response, SQS1);
  }

  @Test
  public void testSqsSendWithProperties() {
    LOG.info("Interacting with: sqs={}", SQS1);
    // send some stuff
    var creds = getEmxSbCreds();
    var messageProps = Map.of("timestamp", getTimeStamp(), "key2", "value2", "key3", "value3");
    var payload = getDefaultPayload();
    sqsSend(creds, SQS1, payload, messageProps);
    // check that it arrived
    var response = sqsGet(creds, SQS1);
    assertThat(response.messages().get(0).body()).isEqualTo(payload);
    assertThat(response.messages().get(0).hasMessageAttributes()).isEqualTo(true);
    assertThat(response.messages().get(0).messageAttributes().get("key2").stringValue()).isEqualTo(
        "value2");
    assertThat(response.messages().get(0).messageAttributes().get("key3").stringValue()).isEqualTo(
        "value3");

    // cleanup
    sqsDelete(creds, response, SQS1);
  }

  @Test
  public void testSqsMove() {
    LOG.info("Interacting with: sqs={}; sqs={}", SQS1, SQS2);
    // put message on queue
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    sqsSend(creds, SQS1, payload);

    // verify message is on the queue
    var fromQResponse = sqsGet(creds, SQS1);
    var body = fromQResponse.messages().get(0).body();
    assertThat(body).isEqualTo(payload);

    // move the message
    pause(4); // waiting for the visibility timeout from the sqsGet()
    sqsMove(creds, SQS1, SQS2);

    // verify the message is on the other queue
    var toQResponse = sqsGet(creds, SQS2);
    body = toQResponse.messages().get(0).body();
    assertThat(body).isEqualTo(payload);

    // cleanup
    sqsDelete(creds, toQResponse, SQS2);

  }
}
