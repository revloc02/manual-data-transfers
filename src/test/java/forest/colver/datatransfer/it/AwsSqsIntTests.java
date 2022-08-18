package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.SqsOperations.sqsConsumeOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsCopy;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDelete;
import static forest.colver.datatransfer.aws.SqsOperations.sqsGetQueueAttributes;
import static forest.colver.datatransfer.aws.SqsOperations.sqsMove;
import static forest.colver.datatransfer.aws.SqsOperations.sqsPurge;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS1;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS2;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStampFormatted;
import static forest.colver.datatransfer.config.Utils.pause;
import static forest.colver.datatransfer.config.Utils.readFile;
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

    // assert the sqs was cleared
    var messages = sqsReadOneMessage(creds, SQS1);
    assertThat(messages.hasMessages()).isFalse();
  }

  @Test
  public void testSqsCopy() {
    LOG.info("Interacting with: sqs={}; sqs={}", SQS1, SQS2);
    // put message on sqs
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    sqsSend(creds, SQS1, payload);

    // verify message is on the sqs
    var fromQResponse = sqsReadOneMessage(creds, SQS1);
    var body = fromQResponse.messages().get(0).body();
    assertThat(body).isEqualTo(payload);

    // copy the message
    pause(4); // waiting for the visibility timeout from the sqsRead()
    sqsCopy(creds, SQS1, SQS2);

    // remove message from source sqs
    sqsDelete(creds, fromQResponse, SQS1);

    // verify the message is on the other sqs
    var toQResponse = sqsReadOneMessage(creds, SQS2);
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
    var payload = "message with payload only, no MessageAttributes";
    sqsSend(creds, SQS1, payload);
    // check that it arrived
    var response = sqsReadOneMessage(creds, SQS1);
    assertThat(response.messages().get(0).body()).isEqualTo(payload);
    // cleanup
    sqsDelete(creds, response, SQS1);
  }

  @Test
  public void testSqsConsume() {
    LOG.info("Interacting with: sqs={}", SQS1);
    // send a message
    var creds = getEmxSbCreds();
    var payload = "message with payload only, no MessageAttributes";
    sqsSend(creds, SQS1, payload);
    // check that it arrived
    var responseGet = sqsReadOneMessage(creds, SQS1);
    assertThat(responseGet.messages().get(0).body()).isEqualTo(payload);
    pause(5);
    // consume the message
    var message = sqsConsumeOneMessage(creds, SQS1);
    assertThat(message.body()).isEqualTo(payload);
    // assert the sqs was cleared
    var messages = sqsReadOneMessage(creds, SQS1);
    assertThat(messages.hasMessages()).isFalse();
  }

  @Test
  public void testSqsSendWithProperties() {
    LOG.info("Interacting with: sqs={}", SQS1);
    // send some stuff
    var creds = getEmxSbCreds();
    var messageProps = Map.of("timestamp", getTimeStampFormatted(), "key2", "value2", "key3",
        "value3");
    var payload = getDefaultPayload();
    sqsSend(creds, SQS1, payload, messageProps);
    // check that it arrived
    var response = sqsReadOneMessage(creds, SQS1);
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
    // put message on sqs
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    sqsSend(creds, SQS1, payload);

    // verify message is on the sqs
    var fromQResponse = sqsReadOneMessage(creds, SQS1);
    var body = fromQResponse.messages().get(0).body();
    assertThat(body).isEqualTo(payload);

    // move the message
    pause(4); // waiting for the visibility timeout from the sqsGet()
    sqsMove(creds, SQS1, SQS2);

    // verify the message is on the other sqs
    var toQResponse = sqsReadOneMessage(creds, SQS2);
    body = toQResponse.messages().get(0).body();
    assertThat(body).isEqualTo(payload);

    // cleanup
    sqsDelete(creds, toQResponse, SQS2);
  }
}
