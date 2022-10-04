package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.SqsOperations.sqsConsumeOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsCopy;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDelete;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDepth;
import static forest.colver.datatransfer.aws.SqsOperations.sqsMove;
import static forest.colver.datatransfer.aws.SqsOperations.sqsMoveAll;
import static forest.colver.datatransfer.aws.SqsOperations.sqsMoveAllVerbose;
import static forest.colver.datatransfer.aws.SqsOperations.sqsPurge;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS1;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS2;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStampFormatted;
import static forest.colver.datatransfer.config.Utils.getUuid;
import static forest.colver.datatransfer.config.Utils.readFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

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
    var numMsgs = 5;
    for (var i = 0; i < numMsgs; i++) {
      sqsSend(
          creds,
          SQS1,
          readFile("src/test/resources/1test.txt", StandardCharsets.UTF_8));
    }
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .until(() -> sqsDepth(creds, SQS1) >= numMsgs);

    sqsPurge(creds, SQS1); // Note: AWS only allows 1 purge per minute for SQS queues

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

    // check that it arrived
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .until(() -> sqsDepth(creds, SQS1) >= 1);

    // copy the message
    sqsCopy(creds, SQS1, SQS2);

    // remove message from source sqs
    var fromQResponse = sqsReadOneMessage(creds, SQS1);
    sqsDelete(creds, fromQResponse, SQS1);

    // verify the message is on the other sqs
    var toQResponse = sqsReadOneMessage(creds, SQS2);
    var body = toQResponse.messages().get(0).body();
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

  /**
   * Sends a {@link software.amazon.awssdk.services.sqs.model.Message Message} to an SQS.
   */
  @Test
  public void testSqsSendMessage() {
    LOG.info("Interacting with: sqs={}", SQS1);
    var payload = "Message Payload. This message also includes message attributes.";
    var value2 = MessageAttributeValue.builder().stringValue("value2").build();
    var messageAttributes = Map.of("key2", value2);
    var message = Message.builder()
        .body(payload)
        .messageAttributes(messageAttributes)
        .messageId(getUuid())
        .build();
    // send some stuff
    var creds = getEmxSbCreds();
    sqsSend(creds, SQS1, message);
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
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .until(() -> sqsDepth(creds, SQS1) >= 1);
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

    // check that it arrived
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .until(() -> sqsDepth(creds, SQS1) >= 1);

    // move the message
    sqsMove(creds, SQS1, SQS2);

    // ensure that the file was moved off of the first SQS
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .until(() -> sqsDepth(creds, SQS1) == 0);

    // verify the message is on the other sqs
    var toQResponse = sqsReadOneMessage(creds, SQS2);
    var body = toQResponse.messages().get(0).body();
    assertThat(body).isEqualTo(payload);

    // cleanup
    sqsDelete(creds, toQResponse, SQS2);
  }

  @Test
  public void testSqsDepth() {
    LOG.info("Interacting with: sqs={}", SQS1);
    // put messages on sqs
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    var numMsgs = 6;
    for (var i = 0; i < numMsgs; i++) {
      sqsSend(creds, SQS1, payload);
    }

    // verify depth is correct
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isEqualTo(numMsgs));

    // cleanup
    sqsPurge(creds, SQS1);
  }

  @Test
  public void testSqsMoveAllVerbose() {
    LOG.info("Interacting with: sqs={}; sqs={}", SQS1, SQS2);
    // put messages on sqs
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    var numMsgs = 14;
    for (var i = 0; i < numMsgs; i++) {
      sqsSend(creds, SQS1, payload);
    }

    // verify message is on the sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isEqualTo(numMsgs));

    // move the message
    sqsMoveAllVerbose(creds, SQS1, SQS2);

    // verify messages are on the sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS2)).isEqualTo(numMsgs));

    // cleanup
    sqsPurge(creds, SQS2);
  }

  @Test
  public void testSqsMoveAll() {
    LOG.info("Interacting with: sqs={}; sqs={}", SQS1, SQS2);
    // put messages on sqs
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    var numMsgs = 14;
    for (var i = 0; i < numMsgs; i++) {
      sqsSend(creds, SQS1, payload);
    }

    // verify messages are on the sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isEqualTo(numMsgs));

    // move the message
    sqsMoveAll(creds, SQS1, SQS2);

    // verify message is on the sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS2)).isEqualTo(numMsgs));

    // cleanup
    sqsPurge(creds, SQS2);
  }

  // todo: Can you access(retrieve) a message from an SQS by its message.ID?
  @Test
  public void testSomeStuff() {
    LOG.info("Interacting with: sqs={}", SQS1);
    // put messages on sqs
    var creds = getEmxSbCreds();
    var numMsgs = 14;
    for (var i = 0; i < numMsgs; i++) {
      sqsSend(creds, SQS1, String.valueOf(i));
    }

    // verify messages are on the sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isGreaterThanOrEqualTo(numMsgs));

    var response = sqsReadOneMessage(creds, SQS1);
    LOG.info("receiptHandle()={}", response.messages().get(0).receiptHandle());
    LOG.info("messageId()={}", response.messages().get(0).messageId());
    LOG.info("body()={}", response.messages().get(0).body());

    // if you have the message.ID then you also have the message itself, and it still exists on the
    // SQS until you delete it with the receiptHandle, which you also have. Thus, accessing a
    // message on the SQS using the message.ID is a moot point.

    // cleanup
    sqsPurge(creds, SQS1);
  }

  @Test
  public void testSqsMoveSelectedMessages() {
    // todo: this
  }
}
