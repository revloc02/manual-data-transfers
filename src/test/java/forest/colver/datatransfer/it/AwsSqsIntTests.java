package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.SqsOperations.sqsClear;
import static forest.colver.datatransfer.aws.SqsOperations.sqsConsumeOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsCopy;
import static forest.colver.datatransfer.aws.SqsOperations.sqsCopyAll;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDeleteMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDeleteMessages;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDeleteMessagesWithPayloadLike;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDepth;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDownloadMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsMove;
import static forest.colver.datatransfer.aws.SqsOperations.sqsMoveAll;
import static forest.colver.datatransfer.aws.SqsOperations.sqsMoveAllVerbose;
import static forest.colver.datatransfer.aws.SqsOperations.sqsMoveMessagesWithPayloadLike;
import static forest.colver.datatransfer.aws.SqsOperations.sqsMoveMessagesWithSelectedAttribute;
import static forest.colver.datatransfer.aws.SqsOperations.sqsPurge;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadMessages;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS1;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS2;
import static forest.colver.datatransfer.aws.Utils.createSqsMessageAttributes;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.config.Utils.deleteFile;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStampFormatted;
import static forest.colver.datatransfer.config.Utils.getUuid;
import static forest.colver.datatransfer.config.Utils.readFile;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sqs.model.Message;

/**
 * Integration Tests for AWS SQS
 */
class AwsSqsIntTests {

  private static final Logger LOG = LoggerFactory.getLogger(AwsSqsIntTests.class);
  private static final String SQS1 = EMX_SANDBOX_TEST_SQS1;
  private static final String SQS2 = EMX_SANDBOX_TEST_SQS2;

  /**
   * A helper method to ensure an SQS queue has been emptied of all messages.
   */
  private void clearSqs(AwsCredentialsProvider creds, String sqs) {
    sqsClear(creds, sqs);
    // assert the sqs was cleared
    await()
        .pollInterval(Duration.ofSeconds(10))
        .atMost(Duration.ofSeconds(120))
        .untilAsserted(() -> assertThat(sqsDepth(creds, sqs)).isZero());
    LOG.info("The queue {} has been cleared.", sqs);
  }

  /**
   * This is actually not a unit test. This is a helper method that allows me to cleanup both SQS1
   * and SQS2 in case other unit tests didn't finish correctly and cleanup after themselves.
   */
  @Test
  void helperPurge() {
    var creds = getEmxSbCreds();

    // log how many messages are on the SQS
    sqsReadMessages(creds, SQS1);
    sqsReadMessages(creds, SQS2);

    sqsPurge(creds, SQS1); // Note: AWS only allows 1 purge per minute for SQS queues
    sqsPurge(creds, SQS2); // Note: AWS only allows 1 purge per minute for SQS queues

    // assert SQS1 was cleared
    var messages1 = sqsReadMessages(creds, SQS1);
    assertThat(messages1.hasMessages()).isFalse();
    // assert SQS2 was cleared
    var messages2 = sqsReadMessages(creds, SQS2);
    assertThat(messages2.hasMessages()).isFalse();
  }

  @Test
  void testSqsPurge() {
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
    await()
        .pollInterval(Duration.ofSeconds(10))
        .atMost(Duration.ofSeconds(120))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isZero());
  }

  @Test
  void testSqsClear() {
    LOG.info("Interacting with: sqs={}", SQS1);
    // place some messages
    var creds = getEmxSbCreds();
    var numMsgs = 5;
    for (var i = 0; i < numMsgs; i++) {
      sqsSend(creds, SQS1, getDefaultPayload());
    }
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(120))
        .until(() -> sqsDepth(creds, SQS1) >= numMsgs);

    sqsClear(creds, SQS1);
    // assert the sqs was cleared
    await()
        .pollInterval(Duration.ofSeconds(10))
        .atMost(Duration.ofSeconds(120))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isZero());
  }

  @Test
  void testSqsDeleteMessages() {
    LOG.info("Interacting with: sqs={}", SQS1);
    // place some messages
    var creds = getEmxSbCreds();
    var numMsgs = 5;
    for (var i = 0; i < numMsgs; i++) {
      sqsSend(creds, SQS1, getDefaultPayload());
    }
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(120))
        .until(() -> sqsDepth(creds, SQS1) >= numMsgs);
    // now delete them
    do {
      var response = sqsReadMessages(creds, SQS1);
      sqsDeleteMessages(creds, SQS1, response);
    } while (sqsDepth(creds, SQS1) > 0);
    // check the SQS is empty
    await()
        .pollInterval(Duration.ofSeconds(10))
        .atMost(Duration.ofSeconds(120))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isZero());
  }

  @Test
  void testSqsCopy() {
    LOG.info("Interacting with: sqs={}; sqs={}", SQS1, SQS2);
    // prep
    var creds = getEmxSbCreds();
    sqsClear(creds, SQS1);
    sqsClear(creds, SQS2);
    // put message on sqs
    var payload = getDefaultPayload();
    sqsSend(creds, SQS1, payload);

    // check that it arrived
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .until(() -> sqsDepth(creds, SQS1) >= 1);

    // copy the message
    sqsCopy(creds, SQS1, SQS2);

    // verify the message is on the other sqs
    var toQMsg = sqsReadOneMessage(creds, SQS2);
    assert toQMsg != null;
    assertThat(toQMsg.body()).isEqualTo(payload);

    // cleanup
    // remove message from source sqs
    sqsDeleteMessage(creds, SQS1, requireNonNull(sqsReadOneMessage(creds, SQS1)));
    // remove message from target sqs
    sqsDeleteMessage(creds, SQS2, toQMsg);
  }

  @Test
  void testSqsSend() {
    LOG.info("Interacting with: sqs={}", SQS1);
    // send a message
    var creds = getEmxSbCreds();
    var payload = "message with payload only, no MessageAttributes";
    sqsSend(creds, SQS1, payload);
    // check that it arrived
    var msg = sqsReadOneMessage(creds, SQS1);
    assert msg != null;
    assertThat(msg.body()).isEqualTo(payload);
    // cleanup
    sqsDeleteMessage(creds, SQS1, msg);
  }

  /**
   * Sends a {@link software.amazon.awssdk.services.sqs.model.Message Message} to an SQS.
   */
  @Test
  void testSqsSendMessage() {
    LOG.info("Interacting with: sqs={}", SQS1);
    var payload = "Message Payload. This message also includes message attributes.";
    var attributes = Map.of("key1", "value1", "key2", "value2");
    var message = Message.builder()
        .body(payload)
        .messageAttributes(createSqsMessageAttributes(attributes))
        .messageId(getUuid())
        .build();
    // send a message
    var creds = getEmxSbCreds();
    sqsSend(creds, SQS1, message);

    // check that it arrived
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .until(() -> sqsDepth(creds, SQS1) >= 1);

    // check the payload
    var msg = sqsReadOneMessage(creds, SQS1);
    assert msg != null;
    assertThat(msg.body()).isEqualTo(payload);

    // cleanup
    sqsDeleteMessage(creds, SQS1, msg);
  }

  @Test
  void testSqsConsume() {
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
    await()
        .pollInterval(Duration.ofSeconds(10))
        .atMost(Duration.ofSeconds(120))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isZero());
  }

  @Test
  void testSqsSendWithProperties() {
    LOG.info("Interacting with: sqs={}", SQS1);
    // send some stuff
    var creds = getEmxSbCreds();
    var messageProps = Map.of("timestamp", getTimeStampFormatted(), "key2", "value2", "key3",
        "value3");
    var payload = getDefaultPayload();
    sqsSend(creds, SQS1, payload, messageProps);

    // check that it arrived
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .until(() -> sqsDepth(creds, SQS1) >= 1);

    // check the payload and properties
    var msg = sqsReadOneMessage(creds, SQS1);
    assert msg != null;
    assertThat(msg.body()).isEqualTo(payload);
    assertThat(msg.hasMessageAttributes()).isTrue();
    assertThat(msg.messageAttributes().get("key2").stringValue()).isEqualTo("value2");
    assertThat(msg.messageAttributes().get("key3").stringValue()).isEqualTo("value3");

    // cleanup
    sqsDeleteMessage(creds, SQS1, msg);
  }

  @Test
  void testSqsMove() {
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
    var toQMsg = sqsReadOneMessage(creds, SQS2);
    assert toQMsg != null;
    assertThat(toQMsg.body()).isEqualTo(payload);

    // cleanup
    sqsDeleteMessage(creds, SQS2, toQMsg);
  }

  @Test
  void testSqsDepth() {
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
    sqsClear(creds, SQS1);
    // assert the sqs was cleared
    await()
        .pollInterval(Duration.ofSeconds(10))
        .atMost(Duration.ofSeconds(120))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isZero());
  }

  @Test
  void testSqsMoveAllVerbose() {
    LOG.info("Interacting with: sqs={}; sqs={}", SQS1, SQS2);
    // put messages on sqs
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    var numMsgs = 12;
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
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS2)).isGreaterThanOrEqualTo(numMsgs));

    // cleanup
    sqsClear(creds, SQS1);
    // assert the sqs was cleared
    await()
        .pollInterval(Duration.ofSeconds(10))
        .atMost(Duration.ofSeconds(120))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isZero());
  }

  @Test
  void testSqsMoveAll() {
    LOG.info("Interacting with: sqs={}; sqs={}", SQS1, SQS2);
    // put messages on sqs
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    var numMsgs = 14;
    for (var i = 0; i < numMsgs; i++) {
      sqsSend(creds, SQS1, payload);
    }

    // verify messages are on the source sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isEqualTo(numMsgs));

    // move the message
    sqsMoveAll(creds, SQS1, SQS2);

    // verify messages are on the target sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS2)).isGreaterThanOrEqualTo(numMsgs));

    // cleanup
    sqsClear(creds, SQS1);
    // assert the sqs was cleared
    await()
        .pollInterval(Duration.ofSeconds(10))
        .atMost(Duration.ofSeconds(120))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isZero());
  }

  @Test
  void testSqsMoveMessagesWithSelectedAttribute() {
    LOG.info("Interacting with: sqs={}; sqs={}", SQS1, SQS2);
    var creds = getEmxSbCreds();
    // Prep: clean the queues
    clearSqs(creds, SQS1);
    clearSqs(creds, SQS2);

    // send the first specific message
    var payload = getDefaultPayload();
    var specificProps = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    sqsSend(creds, SQS1, payload, specificProps);
    // send some generic messages
    var numMsgs = 4;
    for (var i = 0; i < numMsgs; i++) {
      var messageProps = Map.of("timestamp", getTimeStampFormatted(), "key" + i, "value" + i);
      sqsSend(creds, SQS1, payload, messageProps);
    }
    // send additional specific messages
    sqsSend(creds, SQS1, payload, specificProps);
    sqsSend(creds, SQS1, payload, specificProps);

    // verify messages are on the sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isEqualTo(numMsgs + 3));

    // move the specific messages
    assertThat(
        sqsMoveMessagesWithSelectedAttribute(creds, SQS1, "specificKey", "specificValue",
            SQS2)).isEqualTo(3);

    // verify moved messages are on the other sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS2)).isEqualTo(3));

    // assert first sqs has correct number of messages left on it
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isEqualTo(numMsgs));

    // Post: cleanup
    clearSqs(creds, SQS1);
    clearSqs(creds, SQS2);
  }

  @Test
  public void testSqsMoveMessagesWithPayloadLike() {
    LOG.info("Interacting with: sqs={}; sqs={}", SQS1, SQS2);
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    // send some messages
    var numMsgs = 8;
    for (var i = 0; i < numMsgs; i++) {
      sqsSend(creds, SQS1, payload + i);
    }

    // verify messages are on the sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isGreaterThanOrEqualTo(numMsgs));

    // moving only the specific message that has a "2" appended
    assertThat(
        sqsMoveMessagesWithPayloadLike(creds, SQS1, payload + "2", SQS2)).isEqualTo(1);

    // verify moved messages are on the other sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS2)).isGreaterThanOrEqualTo(1));

    // assert first sqs has correct number of messages left on it
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isGreaterThanOrEqualTo(numMsgs - 1));

    // cleanup
    sqsPurge(creds, SQS2);
    sqsPurge(creds, SQS1);
  }

  @Test
  public void testSqsDeleteMessagesWithPayloadLike() {
    LOG.info("Interacting with: sqs={}; sqs={}", SQS1, SQS2);
    var creds = getEmxSbCreds();
    // send some messages with default payload
    var payload = getDefaultPayload();
    var numMsgs1 = 8;
    for (var i = 0; i < numMsgs1; i++) {
      sqsSend(creds, SQS1, payload + i);
    }
    // send some more messages with a specific payload
    var specificPayload = "This is a specific payload.";
    var numMsgs2 = 5;
    for (var i = 0; i < numMsgs2; i++) {
      sqsSend(creds, SQS1, specificPayload);
    }

    // verify messages are on the sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () -> assertThat(sqsDepth(creds, SQS1)).isGreaterThanOrEqualTo(numMsgs1 + numMsgs2));

    // delete the messages with the specific payload
    assertThat(
        sqsDeleteMessagesWithPayloadLike(creds, SQS1, "specific payload")).isEqualTo(numMsgs2);

    // assert first sqs has correct number of messages left on it
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isEqualTo(numMsgs1));

    // cleanup
    sqsClear(creds, SQS2);
    sqsClear(creds, SQS1);
  }

  /**
   * Put more than 100 messages on the first SQS, and then try a
   * {@link
   * forest.colver.datatransfer.aws.SqsOperations#sqsMoveMessagesWithSelectedAttribute(AwsCredentialsProvider,
   * String, String, String, String) sqsMoveMessagesWithSelectedAttribute} and have it fail because
   * the queue is too deep.
   */
  @Test
  public void testSqsMoveSelectedMessagesQueueTooDeep() {
    LOG.info("Interacting with: sqs={}; sqs={}", SQS1, SQS2);
    var creds = getEmxSbCreds();
    // send a specific message
    var specificProps = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    sqsSend(creds, SQS1, getDefaultPayload(), specificProps);
    // send some generic messages
    var numMsgs = 102; // sqsMoveMessagesWithSelectedAttribute() limit is currently hardcoded to 100
    for (var i = 0; i < numMsgs; i++) {
      var messageProps = Map.of("timestamp", getTimeStampFormatted(), "key" + i, "value" + i);
      sqsSend(creds, SQS1, getDefaultPayload(), messageProps);
    }

    // verify messages are on the sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isGreaterThanOrEqualTo(numMsgs + 1));

    // move the specific messages and get a -1 result indicating the queue depth is too big
    assertThat(
        sqsMoveMessagesWithSelectedAttribute(creds, SQS1, "specificKey", "specificValue",
            SQS2)).isEqualTo(-1);

    // cleanup
    sqsPurge(creds, SQS1);
    sqsPurge(creds, SQS2); // just in case, from a previous run
  }

  @Test
  public void testSqsCopyAllMessages() {
    LOG.info("Interacting with: sqs={}; sqs={}", SQS1, SQS2);
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    // send some generic messages
    var numMsgs = 10;
    for (var i = 0; i < numMsgs; i++) {
      var messageProps = Map.of("timestamp", getTimeStampFormatted(), "key" + i, "value" + i);
      sqsSend(creds, SQS1, payload, messageProps);
    }

    // verify starting messages are on the sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isGreaterThanOrEqualTo(numMsgs));

    // copy the messages
    assertThat(
        sqsCopyAll(creds, SQS1, SQS2)).isEqualTo(numMsgs);

    // verify copied messages are on the other sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS2)).isGreaterThanOrEqualTo(numMsgs));

    // assert first sqs has correct number of messages still on it
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isGreaterThanOrEqualTo(numMsgs));

    // cleanup
    sqsPurge(creds, SQS2);
    sqsPurge(creds, SQS1);
  }

  /**
   * This is not a very good test. It would be nice to have sqsReadMessages() guaranteed to read
   * more than 1 message, but I don't know how to do that.
   */
  @Test
  public void testSqsReadMessages() {
    LOG.info("Interacting with: sqs={}", SQS1);
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    // send some messages
    var numMsgs = 6;
    for (var i = 0; i < numMsgs; i++) {
      sqsSend(creds, SQS1, payload);
    }
    // check that they arrived
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isGreaterThanOrEqualTo(numMsgs));
    // assert that 1 or more messages can be read
    assertThat(sqsReadMessages(creds, SQS1).messages().size()).isGreaterThanOrEqualTo(1);
    // now delete them
    do {
      var response = sqsReadMessages(creds, SQS1);
      sqsDeleteMessages(creds, SQS1, response);
    } while (sqsDepth(creds, SQS1) > 0);
    // check the SQS is empty
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isZero());
  }

  @Test
  public void testSqsDeleteMessage() {
    LOG.info("Interacting with: sqs={}", SQS1);
    // send a message
    var creds = getEmxSbCreds();
    var payload = "message with payload only, no MessageAttributes";
    sqsSend(creds, SQS1, payload);
    // check that it arrived
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isOne());
    // now delete
    var message = sqsReadOneMessage(creds, SQS1);
    assert message != null;
    sqsDeleteMessage(creds, SQS1, message);
    // check the SQS is empty
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isZero());
  }

  @Test
  public void testReadOneMessageWhenNoneExists() {
    var creds = getEmxSbCreds();
    var message = sqsReadOneMessage(creds, SQS1);
    assertThat(message).isNull();
  }

  @Test
  public void testConsumeOneMessageWhenNoneExists() {
    var creds = getEmxSbCreds();
    var message = sqsConsumeOneMessage(creds, SQS1);
    assertThat(message).isNull();
  }

  @Test
  public void testSqsDownloadMessage() {
    LOG.info("Interacting with: sqs={}", SQS1);
    var payload = "Message Payload.";
    var message = Message.builder()
        .body(payload)
        .build();
    // send a message
    var creds = getEmxSbCreds();
    sqsSend(creds, SQS1, message);

    // check that it arrived
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .until(() -> sqsDepth(creds, SQS1) >= 1);

    // download the message
    var path = "/Users/revloc02/Downloads/sqs-download-" + getUuid() + ".txt";
    sqsDownloadMessage(creds, SQS1, path);

    // check the payload
    var contents = readFile(path, StandardCharsets.UTF_8);
    assertThat(contents).isEqualTo(payload);

    // cleanup
    var msg = sqsReadMessages(creds, SQS1);
    sqsDeleteMessages(creds, SQS1, msg);
    deleteFile(path);
  }
}
