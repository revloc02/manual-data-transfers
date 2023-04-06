package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.SqsOperations.sqsConsumeOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsCopy;
import static forest.colver.datatransfer.aws.SqsOperations.sqsCopyAll;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDeleteMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDeleteMessageList;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDeleteMessages;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDepth;
import static forest.colver.datatransfer.aws.SqsOperations.sqsGetMessageList;
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
import static forest.colver.datatransfer.aws.Utils.getSqsClient;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStampFormatted;
import static forest.colver.datatransfer.config.Utils.getUuid;
import static forest.colver.datatransfer.config.Utils.readFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sqs.model.Message;

/**
 * Integration Tests for AWS SQS
 */
public class AwsSqsIntTests {

  private static final Logger LOG = LoggerFactory.getLogger(AwsSqsIntTests.class);
  private static final String SQS1 = EMX_SANDBOX_TEST_SQS1;
  private static final String SQS2 = EMX_SANDBOX_TEST_SQS2;

  /**
   * This is actually not a unit test. This is a helper method that allows me to cleanup both SQS1
   * and SQS2 in case other unit tests didn't finish correctly and cleanup after themselves.
   */
  @Test
  public void helperPurge() {
    var creds = getEmxSbCreds();
    sqsPurge(creds, SQS1); // Note: AWS only allows 1 purge per minute for SQS queues
    sqsPurge(creds, SQS2); // Note: AWS only allows 1 purge per minute for SQS queues

    // assert SQS1 was cleared
    var messages1 = sqsReadMessages(creds, SQS1);
    assertThat(messages1.hasMessages()).isFalse();
    // assert SQS2 was cleared
    var messages2 = sqsReadMessages(creds, SQS1);
    assertThat(messages2.hasMessages()).isFalse();
  }

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
    var messages = sqsReadMessages(creds, SQS1);
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

    // verify the message is on the other sqs
    var toQMsg = sqsReadOneMessage(creds, SQS2);
    assertThat(toQMsg.body()).isEqualTo(payload);

    // cleanup
    // remove message from source sqs
    sqsDeleteMessage(creds, SQS1, sqsReadOneMessage(creds, SQS1));
    // remove message from target sqs
    sqsDeleteMessage(creds, SQS2, toQMsg);
  }

  @Test
  public void testSqsSend() {
    LOG.info("Interacting with: sqs={}", SQS1);
    // send a message
    var creds = getEmxSbCreds();
    var payload = "message with payload only, no MessageAttributes";
    sqsSend(creds, SQS1, payload);
    // check that it arrived
    var msg = sqsReadOneMessage(creds, SQS1);
    assertThat(msg.body()).isEqualTo(payload);
    // cleanup
    sqsDeleteMessage(creds, SQS1, msg);
  }

  /**
   * Sends a {@link software.amazon.awssdk.services.sqs.model.Message Message} to an SQS.
   */
  @Test
  public void testSqsSendMessage() {
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
    assertThat(msg.body()).isEqualTo(payload);

    // cleanup
    sqsDeleteMessage(creds, SQS1, msg);
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
    var messages = sqsReadMessages(creds, SQS1);
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
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .until(() -> sqsDepth(creds, SQS1) >= 1);

    // check the payload and properties
    var msg = sqsReadOneMessage(creds, SQS1);
    assertThat(msg.body()).isEqualTo(payload);
    assertThat(msg.hasMessageAttributes()).isEqualTo(true);
    assertThat(msg.messageAttributes().get("key2").stringValue()).isEqualTo("value2");
    assertThat(msg.messageAttributes().get("key3").stringValue()).isEqualTo("value3");

    // cleanup
    sqsDeleteMessage(creds, SQS1, msg);
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
    var toQMsg = sqsReadOneMessage(creds, SQS2);
    assertThat(toQMsg.body()).isEqualTo(payload);

    // cleanup
    sqsDeleteMessage(creds, SQS2, toQMsg);
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

  @Test
  public void testSqsMoveMessagesWithSelectedAttribute() {
    LOG.info("Interacting with: sqs={}; sqs={}", SQS1, SQS2);
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    // send a specific message
    var specificProps = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    sqsSend(creds, SQS1, payload, specificProps);
    // send some generic messages
    var numMsgs = 2;
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
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isGreaterThanOrEqualTo(numMsgs + 3));

    // move the specific messages
    assertThat(
        sqsMoveMessagesWithSelectedAttribute(creds, SQS1, "specificKey", "specificValue",
            SQS2)).isEqualTo(3);

    // verify moved messages are on the other sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS2)).isGreaterThanOrEqualTo(3));

    // assert first sqs has correct number of messages left on it
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isGreaterThanOrEqualTo(numMsgs));

    // cleanup
    sqsPurge(creds, SQS2);
    sqsPurge(creds, SQS1);
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
  public void testSqsDeleteMessageList() {
    LOG.info("Interacting with: sqs={}", SQS1);
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    List<Message> messageList = new ArrayList<>();
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
    // read each message setting an appropriate visibilityTimeout so we can get through the whole queue
    // then wait for all of the visibilityTimeouts to lapse
    // then delete the list

    // below this is all old code, should be deleted
    for (var i = 0; i < numMsgs; i++) {
      // send a message
      sqsSend(creds, SQS1, payload);
      // check that it arrived
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isOne());
      // now pick up that message
      // todo: this might even be dumber than the last idea. If I consume the message, it's gone and there is no need to delete it. So my premise is still off, continue to fix it.
      messageList.add(sqsConsumeOneMessage(creds, SQS1));
      // check the SQS is empty
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isZero());
    }
    // todo: I'm done working on this for today, still revamping my paradigm (premise), commented out the problem here
    try (var sqsClient = getSqsClient(creds)) {
      // now delete them
    sqsDeleteMessageList(sqsClient, SQS1, messageList); // todo: wants a client, so what am I going to do?
    }
    // check the SQS is empty
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isZero());
  }

  @Test
  public void testSqsReadMessageList() {
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
    LOG.info("ARRIVED");
    // now read them
    try (var sqsClient = getSqsClient(creds)) {
      // ok the default visibilityTimeout, which is being used in sqsGetMessageList(), is 30 sec. So the pollInterval on the await() needs to be greater than 30 sec. What was happening was each individual message was being read as it became available, but then it would become unavailable because of the visibilityTimeout. Never were all the of the messages being read at once.
      // the above was going on, now I have no idea what's going on. It read 2 messages at a time, so never more than 6.
      await()
          .pollInterval(Duration.ofSeconds(40))
          .atMost(Duration.ofSeconds(140))
          .untilAsserted(
              () -> assertThat(sqsGetMessageList(sqsClient, SQS1).size()).isGreaterThanOrEqualTo(
                  numMsgs));
      LOG.info("READED");
//      do {
      var list = sqsGetMessageList(sqsClient, SQS1);
      sqsDeleteMessageList(sqsClient, SQS1, list);
//      } while (sqsDepth(creds, SQS1) > 0);
      LOG.info("DELETED");
      // check the SQS is empty
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isZero());
    }
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
//    Throwable thrown = catchThrowable(() -> {
//      var message = sqsReadOneMessage(creds, SQS1);
//    });
//    assertThat(thrown)
//        .isInstanceOf(RuntimeException.class)
//        .hasMessageContaining("has NO messages.");
    var message = sqsReadOneMessage(creds, SQS1);
    assertThat(message).isNull();
  }

  @Test
  public void testConsumeOneMessageWhenNoneExists() {
    var creds = getEmxSbCreds();
//    Throwable thrown = catchThrowable(() -> {
//      var message = sqsReadOneMessage(creds, SQS1);
//    });
//    assertThat(thrown)
//        .isInstanceOf(RuntimeException.class)
//        .hasMessageContaining("has NO messages.");
    var message = sqsConsumeOneMessage(creds, SQS1);
    assertThat(message).isNull();
  }
}
