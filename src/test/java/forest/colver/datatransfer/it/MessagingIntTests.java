package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.SqsOperations.sqsDelete;
import static forest.colver.datatransfer.aws.SqsOperations.sqsGet;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS1;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.config.Utils.generateUniqueStrings;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStamp;
import static forest.colver.datatransfer.messaging.DisplayUtils.stringFromMessage;
import static forest.colver.datatransfer.messaging.Environment.PROD;
import static forest.colver.datatransfer.messaging.Environment.STAGE;
import static forest.colver.datatransfer.messaging.JmsBrowse.browseAndCountSpecificMessages;
import static forest.colver.datatransfer.messaging.JmsBrowse.browseForSpecificMessage;
import static forest.colver.datatransfer.messaging.JmsBrowse.copyAllMessages;
import static forest.colver.datatransfer.messaging.JmsBrowse.copyAllMessagesAcrossEnvironments;
import static forest.colver.datatransfer.messaging.JmsBrowse.copySpecificMessages;
import static forest.colver.datatransfer.messaging.JmsBrowse.queueDepth;
import static forest.colver.datatransfer.messaging.JmsConsume.consumeOneMessage;
import static forest.colver.datatransfer.messaging.JmsConsume.consumeSpecificMessage;
import static forest.colver.datatransfer.messaging.JmsConsume.deleteAllMessagesFromQueue;
import static forest.colver.datatransfer.messaging.JmsConsume.deleteAllSpecificMessages;
import static forest.colver.datatransfer.messaging.JmsConsume.deleteSomeMessages;
import static forest.colver.datatransfer.messaging.JmsConsume.moveAllMessages;
import static forest.colver.datatransfer.messaging.JmsConsume.moveAllSpecificMessages;
import static forest.colver.datatransfer.messaging.JmsConsume.moveOneMessage;
import static forest.colver.datatransfer.messaging.JmsConsume.moveSomeSpecificMessages;
import static forest.colver.datatransfer.messaging.JmsConsume.moveSpecificMessage;
import static forest.colver.datatransfer.messaging.JmsConsume.purgeQueue;
import static forest.colver.datatransfer.messaging.JmsSend.createDefaultMessage;
import static forest.colver.datatransfer.messaging.JmsSend.createTextMessage;
import static forest.colver.datatransfer.messaging.JmsSend.sendDefaultMessage;
import static forest.colver.datatransfer.messaging.JmsSend.sendMessageAutoAck;
import static forest.colver.datatransfer.messaging.JmsSend.sendMultipleSameMessage;
import static forest.colver.datatransfer.messaging.JmsSend.sendMultipleUniqueMessages;
import static org.assertj.core.api.Assertions.assertThat;

import forest.colver.datatransfer.Main;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * These are integration tests for standard messaging against Qpid queues.
 */
public class MessagingIntTests {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingIntTests.class);

  public static Message createMessage() {
    var messageProps = Map.of("timestamp", getTimeStamp(), "key2", "value2", "key3", "value3");
    return createTextMessage(getDefaultPayload(), messageProps);
  }

  @Test
  public void testCreateTextMessage() throws JMSException {
    var payload = "this is the payload, yo";
    var messageProps = Map.of("timestamp", getTimeStamp(), "specificKey", "specificValue");
    var msg = createTextMessage(payload, messageProps);

    assertThat(msg).isExactlyInstanceOf(org.apache.qpid.jms.message.JmsTextMessage.class);
    assertThat(msg.getText()).isEqualTo(payload);
    assertThat(msg.getStringProperty("specificKey")).isEqualTo("specificValue");
  }

  @Test
  public void testDefaultSend() throws JMSException {
    var env = STAGE;
    var fromQueueName = "forest-test";
    sendDefaultMessage();
    var message = consumeOneMessage(env, fromQueueName);
    LOG.info(
        "Consumed from Host={} Queue={}, Message->{}",
        env.name(),
        fromQueueName,
        stringFromMessage(message));
    assertThat(((TextMessage) message).getText()).contains("Default Payload");
    assertThat(message.getStringProperty("defaultKey")).isEqualTo("defaultValue");
  }

  @Test
  public void testSendCustomHeaders() throws JMSException {
    var env = STAGE;
    var queueName = "forest-test";
    sendMessageAutoAck(env, queueName, createMessage());
    var message = consumeOneMessage(env, queueName);
    assertThat(((TextMessage) message).getText()).contains("Default Payload");
    assertThat(message.getStringProperty("key2")).isEqualTo("value2");
    assertThat(message.getStringProperty("key3")).isEqualTo("value3");
  }

  @Test
  public void testDeleteAllMessagesFromQueue() {
    var env = STAGE;
    var queueName = "forest-test";
    var numMessages = 300;
    var message = createMessage();
    sendMultipleSameMessage(env, queueName, message, numMessages);

    var deleted = deleteAllMessagesFromQueue(env, queueName);
    assertThat(deleted).isEqualTo(numMessages);
    assertThat(queueDepth(env, queueName)).isEqualTo(0);
  }

  @Test
  public void testPurgeQueue() {
    var env = STAGE;
    var queueName = "forest-test";
    var numMessages = 200;
    var message = createMessage();
    sendMultipleSameMessage(env, queueName, message, numMessages);

    var deleted = purgeQueue(env, queueName);
    assertThat(deleted).isEqualTo(numMessages);
    assertThat(queueDepth(env, queueName)).isEqualTo(0);
  }

  @Test
  public void testMove() {
    var env = STAGE;
    var fromQueueName = "forest-test";
    var toQueueName = "forest-test2";
    var numMessages = 4;
    for (var i = 0; i < numMessages; i++) {
      sendMessageAutoAck(env, fromQueueName, createMessage());
    }
    moveAllMessages(env, fromQueueName, toQueueName);
    var deleted = deleteAllMessagesFromQueue(env, toQueueName);
    assertThat(deleted).isEqualTo(numMessages);
  }

  @Test
  public void testMoveOneMessage() {
    var env = STAGE;
    var fromQueueName = "forest-test";
    var toQueueName = "forest-test2";

    // send a message
    sendDefaultMessage();

    // move the message
    moveOneMessage(env, fromQueueName, toQueueName);

    // check both queues for correct number of messages
    var deleted = deleteAllMessagesFromQueue(env, fromQueueName);
    assertThat(deleted).isEqualTo(0);
    deleted = deleteAllMessagesFromQueue(env, toQueueName);
    assertThat(deleted).isEqualTo(1);
  }

  @Test
  public void testMoveOneSpecificMessage() throws JMSException {
    var env = STAGE;
    var fromQueueName = "forest-test";
    var toQueueName = "forest-test2";

    // place some messages
    var numMessagesFrom = 3;
    sendMultipleSameMessage(env, fromQueueName, createMessage(), numMessagesFrom);

    // now send a specific message
    var messageProps = Map.of("timestamp", getTimeStamp(), "specificKey", "specificValue");
    var numMessagesTo = 1;
    for (var i = 0; i < numMessagesTo; i++) {
      sendMessageAutoAck(env, fromQueueName, createTextMessage(getDefaultPayload(), messageProps));
    }

    // now go move that specific message
    moveSpecificMessage(env, fromQueueName, "specificKey='specificValue'", toQueueName);

    // check the moved message
    var message = consumeOneMessage(STAGE, toQueueName);
    assertThat(((TextMessage) message).getText()).contains("Default Payload");
    assertThat(message.getStringProperty("specificKey")).isEqualTo("specificValue");

    // cleanup
    var deletedFrom = deleteAllMessagesFromQueue(env, fromQueueName);
    assertThat(deletedFrom).isEqualTo(numMessagesFrom);
  }

  @Test
  public void testBrowseForSpecificMessage() throws JMSException {
    var env = STAGE;
    var queueName = "forest-test";

    // place one kind of message
    sendMultipleSameMessage(env, queueName, createMessage(), 3);

    // place some messages of a different kind
    var messageProps = Map.of("timestamp", getTimeStamp(), "specificKey", "specificValue");
    var numMsg = 2;
    for (var i = 0; i < numMsg; i++) {
      sendMessageAutoAck(env, queueName, createTextMessage(getDefaultPayload(), messageProps));
    }

    // check
    var message = browseForSpecificMessage(STAGE, queueName, "specificKey='specificValue'");
    assertThat(((TextMessage) message).getText()).contains("Default Payload");
    assertThat(message.getStringProperty("specificKey")).isEqualTo("specificValue");

    // cleanup
    purgeQueue(env, queueName);
  }

  @Test
  public void testBrowseAndCountSpecificMessages() {
    var env = STAGE;
    var queueName = "forest-test";

    // place one kind of message
    sendMultipleSameMessage(env, queueName, createMessage(), 4);

    // place some messages of a different kind
    var messageProps = Map.of("timestamp", getTimeStamp(), "specificKey", "specificValue");
    var numMsg = 5;
    for (var i = 0; i < numMsg; i++) {
      sendMessageAutoAck(env, queueName, createTextMessage(getDefaultPayload(), messageProps));
    }

    // check browseAndCountSpecificMessages() gets the correct number
    assertThat(
        browseAndCountSpecificMessages(env, queueName, "specificKey='specificValue'")).isEqualTo(
        numMsg);

    // cleanup
    purgeQueue(env, queueName);
  }

  @Test
  public void testMoveSomeSpecificMessages() {
    var env = STAGE;
    var fromQueueName = "forest-test";
    var toQueueName = "forest-test2";

    // send one kind of message
    var numMsgsFrom = 4;
    sendMultipleSameMessage(env, fromQueueName, createMessage(), numMsgsFrom);

    // send some messages of a different kind
    var messageProps = Map.of("timestamp", getTimeStamp(), "specificKey", "specificValue");
    var numMsgsTo = 5;
    for (var i = 0; i < numMsgsTo; i++) {
      sendMessageAutoAck(env, fromQueueName, createTextMessage(getDefaultPayload(), messageProps));
    }

    // move some of one kind of message
    var numMsgsToMove = 2;
    moveSomeSpecificMessages(env, fromQueueName, "specificKey='specificValue'", toQueueName,
        numMsgsToMove);

    // check each queue has the correct number of specific messages after moving some
    assertThat(browseAndCountSpecificMessages(env, fromQueueName,
        "specificKey='specificValue'")).isEqualTo(
        numMsgsTo - numMsgsToMove);
    assertThat(
        browseAndCountSpecificMessages(env, toQueueName, "specificKey='specificValue'")).isEqualTo(
        numMsgsToMove);

    // cleanup and check that the queues have the correct number of messages
    var deletedTo = deleteAllMessagesFromQueue(env, toQueueName);
    assertThat(deletedTo).isEqualTo(numMsgsToMove);
    var deletedFrom = deleteAllMessagesFromQueue(env, fromQueueName);
    assertThat(deletedFrom).isEqualTo(numMsgsFrom + numMsgsTo - numMsgsToMove);
  }

  @Test
  public void testMoveAllSpecificMessages() {
    var env = STAGE;
    var fromQueueName = "forest-test";
    var toQueueName = "forest-test2";

    // send one kind of message
    var numMessagesFrom = 5;
    sendMultipleSameMessage(env, fromQueueName, createMessage(), numMessagesFrom);

    // send some messages of a different kind
    var messageProps = Map.of("timestamp", getTimeStamp(), "specificKey", "specificValue");
    var numMessagesTo = 3;
    for (var i = 0; i < numMessagesTo; i++) {
      sendMessageAutoAck(env, fromQueueName, createTextMessage(getDefaultPayload(), messageProps));
    }

    // move all of one kind of message
    moveAllSpecificMessages(env, fromQueueName, "specificKey='specificValue'", toQueueName);

    // cleanup and check that the queues have the correct number of messages after the move
    var deletedTo = deleteAllMessagesFromQueue(env, toQueueName);
    assertThat(deletedTo).isEqualTo(numMessagesTo);
    var deletedFrom = deleteAllMessagesFromQueue(env, fromQueueName);
    assertThat(deletedFrom).isEqualTo(numMessagesFrom);
  }

  @Test
  public void testMoveAllSpecificMessagesWhenNoneExist() {
    var env = STAGE;
    var fromQueueName = "forest-test";
    var toQueueName = "forest-test2";

    // send one kind of message
    var numMessagesFrom = 5;
    sendMultipleSameMessage(env, fromQueueName, createMessage(), numMessagesFrom);

    // try to move specific messages that don't exist in the queue
    moveAllSpecificMessages(env, fromQueueName, "specificKey='specificValue'", toQueueName);

    // cleanup and check that the queues have the correct number of messages after the move
    var deletedTo = deleteAllMessagesFromQueue(env, toQueueName);
    assertThat(deletedTo).isEqualTo(0);
    var deletedFrom = deleteAllMessagesFromQueue(env, fromQueueName);
    assertThat(deletedFrom).isEqualTo(numMessagesFrom);
  }

  @Test
  public void testConsumeSpecificMessage() throws JMSException {
    // send a message
    sendDefaultMessage();
    var env = STAGE;
    var queue = "forest-test";
    var messageProps = Map.of("timestamp", getTimeStamp(), "specificKey", "specificValue");
    // send a specific message
    sendMessageAutoAck(env, queue, createTextMessage(getDefaultPayload(), messageProps));

    // consume the specific message and check it
    var message = consumeSpecificMessage(env, queue, "specificKey='specificValue'");
    assertThat(message.getStringProperty("specificKey")).isEqualTo("specificValue");

    // ensure that there was one 1 message left
    var deleted = deleteAllMessagesFromQueue(env, queue);
    assertThat(deleted).isEqualTo(1);
  }

  @Test
  public void testSendMultipleMessages() {
    var queueName = "forest-test";
    var num = 14;
    sendMultipleSameMessage(STAGE, queueName, createMessage(), num);
    var deleted = deleteAllMessagesFromQueue(STAGE, queueName);
    assertThat(deleted).isEqualTo(num);
  }

  @Test
  public void testQueueDepth() {
    var env = STAGE;
    var queue = "forest-test2";
    var num = 14;
    sendMultipleSameMessage(env, queue, createDefaultMessage(), num);

    // check the queue depth
    assertThat(queueDepth(STAGE, queue)).isEqualTo(num);

    // cleanup
    var deleted = deleteAllMessagesFromQueue(env, queue);
    assertThat(deleted).isEqualTo(num);
  }

  @Test
  public void testDeleteAllSpecificMessages() {
    var env = STAGE;
    var queue = "forest-test2";
    // send some messages
    var num = 4;
    sendMultipleSameMessage(env, queue, createDefaultMessage(), num);
    // send some specific messages
    var messageProps = Map.of("timestamp", getTimeStamp(), "specificKey", "specificValue");
    var numSpecific = 5;
    sendMultipleSameMessage(env, queue, createTextMessage(getDefaultPayload(), messageProps),
        numSpecific);

    // check the queue depth
    assertThat(queueDepth(STAGE, queue)).isEqualTo(num + numSpecific);

    // delete specific messages and ensure correct number were deleted
    assertThat(deleteAllSpecificMessages(env, queue, "specificKey='specificValue'"))
        .isEqualTo(numSpecific);

    // cleanup
    assertThat(deleteAllMessagesFromQueue(env, queue)).isEqualTo(num);
  }

  @Test
  public void testDeleteSomeMessages() {
    var queueName = "forest-test";
    purgeQueue(STAGE, queueName); // start clean

    var numMsgs = 50;
    var uuids = generateUniqueStrings(numMsgs);
    sendMultipleUniqueMessages(STAGE, queueName, uuids);
    LOG.info("Sent {} messages.", numMsgs);

    var numToDelete = 12;
    deleteSomeMessages(STAGE, queueName, numToDelete);
    // check the queue depth
    assertThat(queueDepth(STAGE, queueName)).isEqualTo(numMsgs - numToDelete);

    deleteSomeMessages(STAGE, queueName, numToDelete);
    // check the queue depth
    assertThat(queueDepth(STAGE, queueName)).isEqualTo(numMsgs - numToDelete - numToDelete);

    purgeQueue(STAGE, queueName); // cleanup
  }

  @Test
  public void testCopyAllMessages() {
    // send some messages
    var env = STAGE;
    var fromQueue = "forest-test";
    var num = 7;
    sendMultipleSameMessage(env, fromQueue, createDefaultMessage(), num);

    // copy the message over
    var toQueue = "forest-test2";
    copyAllMessages(env, fromQueue, toQueue);

    // check the queue depth on the new queue
    assertThat(queueDepth(env, toQueue)).isEqualTo(num);

    // cleanup
    deleteAllMessagesFromQueue(env, fromQueue);
    deleteAllMessagesFromQueue(env, toQueue);
  }

  @Test
  public void testCopySpecificMessages() {
    var env = STAGE;
    var fromQueue = "forest-test2";
    purgeQueue(env, fromQueue);

    // send some messages
    var num = 7;
    sendMultipleSameMessage(env, fromQueue, createDefaultMessage(), num);
    // send some specific messages
    var messageProps = Map.of("timestamp", getTimeStamp(), "specificKey", "specificValue");
    var numSpecific = 5;
    sendMultipleSameMessage(env, fromQueue, createTextMessage(getDefaultPayload(), messageProps),
        numSpecific);

    // copy specific messages over
    var toQueue = "forest-test";
    purgeQueue(env, toQueue);
    copySpecificMessages(STAGE, fromQueue, "specificKey='specificValue'", toQueue);

    // check the queue depth on the new queue
    assertThat(queueDepth(STAGE, toQueue)).isEqualTo(numSpecific);

    // cleanup and ensure correct number of specific messages are deleted
    assertThat(
        deleteAllSpecificMessages(env, toQueue, "specificKey='specificValue'"))
        .isEqualTo(numSpecific);
    assertThat(deleteAllMessagesFromQueue(env, fromQueue)).isEqualTo(num + numSpecific);
  }

  @Test
  public void testCopyAllMessagesAcrossEnvironments() {
    // send some messages
    var fromQueue = "forest-test";
    var num = 7;
    sendMultipleSameMessage(PROD, fromQueue, createDefaultMessage(), num);

    // copy the message over
    var toQueue = "forest-test";
    copyAllMessagesAcrossEnvironments(PROD, fromQueue, STAGE, toQueue);

    // check the queue depth on the new queue
    assertThat(queueDepth(STAGE, toQueue)).isEqualTo(num);

    // cleanup
    deleteAllMessagesFromQueue(PROD, fromQueue);
    deleteAllMessagesFromQueue(STAGE, toQueue);
  }

  /**
   * The goal is to test that the queue allows competing consumers. Sets up a queue with a bunch of
   * unique messages. Then creates a number of threads to consume each of those messages and compare
   * them against the master list of unique messages to ensure everything got consumed correctly.
   */
  @Test
  public void testCompetingConsumer()
      throws ExecutionException, InterruptedException, JMSException {
    var queueName = "forest-test";
    purgeQueue(STAGE, queueName);

    var numMsgs = 500;
    var uuids = generateUniqueStrings(numMsgs);
    sendMultipleUniqueMessages(STAGE, queueName, uuids);
    LOG.info("Sent {} messages.", numMsgs);

    var threads = 50;
    var es = Executors.newFixedThreadPool(threads);
    List<Future<TextMessage>> futuresList = new ArrayList<>();
    for (var task = 0; task < numMsgs; task++) {
      futuresList.add(es.submit(() -> (TextMessage) consumeOneMessage(STAGE, queueName)));
    }
    LOG.info("Tasks submitted: futuresList.size={}", futuresList.size());

    for (Future<TextMessage> future : futuresList) {
      // remember, future.get() blocks execution until the task is complete
      uuids.remove(future.get().getText());
      LOG.info("removed {}", future.get().getText());
    }
    assertThat(uuids.size()).isEqualTo(0);
  }

  /**
   * This tests moving a message from Qpid to AWS SQS.
   */
  @Test
  public void testMoveJmsToSqs() {
    var payload = "this is the payload";
    var messageProps = Map.of("timestamp", getTimeStamp(), "key2", "value2", "key3", "value3");
    var queueName = "forest-test";
    sendMessageAutoAck(STAGE, queueName, createTextMessage(payload, messageProps));

    var creds = getEmxSbCreds();
    Main.moveJmsToSqs(STAGE, queueName, creds, EMX_SANDBOX_TEST_SQS1);

    // check that it arrived
    var response = sqsGet(creds, EMX_SANDBOX_TEST_SQS1);
    assertThat(response.messages().get(0).body()).isEqualTo(payload);
    assertThat(response.messages().get(0).hasMessageAttributes()).isEqualTo(true);
    assertThat(response.messages().get(0).messageAttributes().get("key2").stringValue()).isEqualTo(
        "value2");
    assertThat(response.messages().get(0).messageAttributes().get("key3").stringValue()).isEqualTo(
        "value3");

    // cleanup
    sqsDelete(creds, response, EMX_SANDBOX_TEST_SQS1);
  }

}
