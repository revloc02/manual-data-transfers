package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.config.Utils.generateUniqueStrings;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStamp;
import static forest.colver.datatransfer.messaging.DisplayUtils.createStringFromMessage;
import static forest.colver.datatransfer.messaging.Environment.STAGE;
import static forest.colver.datatransfer.messaging.JmsBrowse.copyAllMessages;
import static forest.colver.datatransfer.messaging.JmsBrowse.queueDepth;
import static forest.colver.datatransfer.messaging.JmsConsume.consumeOneMessage;
import static forest.colver.datatransfer.messaging.JmsConsume.consumeSpecificMessage;
import static forest.colver.datatransfer.messaging.JmsConsume.deleteAllMessagesFromQueue;
import static forest.colver.datatransfer.messaging.JmsConsume.deleteAllSpecificMessagesFromQueue;
import static forest.colver.datatransfer.messaging.JmsConsume.moveAllMessages;
import static forest.colver.datatransfer.messaging.JmsConsume.moveAllSpecificMessages;
import static forest.colver.datatransfer.messaging.JmsConsume.moveOneMessage;
import static forest.colver.datatransfer.messaging.JmsConsume.moveSpecificMessage;
import static forest.colver.datatransfer.messaging.JmsConsume.purgeQueue;
import static forest.colver.datatransfer.messaging.JmsSend.createDefaultMessage;
import static forest.colver.datatransfer.messaging.JmsSend.createTextMessage;
import static forest.colver.datatransfer.messaging.JmsSend.sendDefaultMessage;
import static forest.colver.datatransfer.messaging.JmsSend.sendMessageAutoAck;
import static forest.colver.datatransfer.messaging.JmsSend.sendMultipleSameMessage;
import static forest.colver.datatransfer.messaging.JmsSend.sendMultipleUniqueMessages;
import static org.assertj.core.api.Assertions.assertThat;

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
        createStringFromMessage(message));
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
    var numMessages = 30; // the purgeQueue method tends to timeout when over 1000 messages
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
  public void testMoveSpecificMessages() {
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

    // only move one kind of message
    moveAllSpecificMessages(env, fromQueueName, "specificKey='specificValue'", toQueueName);

    // check that the queues have the correct number of messages after the move
    var deletedTo = deleteAllMessagesFromQueue(env, toQueueName);
    assertThat(deletedTo).isEqualTo(numMessagesTo);
    var deletedFrom = deleteAllMessagesFromQueue(env, fromQueueName);
    assertThat(deletedFrom).isEqualTo(numMessagesFrom);
  }

  @Test
  public void testMoveOneSpecificMessage() {
    var env = STAGE;
    var fromQueueName = "forest-test";
    var toQueueName = "forest-test2";
    var numMessagesFrom = 3;
    sendMultipleSameMessage(env, fromQueueName, createMessage(), numMessagesFrom);

    var messageProps = Map.of("timestamp", getTimeStamp(), "specificKey", "specificValue");
    var numMessagesTo = 1;
    for (var i = 0; i < numMessagesTo; i++) {
      sendMessageAutoAck(env, fromQueueName, createTextMessage(getDefaultPayload(), messageProps));
    }

    moveSpecificMessage(env, fromQueueName, "specificKey='specificValue'", toQueueName);
    var deletedTo = deleteAllMessagesFromQueue(env, toQueueName);
    assertThat(deletedTo).isEqualTo(numMessagesTo);
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

    // ensure correct number of specific messages are deleted
    assertThat(
        deleteAllSpecificMessagesFromQueue(env, queue, "specificKey='specificValue'"))
        .isEqualTo(numSpecific);

    // cleanup
    assertThat(deleteAllMessagesFromQueue(env, queue)).isEqualTo(num);
  }

  @Test
  public void testCopyAllMessages() {
    // send some messages
    var env = STAGE;
    var fromQueue = "forest-test2";
    var num = 7;
    sendMultipleSameMessage(env, fromQueue, createDefaultMessage(), num);

    // copy the message over
    var toQueue = "skim-forest";
    copyAllMessages(STAGE, fromQueue, toQueue);

    // check the queue depth on the new queue
    assertThat(queueDepth(STAGE, toQueue)).isEqualTo(num);

    // cleanup
    deleteAllMessagesFromQueue(env, fromQueue);
    deleteAllMessagesFromQueue(env, toQueue);
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

  // todo
  @Test
  public void testCopySpecificMessages() {

  }

  private Message createMessage() {
    var messageProps = Map.of("timestamp", getTimeStamp(), "key2", "value2", "key3", "value3");
    return createTextMessage(getDefaultPayload(), messageProps);
  }

}
