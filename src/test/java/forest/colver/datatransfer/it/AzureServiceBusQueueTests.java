package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbConsume;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbCopy;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbCopyAll;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbDlq;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbMove;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbMoveAll;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbPurge;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbRead;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbSend;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.connectAsbQ;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.messageCount;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE2;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE_WITH_DLQ;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE_WITH_FORWARD;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_TTL_QUEUE;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY;
import static forest.colver.datatransfer.azure.Utils.createIMessage;
import static forest.colver.datatransfer.config.Utils.defaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStampFormatted;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AzureServiceBusQueueTests {

  private static final Logger LOG = LoggerFactory.getLogger(AzureServiceBusQueueTests.class);
  private final ConnectionStringBuilder creds = connectAsbQ(EMX_SANDBOX_NAMESPACE,
      EMX_SANDBOX_FOREST_QUEUE,
      EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);

  /**
   * This is simply a convenience method for purging the queues used in other unit tests.
   */
  @Test
  void purgeQueues() {
    var creds2 = connectAsbQ(EMX_SANDBOX_NAMESPACE, EMX_SANDBOX_FOREST_QUEUE2,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    asbPurge(creds);
    asbPurge(creds2);
  }

  @Test
  void testSend() {
    LOG.info("...send a message...");
    Map<String, Object> properties = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    asbSend(creds, createIMessage(defaultPayload, properties));
    LOG.info("...ensure the message arrived...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(creds)).isEqualTo(1));

    LOG.info("...read that message...");
    var message = asbRead(creds);

    LOG.info("...check the message...");
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getProperties()).containsEntry("specificKey", "specificValue");

    LOG.info("...clean up...");
    asbConsume(creds);
  }

  @Test
  void testMove() {
    LOG.info("...send a message...");
    Map<String, Object> properties = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    asbSend(creds, createIMessage(defaultPayload, properties));
    LOG.info("...ensure the message arrived...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(creds)).isEqualTo(1));

    LOG.info("...move that message...");
    var toCreds = connectAsbQ(EMX_SANDBOX_NAMESPACE, EMX_SANDBOX_FOREST_QUEUE2,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    asbMove(creds, toCreds);
    LOG.info("...ensure the message arrived on the other queue...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(toCreds)).isEqualTo(1));

    LOG.info("...check the message...");
    var message = asbRead(toCreds);
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getProperties()).containsEntry("specificKey", "specificValue");

    LOG.info("...ensure there are no messages on the source queue...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(creds)).isZero());

    LOG.info("...cleanup...");
    asbPurge(toCreds);
  }

  @Test
  void testConsume() {
    LOG.info("...send a message...");
    Map<String, Object> properties = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    asbSend(creds, createIMessage(defaultPayload, properties));
    LOG.info("...ensure the message arrived...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(creds)).isEqualTo(1));

    LOG.info("...retrieve that message...");
    var message = asbConsume(creds);
    assertThat(messageCount(creds)).isZero();

    LOG.info("...check the message...");
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getProperties()).containsEntry("specificKey", "specificValue");
  }

  @Test
  void testPurge() {
    var message = createIMessage(defaultPayload);

    LOG.info("...send a bunch of messages...");
    var num = 4;
    for (var i = 0; i < num; i++) {
      asbSend(creds, message);
    }
    LOG.info("...ensure the messages arrived...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(creds)).isGreaterThanOrEqualTo(
                num));

    LOG.info("...now purge the queue...");
    assertThat(asbPurge(creds)).isGreaterThanOrEqualTo(num);
    LOG.info("...and check that it was purged...");
    assertThat(messageCount(creds)).isZero();
  }

  @Test
  void testDeadLetterQueue() {
    var dlqName = EMX_SANDBOX_FOREST_QUEUE_WITH_DLQ + "/$DeadLetterQueue";
    // connection string to the queue with a DLQ configured
    var credsQwDlq = connectAsbQ(EMX_SANDBOX_NAMESPACE,
        EMX_SANDBOX_FOREST_QUEUE_WITH_DLQ,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    // connection string to the actual DLQ of the queue (with a DLQ configured)
    var credsQwDlq_Dlq = connectAsbQ(EMX_SANDBOX_NAMESPACE, dlqName,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);

    LOG.info("...send a message to the queue...");
    Map<String, Object> properties = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    asbSend(credsQwDlq, createIMessage(defaultPayload, properties));
    LOG.info("...ensure the message arrived...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(credsQwDlq)).isEqualTo(
                1));

    LOG.info("...have the message on the queue move to the DLQ...");
    asbDlq(credsQwDlq);

    LOG.info("...check queue to see if main queue is empty...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(credsQwDlq)).isZero());

    LOG.info("...check the DLQ message...");
    var message = asbConsume(credsQwDlq_Dlq);
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getProperties()).containsEntry("specificKey", "specificValue");

    LOG.info("...cleanup the queue and the DLQ...");
    asbPurge(credsQwDlq);
    asbPurge(credsQwDlq_Dlq);
  }

  /**
   * This should send a message to the queue and check that it arrived. The queue is configured with
   * a 10 second time-to-live, then the message expires and goes to the Dead-letter sub-queue and
   * the test checks that it arrived there.
   */
  @Test
  void testExpireToDeadLetterQueue() {
    var dlqName = EMX_SANDBOX_FOREST_TTL_QUEUE + "/$DeadLetterQueue";
    // connection string to the queue with a DLQ configured
    var credsTtlQueue = connectAsbQ(EMX_SANDBOX_NAMESPACE,
        EMX_SANDBOX_FOREST_TTL_QUEUE,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    // connection string to the actual DLQ of the queue (with a DLQ configured)
    var credsTtlQueueDlq = connectAsbQ(EMX_SANDBOX_NAMESPACE, dlqName,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);

    LOG.info("...send a message to the queue...");
    Map<String, Object> properties = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    asbSend(credsTtlQueue, createIMessage(defaultPayload, properties));

    LOG.info("...check to see the message on the queue...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(credsTtlQueue)).isEqualTo(
                1));

    LOG.info(
        "...check queue to see if main queue is empty, indicating that the 10 sec TTL message has expired...");
    await()
        .pollInterval(Duration.ofSeconds(10))
        .atMost(Duration.ofSeconds(120))
        .untilAsserted(
            () -> assertThat(messageCount(credsTtlQueue)).isZero());

    LOG.info("...check the message in the DLQ...");
    // check the DLQ message
    var message = asbConsume(credsTtlQueueDlq);
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getProperties()).containsEntry("specificKey", "specificValue");

    LOG.info("...cleanup the queue and the DLQ...");
    asbPurge(credsTtlQueue);
    asbPurge(credsTtlQueueDlq);
  }

  @Test
  void testSendAutoForwarding() {
    // connection string to the queue with a forward_to configured
    var credsQueWithForward = connectAsbQ(EMX_SANDBOX_NAMESPACE,
        EMX_SANDBOX_FOREST_QUEUE_WITH_FORWARD,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    var toCreds = connectAsbQ(EMX_SANDBOX_NAMESPACE, EMX_SANDBOX_FOREST_QUEUE2,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);

    LOG.info("...send a message to the queue...");
    Map<String, Object> properties = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    asbSend(credsQueWithForward, createIMessage(defaultPayload, properties));

    LOG.info("...check message is not on original queue as it was forwarded...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(12))
        .untilAsserted(
            () -> assertThat(messageCount(credsQueWithForward)).isZero());

    LOG.info("...check the message on the destination queue...");
    var message = asbRead(toCreds);
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getProperties()).containsEntry("specificKey", "specificValue");

    LOG.info("...cleanup...");
    asbConsume(toCreds);
  }

  @Test
  void testMoveAll() {
    LOG.info("...send some messages...");
    var numMsgs = 7;
    for (var i = 0; i < numMsgs; i++) {
      asbSend(creds, createIMessage(defaultPayload));
    }
    LOG.info("...check to see the messages arrived...");
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () -> assertThat(messageCount(creds)).isEqualTo(numMsgs));

    LOG.info("...move the messages...");
    var toCreds = connectAsbQ(EMX_SANDBOX_NAMESPACE, EMX_SANDBOX_FOREST_QUEUE2,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    asbMoveAll(creds, toCreds);

    LOG.info("...verify messages are on the target queue...");
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () -> assertThat(messageCount(toCreds)).isEqualTo(numMsgs));

    LOG.info("...cleanup...");
    asbPurge(toCreds);
  }

  @Test
  void testCopy() {
    LOG.info("...send a message...");
    Map<String, Object> properties = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    asbSend(creds, createIMessage(defaultPayload, properties));
    LOG.info("...ensure the message arrived...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(creds)).isEqualTo(1));

    LOG.info("...copy that message...");
    var toCreds = connectAsbQ(EMX_SANDBOX_NAMESPACE, EMX_SANDBOX_FOREST_QUEUE2,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    asbCopy(creds, toCreds);
    LOG.info("...ensure the message arrived on the other queue...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(toCreds)).isEqualTo(1));

    LOG.info("...check the message...");
    var message = asbRead(toCreds);
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getProperties()).containsEntry("specificKey", "specificValue");

    LOG.info("...ensure the message is still on the original queue...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(creds)).isEqualTo(1));

    LOG.info("...cleanup...");
    asbPurge(toCreds);
    asbPurge(creds);
  }

  // todo: this is not working yet, I haven't found a strategy to apply the equivalent of a VisibilityTimeout to Azure messages
  @Test
  void testCopyAll() {
    LOG.info("...send some messages...");
    var numMsgs = 4;
    for (var i = 0; i < numMsgs; i++) {
      asbSend(creds, createIMessage(defaultPayload));
    }
    LOG.info("...check to see the messages arrived...");
    await()
        .pollInterval(Duration.ofSeconds(5))
        .atMost(Duration.ofSeconds(80))
        .untilAsserted(
            () -> assertThat(messageCount(creds)).isEqualTo(numMsgs));

    LOG.info("...copy the messages...");
    var toCreds = connectAsbQ(EMX_SANDBOX_NAMESPACE, EMX_SANDBOX_FOREST_QUEUE2,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    asbCopyAll(creds, toCreds);

    LOG.info("...verify messages are on the target queue...");
    await()
        .pollInterval(Duration.ofSeconds(5))
        .atMost(Duration.ofSeconds(80))
        .untilAsserted(
            () -> assertThat(messageCount(toCreds)).isEqualTo(numMsgs));

    LOG.info("...ensure the messages are still on the original queue...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(creds)).isEqualTo(numMsgs));

    LOG.info("...cleanup...");
    asbPurge(toCreds);
    asbPurge(creds);
  }
}
