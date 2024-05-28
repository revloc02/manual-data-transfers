package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbConsume;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbDlq;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbMove;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbMoveAll;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbPurge;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbRead;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbSend;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.connect;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.messageCount;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE2;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE_W_DLQ;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE_W_FORWARD;
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
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AzureServiceBusQueueTests {

  private static final Logger LOG = LoggerFactory.getLogger(AzureServiceBusQueueTests.class);
  private final ConnectionStringBuilder creds = connect(EMX_SANDBOX_NAMESPACE,
      EMX_SANDBOX_FOREST_QUEUE,
      EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);

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
  void testMove() throws ServiceBusException, InterruptedException {
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
    var toCreds = connect(EMX_SANDBOX_NAMESPACE, EMX_SANDBOX_FOREST_QUEUE2,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    asbMove(creds, toCreds);
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

    LOG.info("...clean up...");
    asbPurge(toCreds);
    asbPurge(creds);
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
    var dlqName = EMX_SANDBOX_FOREST_QUEUE_W_DLQ + "/$DeadLetterQueue";
    // connection string to the queue with a DLQ configured
    var credsQwDlq = connect(EMX_SANDBOX_NAMESPACE,
        EMX_SANDBOX_FOREST_QUEUE_W_DLQ,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    // connection string to the actual DLQ of the queue (with a DLQ configured)
    var credsQwDlq_Dlq = connect(EMX_SANDBOX_NAMESPACE, dlqName,
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
    var credsTtlQueue = connect(EMX_SANDBOX_NAMESPACE,
        EMX_SANDBOX_FOREST_TTL_QUEUE,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    // connection string to the actual DLQ of the queue (with a DLQ configured)
    var credsTtlQueueDlq = connect(EMX_SANDBOX_NAMESPACE, dlqName,
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

    LOG.info("...check queue to see if main queue is empty, indicating that the 10 sec TTL message has expired...");
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
  public void testSendAutoForwarding() {
    // connection string to the queue with a forward_to configured
    var credsQwForward = connect(EMX_SANDBOX_NAMESPACE,
        EMX_SANDBOX_FOREST_QUEUE_W_FORWARD,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    var toCreds = connect(EMX_SANDBOX_NAMESPACE, EMX_SANDBOX_FOREST_QUEUE2,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);

    // send a message
    Map<String, Object> properties = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    asbSend(credsQwForward, createIMessage(defaultPayload, properties));

    // check message is not on original queue as it was forwarded
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(12))
        .untilAsserted(
            () -> assertThat(messageCount(credsQwForward)).isEqualTo(0));

    // read the message on the other queue
    var message = asbRead(toCreds);

    // check it
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getProperties().get("specificKey")).isEqualTo("specificValue");

    // clean up
    asbConsume(toCreds);
  }

  @Test
  public void testMoveAll() {
    // send messages
    var numMsgs = 7;
    for (var i = 0; i < numMsgs; i++) {
      asbSend(creds, createIMessage(defaultPayload));
    }
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () -> assertThat(messageCount(creds)).isEqualTo(numMsgs));

    // move messages
    var toCreds = connect(EMX_SANDBOX_NAMESPACE, EMX_SANDBOX_FOREST_QUEUE2,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    asbMoveAll(creds, toCreds);

    // verify messages are on the target queue
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () -> assertThat(messageCount(toCreds)).isEqualTo(numMsgs));

    // clean up
    asbPurge(toCreds);
  }
}
