package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.ServiceBusOperations.asbReadMessage;
import static forest.colver.datatransfer.azure.ServiceBusOperations.asbReceiveMessageComplete;
import static forest.colver.datatransfer.azure.ServiceBusOperations.asbSendMessageToQueue;
import static forest.colver.datatransfer.azure.ServiceBusOperations.asbSendMessageToTopic;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.connectAsbQ;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.messageCount;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_ASB_FOREST_TEST_QUEUE_CONN_STR;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_ASB_FOREST_TEST_SUB_QUEUE_CONN_STR;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_ASB_FOREST_TEST_SUB_TOPIC_CONN_STR;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY;
import static forest.colver.datatransfer.azure.Utils.createServiceBusMessage;
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

class AzureServiceBusOpsTest {
  private static final Logger LOG = LoggerFactory.getLogger(AzureServiceBusOpsTest.class);
  private static final ConnectionStringBuilder CREDS =
      connectAsbQ(
          EMX_SANDBOX_NAMESPACE,
          EMX_SANDBOX_FOREST_QUEUE,
          EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
          EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);

  @Test
  void testSendReadQueue() {
    LOG.info("...send a message to a queue...");
    Map<String, Object> properties =
        Map.of("timestamp", getTimeStampFormatted(), "specificKey", "specificValue");
    asbSendMessageToQueue(
        EMX_SANDBOX_ASB_FOREST_TEST_QUEUE_CONN_STR,
        EMX_SANDBOX_FOREST_QUEUE,
        createServiceBusMessage(defaultPayload, properties));
    LOG.info("...ensure the message arrived...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(messageCount(CREDS)).isOne());

    LOG.info("...read that message from the queue...");
    var message =
        asbReadMessage(EMX_SANDBOX_ASB_FOREST_TEST_QUEUE_CONN_STR, EMX_SANDBOX_FOREST_QUEUE);

    LOG.info("...check the message...");
    var body = message.getBody().toString();
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getApplicationProperties()).containsEntry("specificKey", "specificValue");

    LOG.info("...clean up the queue...");
    asbReceiveMessageComplete(
        EMX_SANDBOX_ASB_FOREST_TEST_QUEUE_CONN_STR, EMX_SANDBOX_FOREST_QUEUE, message);
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(messageCount(CREDS)).isZero());
  }

  // todo: ...and then possibly read from forest-test-sub-subscription (if it is a durable
  // subscription...I think Azure does those)

  @Test
  void testSendTopicReadQueue() {
    var topic = "forest-test-sub-topic";
    var queue = "forest-test-sub-queue";
    LOG.info("...send a message to a topic with a queue subscription...");
    Map<String, Object> properties =
        Map.of("timestamp", getTimeStampFormatted(), "specificKey", "specificValue");
    asbSendMessageToTopic(
        EMX_SANDBOX_ASB_FOREST_TEST_SUB_TOPIC_CONN_STR,
        topic,
        createServiceBusMessage(defaultPayload, properties));
    LOG.info("...ensure the message arrived in the subscribed queue...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(EMX_SANDBOX_ASB_FOREST_TEST_SUB_QUEUE_CONN_STR)).isOne());

    LOG.info("...read that message from the queue...");
    var message = asbReadMessage(EMX_SANDBOX_ASB_FOREST_TEST_SUB_QUEUE_CONN_STR, queue);

    LOG.info("...check the message...");
    var body = message.getBody().toString();
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getApplicationProperties()).containsEntry("specificKey", "specificValue");

    LOG.info("...clean up the queue...");
    asbReceiveMessageComplete(EMX_SANDBOX_ASB_FOREST_TEST_SUB_QUEUE_CONN_STR, queue, message);
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () ->
                assertThat(messageCount(EMX_SANDBOX_ASB_FOREST_TEST_SUB_QUEUE_CONN_STR)).isZero());
  }
}
