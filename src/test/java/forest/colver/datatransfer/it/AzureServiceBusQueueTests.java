package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_FOREST_QUEUE;
import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_FOREST_QUEUE2;
import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_FOREST_QUEUE_WITH_DLQ;
import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_FOREST_QUEUE_WITH_FORWARD;
import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_FOREST_TTL_QUEUE;
import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_NAMESPACE;
import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY;
import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY;
import static forest.colver.datatransfer.azure.AzureUtils.buildAsbConnectionString;
import static forest.colver.datatransfer.azure.AzureUtils.createServiceBusMessage;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbConsume;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbCopy;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbCopyAll;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbDlq;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbMove;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbMoveAll;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbQueuePurge;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbRead;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbSend;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.messageCount;
import static forest.colver.datatransfer.config.ConfigUtils.defaultPayload;
import static forest.colver.datatransfer.config.ConfigUtils.getTimeStampFormatted;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AzureServiceBusQueueTests {

  private static final Logger LOG = LoggerFactory.getLogger(AzureServiceBusQueueTests.class);
  private static final String CONN_STR =
      buildAsbConnectionString(
          EMX_SANDBOX_NAMESPACE,
          EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
          EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);

  /** This is simply a convenience method for purging the queues used in other unit tests. */
  @Test
  void purgeQueues() {
    asbQueuePurge(CONN_STR, EMX_SANDBOX_FOREST_QUEUE);
    asbQueuePurge(CONN_STR, EMX_SANDBOX_FOREST_QUEUE2);
  }

  @Test
  void testSend() {
    LOG.info("...send a message...");
    Map<String, Object> properties =
        Map.of("timestamp", getTimeStampFormatted(), "specificKey", "specificValue");
    asbSend(
        CONN_STR, EMX_SANDBOX_FOREST_QUEUE, createServiceBusMessage(defaultPayload, properties));
    LOG.info("...ensure the message arrived...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(1));

    LOG.info("...read that message...");
    var message = asbRead(CONN_STR, EMX_SANDBOX_FOREST_QUEUE).orElseThrow();

    LOG.info("...check the message...");
    var body = message.getBody().toString();
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getApplicationProperties()).containsEntry("specificKey", "specificValue");

    LOG.info("...clean up...");
    asbConsume(CONN_STR, EMX_SANDBOX_FOREST_QUEUE);
  }

  @Test
  void testMove() {
    LOG.info("...send a message...");
    Map<String, Object> properties =
        Map.of("timestamp", getTimeStampFormatted(), "specificKey", "specificValue");
    asbSend(
        CONN_STR, EMX_SANDBOX_FOREST_QUEUE, createServiceBusMessage(defaultPayload, properties));
    LOG.info("...ensure the message arrived...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(1));

    LOG.info("...move that message...");
    asbMove(CONN_STR, EMX_SANDBOX_FOREST_QUEUE, EMX_SANDBOX_FOREST_QUEUE2);
    LOG.info("...ensure the message arrived on the other queue...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE2)).isEqualTo(1));

    LOG.info("...check the message...");
    var message = asbRead(CONN_STR, EMX_SANDBOX_FOREST_QUEUE2).orElseThrow();
    var body = message.getBody().toString();
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getApplicationProperties()).containsEntry("specificKey", "specificValue");

    LOG.info("...ensure there are no messages on the source queue...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE)).isZero());

    LOG.info("...cleanup...");
    asbQueuePurge(CONN_STR, EMX_SANDBOX_FOREST_QUEUE2);
  }

  @Test
  void testConsume() {
    LOG.info("...send a message...");
    Map<String, Object> properties =
        Map.of("timestamp", getTimeStampFormatted(), "specificKey", "specificValue");
    asbSend(
        CONN_STR, EMX_SANDBOX_FOREST_QUEUE, createServiceBusMessage(defaultPayload, properties));
    LOG.info("...ensure the message arrived...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(1));

    LOG.info("...retrieve that message...");
    var message = asbConsume(CONN_STR, EMX_SANDBOX_FOREST_QUEUE).orElseThrow();
    assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE)).isZero();

    LOG.info("...check the message...");
    var body = message.getBody().toString();
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getApplicationProperties()).containsEntry("specificKey", "specificValue");
  }

  @Test
  void testPurge() {
    var message = createServiceBusMessage(defaultPayload);

    LOG.info("...send a bunch of messages...");
    var num = 4;
    for (var i = 0; i < num; i++) {
      asbSend(CONN_STR, EMX_SANDBOX_FOREST_QUEUE, message);
    }
    LOG.info("...ensure the messages arrived...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () ->
                assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE))
                    .isGreaterThanOrEqualTo(num));

    LOG.info("...now purge the queue...");
    assertThat(asbQueuePurge(CONN_STR, EMX_SANDBOX_FOREST_QUEUE)).isGreaterThanOrEqualTo(num);
    LOG.info("...and check that it was purged...");
    assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE)).isZero();
  }

  @Test
  void testDeadLetterQueue() {
    var dlqName = EMX_SANDBOX_FOREST_QUEUE_WITH_DLQ + "/$DeadLetterQueue";

    LOG.info("...send a message to the queue...");
    Map<String, Object> properties =
        Map.of("timestamp", getTimeStampFormatted(), "specificKey", "specificValue");
    asbSend(
        CONN_STR,
        EMX_SANDBOX_FOREST_QUEUE_WITH_DLQ,
        createServiceBusMessage(defaultPayload, properties));
    LOG.info("...ensure the message arrived...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () ->
                assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE_WITH_DLQ)).isEqualTo(1));

    LOG.info("...have the message on the queue move to the DLQ...");
    asbDlq(CONN_STR, EMX_SANDBOX_FOREST_QUEUE_WITH_DLQ);

    LOG.info("...check queue to see if main queue is empty...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE_WITH_DLQ)).isZero());

    LOG.info("...check the DLQ message...");
    var message = asbConsume(CONN_STR, dlqName).orElseThrow();
    var body = message.getBody().toString();
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getApplicationProperties()).containsEntry("specificKey", "specificValue");

    LOG.info("...cleanup the queue and the DLQ...");
    asbQueuePurge(CONN_STR, EMX_SANDBOX_FOREST_QUEUE_WITH_DLQ);
    asbQueuePurge(CONN_STR, dlqName);
  }

  /**
   * This should send a message to the queue and check that it arrived. The queue is configured with
   * a 10 second time-to-live, then the message expires and goes to the Dead-letter sub-queue and
   * the test checks that it arrived there.
   */
  @Test
  void testExpireToDeadLetterQueue() {
    var dlqName = EMX_SANDBOX_FOREST_TTL_QUEUE + "/$DeadLetterQueue";
    asbQueuePurge(CONN_STR, EMX_SANDBOX_FOREST_TTL_QUEUE);
    asbQueuePurge(CONN_STR, dlqName);

    LOG.info("...send a message to the queue...");
    Map<String, Object> properties =
        Map.of("timestamp", getTimeStampFormatted(), "specificKey", "specificValue");
    asbSend(
        CONN_STR,
        EMX_SANDBOX_FOREST_TTL_QUEUE,
        createServiceBusMessage(defaultPayload, properties));

    LOG.info("...check to see the message on the queue...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_TTL_QUEUE)).isEqualTo(1));

    LOG.info(
        "...check queue to see if main queue is empty, indicating that the 10 sec TTL message has expired...");
    await()
        .pollInterval(Duration.ofSeconds(10))
        .atMost(Duration.ofSeconds(120))
        .untilAsserted(
            () -> assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_TTL_QUEUE)).isZero());

    LOG.info("...check the message in the DLQ...");
    var message = asbConsume(CONN_STR, dlqName).orElseThrow();
    var body = message.getBody().toString();
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getApplicationProperties()).containsEntry("specificKey", "specificValue");

    LOG.info("...cleanup the queue and the DLQ...");
    asbQueuePurge(CONN_STR, EMX_SANDBOX_FOREST_TTL_QUEUE);
    asbQueuePurge(CONN_STR, dlqName);
  }

  @Test
  void testSendAutoForwarding() {
    LOG.info("...send a message to the queue...");
    Map<String, Object> properties =
        Map.of("timestamp", getTimeStampFormatted(), "specificKey", "specificValue");
    asbSend(
        CONN_STR,
        EMX_SANDBOX_FOREST_QUEUE_WITH_FORWARD,
        createServiceBusMessage(defaultPayload, properties));

    LOG.info("...check message is not on original queue as it was forwarded...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(12))
        .untilAsserted(
            () ->
                assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE_WITH_FORWARD)).isZero());

    LOG.info("...check the message on the destination queue...");
    var message = asbRead(CONN_STR, EMX_SANDBOX_FOREST_QUEUE2).orElseThrow();
    var body = message.getBody().toString();
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getApplicationProperties()).containsEntry("specificKey", "specificValue");

    LOG.info("...cleanup...");
    asbConsume(CONN_STR, EMX_SANDBOX_FOREST_QUEUE2);
  }

  @Test
  void testMoveAll() {
    LOG.info("...send some messages...");
    var numMsgs = 7;
    for (var i = 0; i < numMsgs; i++) {
      asbSend(CONN_STR, EMX_SANDBOX_FOREST_QUEUE, createServiceBusMessage(defaultPayload));
    }
    LOG.info("...check to see the messages arrived...");
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () -> assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(numMsgs));

    LOG.info("...move the messages...");
    asbMoveAll(CONN_STR, EMX_SANDBOX_FOREST_QUEUE, EMX_SANDBOX_FOREST_QUEUE2);

    LOG.info("...verify messages are on the target queue...");
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () -> assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE2)).isEqualTo(numMsgs));

    LOG.info("...cleanup...");
    asbQueuePurge(CONN_STR, EMX_SANDBOX_FOREST_QUEUE2);
  }

  @Test
  void testCopy() {
    LOG.info("...send a message...");
    Map<String, Object> properties =
        Map.of("timestamp", getTimeStampFormatted(), "specificKey", "specificValue");
    asbSend(
        CONN_STR, EMX_SANDBOX_FOREST_QUEUE, createServiceBusMessage(defaultPayload, properties));
    LOG.info("...ensure the message arrived...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(1));

    LOG.info("...copy that message...");
    asbCopy(CONN_STR, EMX_SANDBOX_FOREST_QUEUE, EMX_SANDBOX_FOREST_QUEUE2);
    LOG.info("...ensure the message arrived on the other queue...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE2)).isEqualTo(1));

    LOG.info("...check the message...");
    var message = asbRead(CONN_STR, EMX_SANDBOX_FOREST_QUEUE2).orElseThrow();
    var body = message.getBody().toString();
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getApplicationProperties()).containsEntry("specificKey", "specificValue");

    LOG.info("...ensure the message is still on the original queue...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(1));

    LOG.info("...cleanup...");
    asbQueuePurge(CONN_STR, EMX_SANDBOX_FOREST_QUEUE2);
    asbQueuePurge(CONN_STR, EMX_SANDBOX_FOREST_QUEUE);
  }

  @Test
  void testCopyAll() {
    LOG.info("...send some messages...");
    var numMsgs = 4;
    for (var i = 0; i < numMsgs; i++) {
      asbSend(CONN_STR, EMX_SANDBOX_FOREST_QUEUE, createServiceBusMessage(defaultPayload));
    }
    LOG.info("...check to see the messages arrived...");
    await()
        .pollInterval(Duration.ofSeconds(5))
        .atMost(Duration.ofSeconds(80))
        .untilAsserted(
            () -> assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(numMsgs));

    LOG.info("...copy the messages...");
    asbCopyAll(CONN_STR, EMX_SANDBOX_FOREST_QUEUE, EMX_SANDBOX_FOREST_QUEUE2);

    LOG.info("...verify messages are on the target queue...");
    await()
        .pollInterval(Duration.ofSeconds(5))
        .atMost(Duration.ofSeconds(80))
        .untilAsserted(
            () -> assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE2)).isEqualTo(numMsgs));

    LOG.info("...ensure the messages are still on the original queue...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(CONN_STR, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(numMsgs));

    LOG.info("...cleanup...");
    asbQueuePurge(CONN_STR, EMX_SANDBOX_FOREST_QUEUE2);
    asbQueuePurge(CONN_STR, EMX_SANDBOX_FOREST_QUEUE);
  }
}
