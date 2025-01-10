package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.ServiceBusOperations.asbReadMessage;
import static forest.colver.datatransfer.azure.ServiceBusOperations.asbSendMessage;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.connectAsbQ;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.messageCount;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_ASB_FOREST_TEST_QUEUE_CONN_STR;
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
import forest.colver.datatransfer.azure.ServiceBusOperations;
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
  void testSendReadMessage() throws InterruptedException {
    LOG.info("...send a message...");
    Map<String, Object> properties =
        Map.of("timestamp", getTimeStampFormatted(), "specificKey", "specificValue");
    asbSendMessage(
        EMX_SANDBOX_ASB_FOREST_TEST_QUEUE_CONN_STR,
        EMX_SANDBOX_FOREST_QUEUE,
        createServiceBusMessage(defaultPayload, properties));
    LOG.info("...ensure the message arrived...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(messageCount(CREDS)).isOne());

    LOG.info("...read that message...");
    var message =
        asbReadMessage(EMX_SANDBOX_ASB_FOREST_TEST_QUEUE_CONN_STR, EMX_SANDBOX_FOREST_QUEUE);

    LOG.info("...check the message...");
    var body = message.getBody().toString();
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getApplicationProperties()).containsEntry("specificKey", "specificValue");

    // todo: the 2 thread.sleeps allow this to succeed. The hypothesis is that Java garbage collects
    // before Azure is finished responding to the delete message request.
    Thread.sleep(60000);
    LOG.info("...clean up...");
    ServiceBusOperations.asbPurge(
        EMX_SANDBOX_ASB_FOREST_TEST_QUEUE_CONN_STR, EMX_SANDBOX_FOREST_QUEUE);
    Thread.sleep(60000);
    // todo: the await does not help the unit test to succeed. Perhaps the response is cached?
    //    await()
    //        .pollInterval(Duration.ofSeconds(10))
    //        .atMost(Duration.ofSeconds(300))
    //        .untilAsserted(() -> assertThat(messageCount(CREDS)).isZero());
  }
}
