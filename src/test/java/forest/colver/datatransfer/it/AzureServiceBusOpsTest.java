package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.ServiceBusOperations.asbGetMessage;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbConsume;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbSend;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.connectAsbQ;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.messageCount;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_ASB_FOREST_TEST_QUEUE_CONN_STR;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE;
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

public class AzureServiceBusOpsTest {
  private static final Logger LOG = LoggerFactory.getLogger(AzureServiceBusOpsTest.class);
  private static final ConnectionStringBuilder CREDS =
      connectAsbQ(
          EMX_SANDBOX_NAMESPACE,
          EMX_SANDBOX_FOREST_QUEUE,
          EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
          EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);

  @Test
  void testGetMessage() {
    LOG.info("...send a message...");
    Map<String, Object> properties =
        Map.of("timestamp", getTimeStampFormatted(), "specificKey", "specificValue");
    asbSend(CREDS, createIMessage(defaultPayload, properties));
    LOG.info("...ensure the message arrived...");
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(messageCount(CREDS)).isEqualTo(1));

    LOG.info("...read that message...");
    var message =
        asbGetMessage(EMX_SANDBOX_ASB_FOREST_TEST_QUEUE_CONN_STR, EMX_SANDBOX_FOREST_QUEUE);

    LOG.info("...check the message...");
    var body = message.getBody().toString();
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getApplicationProperties()).containsEntry("specificKey", "specificValue");

    LOG.info("...clean up...");
    asbConsume(CREDS);
  }
}
