package forest.colver.datatransfer.azure;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operations to interact with Azure Service Bus, both queue and topic. Currently, this is an
 * experiment.
 */
public class ServiceBusOperations {
  private ServiceBusOperations() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

  private static final Logger LOG = LoggerFactory.getLogger(ServiceBusOperations.class);

  public static void asbSendMessage(
      String connectionString, String queueName, ServiceBusMessage message) {
    try (ServiceBusSenderClient sender =
        new ServiceBusClientBuilder()
            .connectionString(connectionString)
            .sender()
            .queueName(queueName)
            .buildClient()) {
      sender.sendMessage(message);
    }
    LOG.info("Message sent to queue: {}", queueName);
  }

  /**
   * Read a message from a Service Bus queue.
   *
   * @param connectionString The connection string to the Service Bus namespace.
   * @param queueName The name of the queue.
   * @return The message, or null if no message is available.
   */
  public static ServiceBusReceivedMessage asbGetMessage(String connectionString, String queueName) {
    try (ServiceBusReceiverClient receiver =
        new ServiceBusClientBuilder()
            .connectionString(connectionString)
            .receiver()
            .queueName(queueName)
            .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
            .buildClient()) {
      var message = receiver.receiveMessages(1, Duration.ofSeconds(1)).stream().findFirst();
      LOG.info("Message read from queue: {}", queueName);
      return message.orElse(null);
    }
  }
}
