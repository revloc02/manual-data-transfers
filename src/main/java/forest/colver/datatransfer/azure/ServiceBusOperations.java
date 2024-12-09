package forest.colver.datatransfer.azure;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import java.time.Duration;

/**
 * Operations to interact with Azure Service Bus, both queue and topic. Currently, this is an
 * experiment.
 */
public class ServiceBusOperations {
  private ServiceBusOperations() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

  public static ServiceBusReceivedMessage asbGetMessage(
      String connectionString, String queueName) {
    try (ServiceBusReceiverClient receiver =
        new ServiceBusClientBuilder()
            .connectionString(connectionString)
            .receiver()
            .queueName(queueName)
            .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
            .buildClient()) {
      var message = receiver.receiveMessages(1, Duration.ofSeconds(1)).stream().findFirst();
      return message.orElse(null);
    }
  }
}
