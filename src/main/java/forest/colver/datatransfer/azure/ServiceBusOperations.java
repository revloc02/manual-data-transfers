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
 * experiment, but I think this will end up being the better way to interact with Azure Service Bus.
 */
public class ServiceBusOperations {
  private ServiceBusOperations() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

  private static final Logger LOG = LoggerFactory.getLogger(ServiceBusOperations.class);

  public static void asbSendMessageToQueue(
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
   * Send a message to a Service Bus topic. (I've noticed that this can be used to send messages to
   * a queue as well, if you send in the correct connection string and queue name.)
   */
  public static void asbSendMessageToTopic(
      String connectionString, String topicName, ServiceBusMessage message) {
    try (ServiceBusSenderClient sender =
        new ServiceBusClientBuilder()
            .connectionString(connectionString)
            .sender()
            .topicName(topicName)
            .buildClient()) {
      sender.sendMessage(message);
    }
    LOG.info("Message sent to topic: {}", topicName);
  }

  /**
   * Read a message from a Service Bus queue.
   *
   * @param connectionString The connection string to the Service Bus namespace.
   * @param queueName The name of the queue.
   * @return The message, or null if no message is available.
   */
  public static ServiceBusReceivedMessage asbReadMessage(
      String connectionString, String queueName) {
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

  /**
   * Delete a message. A message received using PEEK_LOCK must be completed to remove it from the
   * queue.
   */
  public static void asbReceiveMessageComplete(
      String connectionString, String queueName, ServiceBusReceivedMessage message) {
    try (ServiceBusReceiverClient receiver =
        new ServiceBusClientBuilder()
            .connectionString(connectionString)
            .receiver()
            .queueName(queueName)
            .buildClient()) {
      receiver.complete(message);
      LOG.info("Message {} removed from queue: {}", message.getMessageId(), queueName);
    }
  }

  /**
   * Use asbReceiveMessageComplete to delete messages if possible as this method will not target
   * messages that have been locked by PEEK_LOCK.
   */
  public static long asbPurge(String connectionString, String queueName) {
    long counter = 0;
    try (ServiceBusReceiverClient receiver =
        new ServiceBusClientBuilder()
            .connectionString(connectionString)
            .receiver()
            .queueName(queueName)
            .receiveMode(ServiceBusReceiveMode.RECEIVE_AND_DELETE)
            .buildClient()) {
      while (receiver.peekMessage() != null) {
        var messages = receiver.receiveMessages(10, Duration.ofSeconds(1));
        if (messages != null && messages.stream().findAny().isPresent()) {
          long messageCount = messages.stream().count();
          LOG.info("asbPurge received {} messages, purging...", messageCount);
          counter += messageCount;
        }
      }
    }
    LOG.info("asbPurge purged {} messages from queue: {}", counter, queueName);
    return counter;
  }
}
