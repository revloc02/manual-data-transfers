package forest.colver.datatransfer.azure;

import com.microsoft.azure.servicebus.ClientFactory;
import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.IMessageReceiver;
import com.microsoft.azure.servicebus.IMessageSender;
import com.microsoft.azure.servicebus.ReceiveMode;
import com.microsoft.azure.servicebus.management.ManagementClient;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceBusQueueOperations {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceBusQueueOperations.class);

  /**
   * Azure Service Bus Send message
   * @param connectionStringBuilder
   * @param message
   */
  public static void asbSend(ConnectionStringBuilder connectionStringBuilder, IMessage message) {
    try {
      IMessageSender iMessageSender = ClientFactory.createMessageSenderFromConnectionStringBuilder(
          connectionStringBuilder);
      iMessageSender.send(message);
    } catch (InterruptedException | ServiceBusException e) {
      e.printStackTrace();
    }
  }

  public static IMessage asbRead(ConnectionStringBuilder connectionStringBuilder) {
    IMessage message = null;
    try {
      IMessageReceiver iMessageReceiver = ClientFactory.createMessageReceiverFromConnectionStringBuilder(
          connectionStringBuilder, ReceiveMode.PEEKLOCK);
      message = iMessageReceiver.receive(Duration.of(1, ChronoUnit.SECONDS));
      iMessageReceiver.abandon(
          message.getLockToken()); // make message available for other consumers
    } catch (InterruptedException | ServiceBusException e) {
      e.printStackTrace();
    }
    return message;
  }

  public static IMessage asbConsume(ConnectionStringBuilder connectionStringBuilder) {
    IMessage message = null;
    try {
      IMessageReceiver iMessageReceiver = ClientFactory.createMessageReceiverFromConnectionStringBuilder(
          connectionStringBuilder, ReceiveMode.PEEKLOCK);
      message = iMessageReceiver.receive(Duration.ofSeconds(1));
      iMessageReceiver.completeAsync(message.getLockToken());
    } catch (InterruptedException | ServiceBusException e) {
      e.printStackTrace();
    }
    return message;
  }

  public static void asbMove(ConnectionStringBuilder fromCsb, ConnectionStringBuilder toCsb)
      throws ServiceBusException, InterruptedException {
    asbSend(toCsb, asbConsume(fromCsb));
  }

  public static int asbPurge(ConnectionStringBuilder connectionStringBuilder) {
    var counter = 0;
    try {
      IMessageReceiver iMessageReceiver = ClientFactory.createMessageReceiverFromConnectionStringBuilder(
          connectionStringBuilder, ReceiveMode.RECEIVEANDDELETE);
      while (iMessageReceiver.peek() != null) {
        var messages = iMessageReceiver.receiveBatch(300);
        LOG.info("asbPurge received {} messages, purging...", messages.size());
        counter += messages.size();
      }
    } catch (ServiceBusException | InterruptedException e) {
      e.printStackTrace();
    }
    LOG.info("Purged {} messages.", counter);
    return counter;
  }

  public static long messageCount(ConnectionStringBuilder connectionStringBuilder,
      String queueName) {
    ManagementClient client = new ManagementClient(connectionStringBuilder);
    long messageCount = -1;
    try {
      var queue = client.getQueueRuntimeInfo(queueName);
      messageCount = queue.getMessageCount();
    } catch (ServiceBusException | InterruptedException e) {
      e.printStackTrace();
    }
    return messageCount;
  }

  public static ConnectionStringBuilder connect(URI endPoint, String entityPath,
      String sharedAccessKeyName, String sharedAccessKey) {
    return new ConnectionStringBuilder(endPoint, entityPath, sharedAccessKeyName, sharedAccessKey);
  }
}
