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
   * Azure Service Bus Send message.
   *
   * @param connectionStringBuilder Credentials.
   * @param message The message to send.
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
      if (message != null) {
        iMessageReceiver.completeAsync(message.getLockToken());
      }
    } catch (InterruptedException | ServiceBusException e) {
      e.printStackTrace();
    }
    return message;
  }

  /**
   * Take a message on the queue and send it to the dead-letter sub-queue.
   *
   * @param connectionStringBuilder Credentials.
   */
  public static void asbDlq(ConnectionStringBuilder connectionStringBuilder) {
    try {
      IMessageReceiver iMessageReceiver = ClientFactory.createMessageReceiverFromConnectionStringBuilder(
          connectionStringBuilder, ReceiveMode.PEEKLOCK);
      var message = iMessageReceiver.receive(Duration.ofSeconds(1));
      iMessageReceiver.deadLetterAsync(message.getLockToken());
    } catch (InterruptedException | ServiceBusException e) {
      e.printStackTrace();
    }
  }

  public static void asbMove(ConnectionStringBuilder fromCsb, ConnectionStringBuilder toCsb) {
    asbSend(toCsb, asbConsume(fromCsb));
  }

  public static void asbMoveAll(ConnectionStringBuilder fromCsb, ConnectionStringBuilder toCsb) {
    while (messageCount(fromCsb) > 0) {
      asbSend(toCsb, asbConsume(fromCsb));
    }
  }

  public static void asbCopy(ConnectionStringBuilder fromCsb, ConnectionStringBuilder toCsb) {
    asbSend(toCsb, asbRead(fromCsb));
  }

  // todo: add a copyAll op

  public static int asbPurge(ConnectionStringBuilder connectionStringBuilder) {
    var counter = 0;
    try {
      IMessageReceiver iMessageReceiver = ClientFactory.createMessageReceiverFromConnectionStringBuilder(
          connectionStringBuilder, ReceiveMode.RECEIVEANDDELETE);
      while (iMessageReceiver.peek() != null) {
        var messages = iMessageReceiver.receiveBatch(1);
        LOG.info("asbPurge received {} messages, purging...", messages.size());
        counter += messages.size();
      }
    } catch (ServiceBusException | InterruptedException e) {
      e.printStackTrace();
    }
    LOG.info("Purged {} messages.", counter);
    return counter;
  }

  /**
   * Returns the ActiveMessageCount for the queue.
   *
   * @param connectionStringBuilder Credentials.
   * @return The number of Active messages on the queue.
   */
  public static long messageCount(ConnectionStringBuilder connectionStringBuilder) {
    ManagementClient client = new ManagementClient(connectionStringBuilder);
    long messageCount = -1;
    try {
      var mcd = client.getQueueRuntimeInfo(connectionStringBuilder.getEntityPath())
          .getMessageCountDetails();
      messageCount = mcd.getActiveMessageCount();
      LOG.info(
          "Message Count Details:\n  ActiveMessageCount={}\n  DeadLetterMessageCount={}\n  ScheduledMessageCount={}\n  TransferMessageCount={}\n  TransferDeadLetterMessageCount={}\n",
          mcd.getActiveMessageCount(), mcd.getDeadLetterMessageCount(),
          mcd.getScheduledMessageCount(), mcd.getTransferMessageCount(),
          mcd.getTransferDeadLetterMessageCount());
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
