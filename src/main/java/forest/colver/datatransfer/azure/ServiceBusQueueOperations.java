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

  public static IMessageReceiver getReceiver(ConnectionStringBuilder connectionStringBuilder) {
    try {
      return ClientFactory.createMessageReceiverFromConnectionStringBuilder(
          connectionStringBuilder, ReceiveMode.PEEKLOCK);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ServiceBusException e) {
      throw new RuntimeException(e);
    }
  }

  // todo: okay, okay...I have some things to learn about receiving messages.
  // todo: Should I be using PEEKLOCK with .abandon immediately afterwards? Hypothesis: without .abandon() you can't get the message payload.
  // todo: How do I create a message retriever that makes the message unavailable for the default 60 sec

  /**
   * This method reads a message off of the queue, and then immediately abandons the lock on the message and makes it available for any other consumers to access that message.
   */
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

  // todo: I tried this version of asbRead and got a null when I tried to get the message body, so what gives?
  /**
   * This method will read a message, and then it will not be available for any other consumers for 60 sec, as the message is locked by the queue.
   * @param connectionStringBuilder
   * @return
   */
  public static IMessage asbReadWithPeeklock(ConnectionStringBuilder connectionStringBuilder) {
    IMessage message = null;
    try {
      IMessageReceiver iMessageReceiver = ClientFactory.createMessageReceiverFromConnectionStringBuilder(
          connectionStringBuilder, ReceiveMode.PEEKLOCK);
      message = iMessageReceiver.receive(Duration.of(1, ChronoUnit.SECONDS));
    } catch (ServiceBusException | InterruptedException e) {
      e.printStackTrace();
    }
    return message;
  }

  public static IMessage asbConsume(ConnectionStringBuilder connectionStringBuilder) {
    IMessage message = null;
    try {
      IMessageReceiver iMessageReceiver = ClientFactory.createMessageReceiverFromConnectionStringBuilder(
          connectionStringBuilder, ReceiveMode.PEEKLOCK); // PEEKLOCK should be used when the message needs to be processed before it is deleted from the queue.
      message = iMessageReceiver.receive(Duration.ofSeconds(1));
      // todo: why are we doing this null check? Can't we just use ReceiveMode.RECEIVEANDDELETE (see below)? Perhaps there are implications that are not apparent?
      if (message != null) { // ensuring the message is not null before removing it from the queue
        iMessageReceiver.completeAsync(message.getLockToken());
      }
    } catch (InterruptedException | ServiceBusException e) {
      e.printStackTrace();
    }
    return message;
  }

  /**
   * //todo: Will this method work in some cases? In all cases? I need to try it out.
   * // todo: Eventually write this JavaDoc.
   * @param connectionStringBuilder
   * @return
   */
  public static IMessage asbConsumeReceiveAndDelete(ConnectionStringBuilder connectionStringBuilder) {
    IMessage message = null;
    try {
      IMessageReceiver iMessageReceiver = ClientFactory.createMessageReceiverFromConnectionStringBuilder(
          connectionStringBuilder, ReceiveMode.RECEIVEANDDELETE);
      message = iMessageReceiver.receive(Duration.ofSeconds(1));
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
    var receiver = getReceiver(fromCsb);
    IMessage message;
    try {
      message = receiver.receive(Duration.ofSeconds(1));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ServiceBusException e) {
      throw new RuntimeException(e);
    }
    asbSend(toCsb, message);
    receiver.completeAsync(message.getLockToken()); // delete the message
  }

  public static void asbMoveAll(ConnectionStringBuilder fromCsb, ConnectionStringBuilder toCsb) {
    while (messageCount(fromCsb) > 0) {
      // todo: if asbMove get fixed so it is safer, this method should call that one
      asbSend(toCsb, asbConsume(fromCsb));
    }
  }

  public static void asbCopy(ConnectionStringBuilder fromCsb, ConnectionStringBuilder toCsb) {
    asbSend(toCsb, asbRead(fromCsb));
  }

  // todo: this won't work because there needs to be a visibility timeout
  public static void asbCopyAll(ConnectionStringBuilder fromCsb, ConnectionStringBuilder toCsb) {
    while (messageCount(fromCsb) > 0) {
      asbSend(toCsb, asbReadWithPeeklock(fromCsb));
    }
  }

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
