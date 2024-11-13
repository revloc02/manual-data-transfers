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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceBusQueueOperations {

  private ServiceBusQueueOperations() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

  private static final Logger LOG = LoggerFactory.getLogger(ServiceBusQueueOperations.class);

  /**
   * Azure Service Bus Send message.
   *
   * @param connectionStringBuilder Credentials.
   * @param message The message to send.
   */
  public static void asbSend(ConnectionStringBuilder connectionStringBuilder, IMessage message) {
    try {
      IMessageSender iMessageSender =
          ClientFactory.createMessageSenderFromConnectionStringBuilder(connectionStringBuilder);
      iMessageSender.send(message);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("An error occurred in asbSend: {}", e.getMessage(), e);
    } catch (ServiceBusException e) {
      LOG.error("An error occurred in asbSend: {}", e.getMessage(), e);
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
  // todo: Should I be using PEEKLOCK with .abandon immediately afterwards? Hypothesis: without
  // .abandon() you can't get the message payload.
  // todo: How do I create a message retriever that makes the message unavailable for the default 60
  // sec

  /**
   * This method reads a message off of the queue, and then immediately abandons the lock on the
   * message and makes it available for any other consumers to access that message.
   */
  public static IMessage asbRead(ConnectionStringBuilder connectionStringBuilder) {
    IMessage message = null;
    try {
      IMessageReceiver iMessageReceiver =
          ClientFactory.createMessageReceiverFromConnectionStringBuilder(
              connectionStringBuilder, ReceiveMode.PEEKLOCK);
      message = iMessageReceiver.receive(Duration.ofSeconds(1));
      iMessageReceiver.abandon(
          message.getLockToken()); // make message available for other consumers
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("An error occurred in asbRead: {}", e.getMessage(), e);
    } catch (ServiceBusException e) {
      LOG.error("An error occurred in asbRead: {}", e.getMessage(), e);
    }
    return message;
  }

  // todo: I tried this version of asbRead and got a null when I tried to get the message body, so
  // what gives? Works with RECEIVEANDDELETE, Azure is stupid.

  /**
   * This method will read a message, and then it will not be available for any other consumers for
   * 60 sec, as the message is locked by the queue.
   */
  public static IMessage asbReadWithPeeklock(ConnectionStringBuilder connectionStringBuilder) {
    IMessage message = null;
    try {
      IMessageReceiver iMessageReceiver =
          ClientFactory.createMessageReceiverFromConnectionStringBuilder(
              connectionStringBuilder, ReceiveMode.PEEKLOCK);
      message = iMessageReceiver.receive(Duration.ofSeconds(1));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("An error occurred in asbReadWithPeeklock: {}", e.getMessage(), e);
    } catch (ServiceBusException e) {
      LOG.error("An error occurred in asbReadWithPeeklock: {}", e.getMessage(), e);
    }
    return message;
  }

  /**
   * Consumes a message from an ASB queue. Uses ReceiveMode.RECEIVEANDDELETE which immediately
   * deletes the message after retrieving it--simply consuming the message without any thought of
   * processing it first.
   */
  public static IMessage asbConsume(ConnectionStringBuilder connectionStringBuilder) {
    IMessage message = null;
    try {
      IMessageReceiver iMessageReceiver =
          ClientFactory.createMessageReceiverFromConnectionStringBuilder(
              connectionStringBuilder, ReceiveMode.RECEIVEANDDELETE);
      message = iMessageReceiver.receive(Duration.ofSeconds(1));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("An error occurred in asbConsume: {}", e.getMessage(), e);
    } catch (ServiceBusException e) {
      LOG.error("An error occurred in asbConsume: {}", e.getMessage(), e);
    }
    LOG.info("=====Consumed message from ASB queue: {}", connectionStringBuilder.getEntityPath());
    return message;
  }

  /**
   * Take a message on the queue and send it to the dead-letter sub-queue.
   *
   * @param connectionStringBuilder Credentials.
   */
  public static void asbDlq(ConnectionStringBuilder connectionStringBuilder) {
    try {
      IMessageReceiver iMessageReceiver =
          ClientFactory.createMessageReceiverFromConnectionStringBuilder(
              connectionStringBuilder, ReceiveMode.PEEKLOCK);
      var message = iMessageReceiver.receive(Duration.ofSeconds(1));
      iMessageReceiver.deadLetterAsync(message.getLockToken());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("An error occurred in asbDlq: {}", e.getMessage(), e);
    } catch (ServiceBusException e) {
      LOG.error("An error occurred in asbDlq: {}", e.getMessage(), e);
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
      asbMove(fromCsb, toCsb);
    }
  }

  public static void asbCopy(ConnectionStringBuilder fromCsb, ConnectionStringBuilder toCsb) {
    asbSend(toCsb, asbRead(fromCsb));
  }

  // todo: this won't work because there needs to be a visibility timeout
  /**
   * Copy all messages from one queue to another. The challenge here is if the queue is too deep,
   * messages that were copied early will become available on the queue before the copyAll process
   * is complete, and then those messages will get copied again. So this code employs this strategy:
   * 1) Check the queue message count, if it is deeper than 1000 messages, abort. 2) Currently this
   * code relies on ReceiveMode.PEEKLOCK which has a 60 sec timeout before making the message
   * available again. 3) Retrieve each message from the queue. 4) Copy the message to the other
   * queue.
   */
  public static void asbCopyAll(ConnectionStringBuilder fromCsb, ConnectionStringBuilder toCsb) {
    // check the queue depth, if it is beyond a certain size, abort
    var depth = messageCount(fromCsb);
    var maxDepth = 1000;
    if (depth < maxDepth) {
      while (depth > 0) {
        LOG.info("depth={}", depth);
        var message = asbReadWithPeeklock(fromCsb);
        if (message != null) {
          asbSend(toCsb, asbReadWithPeeklock(fromCsb));
        }
        depth = messageCount(fromCsb);
      }
    } else {
      LOG.info(
          "Queue is too deep ({}), for a copy all, max depth is currently {}.", depth, maxDepth);
    }
  }

  public static int asbPurge(ConnectionStringBuilder connectionStringBuilder) {
    var counter = 0;
    try {
      IMessageReceiver iMessageReceiver =
          ClientFactory.createMessageReceiverFromConnectionStringBuilder(
              connectionStringBuilder, ReceiveMode.RECEIVEANDDELETE);
      while (iMessageReceiver.peek() != null) {
        var messages = iMessageReceiver.receiveBatch(1);
        LOG.info("asbPurge received {} messages, purging...", messages.size());
        counter += messages.size();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("An error occurred in asbPurge: {}", e.getMessage(), e);
    } catch (ServiceBusException e) {
      LOG.error("An error occurred in asbPurge: {}", e.getMessage(), e);
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
      var mcd =
          client
              .getQueueRuntimeInfo(connectionStringBuilder.getEntityPath())
              .getMessageCountDetails();
      messageCount = mcd.getActiveMessageCount();
      LOG.info(
          "Message Count Details:\n  ActiveMessageCount={}\n  DeadLetterMessageCount={}\n  ScheduledMessageCount={}\n  TransferMessageCount={}\n  TransferDeadLetterMessageCount={}\n",
          mcd.getActiveMessageCount(),
          mcd.getDeadLetterMessageCount(),
          mcd.getScheduledMessageCount(),
          mcd.getTransferMessageCount(),
          mcd.getTransferDeadLetterMessageCount());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("An error occurred in messageCount: {}", e.getMessage(), e);
    } catch (ServiceBusException e) {
      LOG.error("An error occurred in messageCount: {}", e.getMessage(), e);
    }
    return messageCount;
  }

  public static ConnectionStringBuilder connectAsbQ(
      URI endPoint, String entityPath, String sharedAccessKeyName, String sharedAccessKey) {
    return new ConnectionStringBuilder(endPoint, entityPath, sharedAccessKeyName, sharedAccessKey);
  }
}
