package forest.colver.datatransfer.azure;

import static forest.colver.datatransfer.azure.AzureUtils.ASB_MAX_BATCH_SIZE;
import static forest.colver.datatransfer.azure.AzureUtils.ASB_RECEIVE_TIMEOUT;

import com.microsoft.azure.servicebus.ClientFactory;
import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.IMessageReceiver;
import com.microsoft.azure.servicebus.IMessageSender;
import com.microsoft.azure.servicebus.ReceiveMode;
import com.microsoft.azure.servicebus.management.ManagementClient;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceBusQueueOperations {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceBusQueueOperations.class);

  private ServiceBusQueueOperations() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

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
      LOG.error("An error occurred in asbSend", e);
    } catch (ServiceBusException e) {
      LOG.error("An error occurred in asbSend", e);
    }
  }

  public static Optional<IMessageReceiver> getReceiver(
      ConnectionStringBuilder connectionStringBuilder) {
    try {
      return Optional.of(
          ClientFactory.createMessageReceiverFromConnectionStringBuilder(
              connectionStringBuilder, ReceiveMode.PEEKLOCK));
    } catch (InterruptedException | ServiceBusException e) {
      Thread.currentThread().interrupt();
      LOG.error("An error occurred in getReceiver", e);
    }
    return Optional.empty();
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
  public static Optional<IMessage> asbRead(ConnectionStringBuilder connectionStringBuilder) {
    try {
      IMessageReceiver iMessageReceiver =
          ClientFactory.createMessageReceiverFromConnectionStringBuilder(
              connectionStringBuilder, ReceiveMode.PEEKLOCK);
      var message = iMessageReceiver.receive(ASB_RECEIVE_TIMEOUT);
      if (message != null) {
        iMessageReceiver.abandon(
            message.getLockToken()); // make message available for other consumers
        return Optional.of(message);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("An error occurred in asbRead", e);
    } catch (ServiceBusException e) {
      LOG.error("An error occurred in asbRead", e);
    }
    return Optional.empty();
  }

  // todo: I tried this version of asbRead and got a null when I tried to get the message body, so
  // what gives? Works with RECEIVEANDDELETE, Azure is stupid.

  /**
   * This method will read a message, and then it will not be available for any other consumers for
   * 60 sec, as the message is locked by the queue.
   */
  public static Optional<IMessage> asbReadWithPeeklock(
      ConnectionStringBuilder connectionStringBuilder) {
    try {
      IMessageReceiver iMessageReceiver =
          ClientFactory.createMessageReceiverFromConnectionStringBuilder(
              connectionStringBuilder, ReceiveMode.PEEKLOCK);
      return Optional.ofNullable(iMessageReceiver.receive(ASB_RECEIVE_TIMEOUT));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("An error occurred in asbReadWithPeeklock", e);
    } catch (ServiceBusException e) {
      LOG.error("An error occurred in asbReadWithPeeklock", e);
    }
    return Optional.empty();
  }

  /**
   * Consumes a message from an ASB queue. Uses ReceiveMode.RECEIVEANDDELETE which immediately
   * deletes the message after retrieving it--simply consuming the message without any thought of
   * processing it first.
   */
  public static Optional<IMessage> asbConsume(ConnectionStringBuilder connectionStringBuilder) {
    try {
      IMessageReceiver iMessageReceiver =
          ClientFactory.createMessageReceiverFromConnectionStringBuilder(
              connectionStringBuilder, ReceiveMode.RECEIVEANDDELETE);
      var message = iMessageReceiver.receive(ASB_RECEIVE_TIMEOUT);
      LOG.info("=====Consumed message from ASB queue: {}", connectionStringBuilder.getEntityPath());
      return Optional.ofNullable(message);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("An error occurred in asbConsume", e);
    } catch (ServiceBusException e) {
      LOG.error("An error occurred in asbConsume", e);
    }
    return Optional.empty();
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
      var message = iMessageReceiver.receive(ASB_RECEIVE_TIMEOUT);
      iMessageReceiver.deadLetterAsync(message.getLockToken());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("An error occurred in asbDlq", e);
    } catch (ServiceBusException e) {
      LOG.error("An error occurred in asbDlq", e);
    }
  }

  public static void asbMove(ConnectionStringBuilder fromCsb, ConnectionStringBuilder toCsb) {
    getReceiver(fromCsb)
        .ifPresentOrElse(
            receiver -> {
              try {
                var message = receiver.receive(ASB_RECEIVE_TIMEOUT);
                if (message != null) {
                  asbSend(toCsb, message);
                  receiver.completeAsync(message.getLockToken()); // delete the message
                  LOG.info(
                      "Message moved from {} to {}",
                      fromCsb.getEntityPath(),
                      toCsb.getEntityPath());
                } else {
                  LOG.warn("No message to move from {}", fromCsb.getEntityPath());
                }
              } catch (InterruptedException | ServiceBusException e) {
                Thread.currentThread().interrupt();
                LOG.error("An error occurred in asbMove", e);
              }
            },
            () -> LOG.error("Could not get receiver for {}", fromCsb.getEntityPath()));
  }

  public static void asbMoveAll(ConnectionStringBuilder fromCsb, ConnectionStringBuilder toCsb) {
    while (messageCount(fromCsb) > 0) {
      asbMove(fromCsb, toCsb);
    }
  }

  public static void asbCopy(ConnectionStringBuilder fromCsb, ConnectionStringBuilder toCsb) {
    asbRead(fromCsb)
        .ifPresentOrElse(
            message -> asbSend(toCsb, message), () -> LOG.error("No ASB message available."));
  }

  /**
   * Copy all messages from one queue to another using peek (non-destructive read).
   *
   * <p>Why peek and not receive: ASB has no per-receive visibility timeout like AWS SQS, so a
   * receive-based copy would either consume messages or rely on the PEEKLOCK timer (default 60
   * sec), which fails on deeper queues — locks expire mid-copy and messages get re-read and
   * duplicated. Peek reads a snapshot without any lock or state change, so there is no time
   * pressure, no depth limit, and no duplicate risk.
   *
   * <p>Iteration: ASB assigns each message a monotonically increasing sequence number. We peek a
   * batch, remember the highest sequence number seen, then peek the next batch starting from
   * (lastSeq + 1). Loop until a batch comes back empty.
   */
  public static int asbCopyAll(ConnectionStringBuilder fromCsb, ConnectionStringBuilder toCsb) {
    var counter = 0;
    try {
      // ReceiveMode is required to construct the receiver but is irrelevant for peek — peek never
      // locks or consumes regardless of mode.
      var receiver =
          ClientFactory.createMessageReceiverFromConnectionStringBuilder(
              fromCsb, ReceiveMode.PEEKLOCK);
      try {
        // First batch: no sequence number, so peek starts from the earliest active message.
        var batch = receiver.peekBatch(ASB_MAX_BATCH_SIZE);
        while (batch != null && !batch.isEmpty()) {
          long lastSequenceNumber = 0;
          for (var message : batch) {
            asbSend(toCsb, message);
            counter++;
            lastSequenceNumber = message.getSequenceNumber();
          }
          LOG.info("Copied {} messages so far...", counter);
          // Advance past the last seen message — without +1 we'd re-peek it and loop forever.
          batch = receiver.peekBatch(lastSequenceNumber + 1, ASB_MAX_BATCH_SIZE);
        }
      } finally {
        receiver.close();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("An error occurred in asbCopyAll", e);
    } catch (ServiceBusException e) {
      LOG.error("An error occurred in asbCopyAll", e);
    }
    LOG.info("asbCopyAll copied {} messages total.", counter);
    return counter;
  }

  public static int asbQueuePurge(ConnectionStringBuilder connectionStringBuilder) {
    int counter = 0;
    try {
      var iMessageReceiver =
          ClientFactory.createMessageReceiverFromConnectionStringBuilder(
              connectionStringBuilder, ReceiveMode.RECEIVEANDDELETE);
      try {
        while (iMessageReceiver.peek() != null) {
          var messages = iMessageReceiver.receiveBatch(ASB_MAX_BATCH_SIZE);
          if (messages != null && !messages.isEmpty()) {
            LOG.info("asbQueuePurge received {} messages, purging...", messages.size());
            counter += messages.size();
          }
        }
      } finally {
        iMessageReceiver.close();
      }
    } catch (InterruptedException | ServiceBusException e) {
      Thread.currentThread().interrupt();
      LOG.error("An error occurred in asbQueuePurge", e);
    }
    LOG.info("asbQueuePurge purged {} messages.", counter);
    return counter;
  }

  /**
   * Returns the ActiveMessageCount for the queue.
   *
   * @return The number of Active messages on the queue.
   */
  public static long messageCount(String connectionString) {
    return messageCount(new ConnectionStringBuilder(connectionString));
  }

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
      LOG.error("An error occurred in messageCount", e);
    } catch (ServiceBusException e) {
      LOG.error("An error occurred in messageCount", e);
    } finally {
      try {
        client.close();
      } catch (IOException e) {
        LOG.error("Failed to close ManagementClient", e);
      }
    }
    return messageCount;
  }

  public static ConnectionStringBuilder connectAsbQ(
      URI endPoint, String entityPath, String sharedAccessKeyName, String sharedAccessKey) {
    return new ConnectionStringBuilder(endPoint, entityPath, sharedAccessKeyName, sharedAccessKey);
  }
}
