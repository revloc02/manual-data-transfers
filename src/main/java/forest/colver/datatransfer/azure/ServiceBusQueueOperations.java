package forest.colver.datatransfer.azure;

import static forest.colver.datatransfer.azure.AzureUtils.ASB_MAX_BATCH_SIZE;
import static forest.colver.datatransfer.azure.AzureUtils.ASB_RECEIVE_TIMEOUT;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClientBuilder;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
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
   * @param connectionString Connection string to the Service Bus namespace or queue.
   * @param queueName The queue name.
   * @param message The message to send.
   */
  public static void asbSend(String connectionString, String queueName, ServiceBusMessage message) {
    try (ServiceBusSenderClient sender =
        new ServiceBusClientBuilder()
            .connectionString(connectionString)
            .sender()
            .queueName(queueName)
            .buildClient()) {
      sender.sendMessage(message);
    }
    LOG.info("ASB_SEND: Message sent to queue: {}", queueName);
  }

  // ASB ReceiveMode primer — ASB has two modes, and the three methods below show the three
  // patterns built from them:
  //   PEEK_LOCK         — message is locked for 60 sec (configurable on the queue, up to 5 min).
  //                      You must call complete() to delete it, or abandon() to release it back.
  //                      If you do nothing, the lock expires and the message reappears.
  //   RECEIVE_AND_DELETE — atomic read+delete. Once received, it's gone. If your process crashes
  //                      mid-handling, the message is lost.
  // The patterns:
  //   asbRead              — PEEK_LOCK + immediate abandon  → "peek without consuming"
  //   asbReadWithPeeklock  — PEEK_LOCK alone, no follow-up  → "claim it for ~60 sec"
  //   asbConsume           — RECEIVE_AND_DELETE              → "take it and don't look back"

  /**
   * Reads a message from the queue non-destructively: locks it via PEEK_LOCK, then immediately
   * abandons the lock so other consumers can see it. Returns the full message payload.
   */
  public static Optional<ServiceBusReceivedMessage> asbRead(
      String connectionString, String queueName) {
    try (ServiceBusReceiverClient receiver =
        new ServiceBusClientBuilder()
            .connectionString(connectionString)
            .receiver()
            .queueName(queueName)
            .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
            .buildClient()) {
      var message = receiver.receiveMessages(1, ASB_RECEIVE_TIMEOUT).stream().findFirst();
      message.ifPresent(receiver::abandon);
      return message;
    }
  }

  /**
   * Reads a message under PEEK_LOCK and leaves the lock held. The message stays invisible to other
   * consumers until the lock expires (default 60 sec) and then becomes available again. Useful when
   * you want to claim a message for a window of time without deleting it.
   */
  public static Optional<ServiceBusReceivedMessage> asbReadWithPeeklock(
      String connectionString, String queueName) {
    try (ServiceBusReceiverClient receiver =
        new ServiceBusClientBuilder()
            .connectionString(connectionString)
            .receiver()
            .queueName(queueName)
            .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
            .buildClient()) {
      return receiver.receiveMessages(1, ASB_RECEIVE_TIMEOUT).stream().findFirst();
    }
  }

  /**
   * Consumes a message destructively using RECEIVE_AND_DELETE — the message is removed from the
   * queue atomically with the receive. No lock, no follow-up call needed. If this process dies
   * before handling the message, it's gone.
   */
  public static Optional<ServiceBusReceivedMessage> asbConsume(
      String connectionString, String queueName) {
    try (ServiceBusReceiverClient receiver =
        new ServiceBusClientBuilder()
            .connectionString(connectionString)
            .receiver()
            .queueName(queueName)
            .receiveMode(ServiceBusReceiveMode.RECEIVE_AND_DELETE)
            .buildClient()) {
      var message = receiver.receiveMessages(1, ASB_RECEIVE_TIMEOUT).stream().findFirst();
      message.ifPresent(m -> LOG.info("ASB_CONSUME: Consumed message from queue: {}", queueName));
      return message;
    }
  }

  /**
   * Take a message on the queue and send it to the dead-letter sub-queue.
   *
   * @param connectionString Connection string to the Service Bus namespace or queue.
   * @param queueName The queue name.
   */
  public static void asbDlq(String connectionString, String queueName) {
    try (ServiceBusReceiverClient receiver =
        new ServiceBusClientBuilder()
            .connectionString(connectionString)
            .receiver()
            .queueName(queueName)
            .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
            .buildClient()) {
      var message = receiver.receiveMessages(1, ASB_RECEIVE_TIMEOUT).stream().findFirst();
      message.ifPresent(receiver::deadLetter);
    }
  }

  public static void asbMove(String connectionString, String fromQueue, String toQueue) {
    try (ServiceBusReceiverClient receiver =
        new ServiceBusClientBuilder()
            .connectionString(connectionString)
            .receiver()
            .queueName(fromQueue)
            .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
            .buildClient()) {
      var message = receiver.receiveMessages(1, ASB_RECEIVE_TIMEOUT).stream().findFirst();
      if (message.isPresent()) {
        var msg = message.get();
        asbSend(connectionString, toQueue, new ServiceBusMessage(msg));
        receiver.complete(msg);
        LOG.info("Message moved from {} to {}", fromQueue, toQueue);
      } else {
        LOG.warn("No message to move from {}", fromQueue);
      }
    }
  }

  public static void asbMoveAll(String connectionString, String fromQueue, String toQueue) {
    while (messageCount(connectionString, fromQueue) > 0) {
      asbMove(connectionString, fromQueue, toQueue);
    }
  }

  public static void asbCopy(String connectionString, String fromQueue, String toQueue) {
    asbRead(connectionString, fromQueue)
        .ifPresentOrElse(
            message -> asbSend(connectionString, toQueue, new ServiceBusMessage(message)),
            () -> LOG.error("No ASB message available."));
  }

  /**
   * Copy all messages from one queue to another using peek (non-destructive read).
   *
   * <p>Why peek and not receive: ASB has no per-receive visibility timeout like AWS SQS, so a
   * receive-based copy would either consume messages or rely on the PEEK_LOCK timer (default 60
   * sec), which fails on deeper queues — locks expire mid-copy and messages get re-read and
   * duplicated. Peek reads a snapshot without any lock or state change, so there is no time
   * pressure, no depth limit, and no duplicate risk.
   *
   * <p>Iteration: ASB assigns each message a monotonically increasing sequence number. We peek a
   * batch, remember the highest sequence number seen, then peek the next batch starting from
   * (lastSeq + 1). Loop until a batch comes back empty.
   */
  public static int asbCopyAll(String connectionString, String fromQueue, String toQueue) {
    var counter = 0;
    try (ServiceBusReceiverClient receiver =
        new ServiceBusClientBuilder()
            .connectionString(connectionString)
            .receiver()
            .queueName(fromQueue)
            .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
            .buildClient()) {
      long lastSequenceNumber = 0;
      var batch = receiver.peekMessages(ASB_MAX_BATCH_SIZE);
      while (batch != null && batch.iterator().hasNext()) {
        for (var message : batch) {
          asbSend(connectionString, toQueue, new ServiceBusMessage(message));
          counter++;
          lastSequenceNumber = message.getSequenceNumber();
        }
        LOG.info("Copied {} messages so far...", counter);
        batch = receiver.peekMessages(ASB_MAX_BATCH_SIZE, lastSequenceNumber + 1);
      }
    }
    LOG.info("asbCopyAll copied {} messages total.", counter);
    return counter;
  }

  public static int asbQueuePurge(String connectionString, String queueName) {
    int counter = 0;
    try (ServiceBusReceiverClient receiver =
        new ServiceBusClientBuilder()
            .connectionString(connectionString)
            .receiver()
            .queueName(queueName)
            .receiveMode(ServiceBusReceiveMode.RECEIVE_AND_DELETE)
            .buildClient()) {
      while (receiver.peekMessage() != null) {
        var messages = receiver.receiveMessages(ASB_MAX_BATCH_SIZE, ASB_RECEIVE_TIMEOUT);
        if (messages != null && messages.stream().findAny().isPresent()) {
          long messageCount = messages.stream().count();
          LOG.info("asbQueuePurge received {} messages, purging...", messageCount);
          counter += messageCount;
        }
      }
    }
    LOG.info("asbQueuePurge purged {} messages.", counter);
    return counter;
  }

  /**
   * Returns the ActiveMessageCount for the queue.
   *
   * @return The number of Active messages on the queue.
   */
  public static long messageCount(String connectionString, String queueName) {
    ServiceBusAdministrationClient adminClient =
        new ServiceBusAdministrationClientBuilder()
            .connectionString(connectionString)
            .buildClient();
    var properties = adminClient.getQueueRuntimeProperties(queueName);
    var activeCount = properties.getActiveMessageCount();
    LOG.info(
        "Message Count Details:\n  ActiveMessageCount={}\n  DeadLetterMessageCount={}\n  ScheduledMessageCount={}\n  TransferMessageCount={}\n  TransferDeadLetterMessageCount={}\n",
        properties.getActiveMessageCount(),
        properties.getDeadLetterMessageCount(),
        properties.getScheduledMessageCount(),
        properties.getTransferMessageCount(),
        properties.getTransferDeadLetterMessageCount());
    return activeCount;
  }
}
