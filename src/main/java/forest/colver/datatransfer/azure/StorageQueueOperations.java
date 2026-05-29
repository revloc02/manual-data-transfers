package forest.colver.datatransfer.azure;

import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.models.PeekedMessageItem;
import com.azure.storage.queue.models.QueueMessageItem;
import com.azure.storage.queue.models.QueueStorageException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageQueueOperations {

  private static final Logger LOG = LoggerFactory.getLogger(StorageQueueOperations.class);

  private StorageQueueOperations() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

  public static void asqSend(String connectStr, String queueName, String messageText) {
    var queueClient =
        new QueueClientBuilder().connectionString(connectStr).queueName(queueName).buildClient();
    try {
      queueClient.sendMessage(messageText);
    } catch (QueueStorageException e) {
      LOG.error("An error occurred while sending the message", e);
    }
  }

  /**
   * Peeks at the first message in the queue. Peeked messages don't contain the necessary
   * information needed to interact with the message nor will it hide messages from other operations
   * on the queue. I've been using it to ensure a message arrived on the queue.
   *
   * @param connectStr The connection string to connect to the service.
   * @param queueName The name of the queue that the client will interact with.
   * @return The content of the Message.
   */
  public static String asqPeek(String connectStr, String queueName) {
    String body = "";
    var queueClient =
        new QueueClientBuilder().connectionString(connectStr).queueName(queueName).buildClient();
    try {
      PeekedMessageItem peekedMessageItem = queueClient.peekMessage();
      body = peekedMessageItem.getBody().toString();
    } catch (QueueStorageException e) {
      LOG.error("An error occurred while peeking at the message", e);
    }
    return body;
  }

  public static Optional<QueueMessageItem> asqConsume(String connectStr, String queueName) {
    var queueClient =
        new QueueClientBuilder().connectionString(connectStr).queueName(queueName).buildClient();
    try {
      var message = queueClient.receiveMessage();
      if (message != null) {
        queueClient.deleteMessage(message.getMessageId(), message.getPopReceipt());
        return Optional.of(message);
      }
      LOG.warn("No visible messages in {} queue", queueName);
    } catch (QueueStorageException e) {
      LOG.error("An error occurred while receiving the message", e);
    }
    return Optional.empty();
  }

  public static int asqQueueDepth(String connectStr, String queueName) {
    int queueDepth = 0;
    var queueClient =
        new QueueClientBuilder().connectionString(connectStr).queueName(queueName).buildClient();
    try {
      queueDepth = queueClient.getProperties().getApproximateMessagesCount();
    } catch (QueueStorageException e) {
      LOG.error("An error occurred while getting message properties", e);
    }
    return queueDepth;
  }

  public static void asqPurge(String connectStr, String queueName) {
    var queueClient =
        new QueueClientBuilder().connectionString(connectStr).queueName(queueName).buildClient();
    try {
      queueClient.clearMessages();
    } catch (QueueStorageException e) {
      LOG.error("An error occurred while clearing messages", e);
    }
    LOG.info("Purged {}", queueName);
  }

  public static void asqSendMultipleUniqueMessages(
      String connectStr, String queueName, List<String> payloads) {
    var queueClient =
        new QueueClientBuilder().connectionString(connectStr).queueName(queueName).buildClient();
    try {
      for (String payload : payloads) {
        queueClient.sendMessage(payload);
      }
    } catch (QueueStorageException e) {
      LOG.error("An error occurred while sending messages", e);
    }
  }

  public static void asqMove(String connectStr, String queue1, String queue2) {
    asqConsume(connectStr, queue1)
        .ifPresentOrElse(
            message -> asqSend(connectStr, queue2, String.valueOf(message.getBody())),
            () -> LOG.error("No ASQ message available."));
  }

  public static void asqMoveAll(String connectStr, String queue1, String queue2) {
    while (asqQueueDepth(connectStr, queue1) > 0) {
      asqConsume(connectStr, queue1)
          .ifPresent(message -> asqSend(connectStr, queue2, String.valueOf(message.getBody())));
    }
  }

  public static void asqCopy(String connectStr, String queue1, String queue2) {
    QueueMessageItem message;
    var queueClient =
        new QueueClientBuilder().connectionString(connectStr).queueName(queue1).buildClient();
    try {
      message = queueClient.receiveMessage(); // default visibilityTimeout=30 seconds
      asqSend(connectStr, queue2, String.valueOf(message.getBody()));
    } catch (QueueStorageException e) {
      LOG.error("An error occurred while copying the message", e);
    }
  }

  /**
   * Copy all messages from one ASQ queue to another non-destructively. Designed for ad-hoc manual
   * transfers — we just want every visible message replicated to the destination, leaving the
   * source queue intact.
   *
   * <p>Strategy: receive in batches with a 30-sec visibility timeout, copy each message to the
   * destination, and let the timeout naturally release messages back to the source queue. A Set of
   * seen message IDs is the safety net: if a batch is slow and a message's visibility timeout
   * expires before we finish, we detect the reappearance and skip rather than silently duplicate.
   * When a batch returns nothing but already-seen IDs (or nothing at all), we're done.
   *
   * <p>If you ever need to release messages back to the queue immediately rather than waiting 30
   * sec, see {@link #asqMoveMatching} — it shows the explicit-release pattern using {@code
   * updateMessage(id, popReceipt, body, Duration.ZERO)}.
   *
   * <p>ASQ vs ASB: ASQ has a true visibility timeout (like AWS SQS), unlike Azure Service Bus which
   * uses message locks. That's why this looks like sqsCopyAll and not asbCopyAll.
   *
   * @return count of messages copied
   */
  public static int asqCopyAll(String connectStr, String fromQueue, String toQueue) {
    // Batch size cap is 32 — that's an Azure Storage Queue API hard limit, not a tuning choice.
    final int batchSize = 32;
    // 30 sec is short enough to be a non-issue for partner consumers and long enough to comfortably
    // copy a batch even on a slow day.
    final Duration visibilityTimeout = Duration.ofSeconds(30);

    var fromClient =
        new QueueClientBuilder().connectionString(connectStr).queueName(fromQueue).buildClient();
    Set<String> seenIds = new HashSet<>();
    int copied = 0;
    try {
      while (true) {
        // receiveMessages returns a PagedIterable; for our small batches it's effectively a list.
        var batch = fromClient.receiveMessages(batchSize, visibilityTimeout, null, null);
        boolean foundNew = false;
        for (QueueMessageItem message : batch) {
          if (seenIds.contains(message.getMessageId())) {
            // Same message reappeared — its visibility timeout expired before we drained the rest
            // of this batch. Skip rather than duplicate.
            LOG.warn(
                "Message {} reappeared during copy; skipping to avoid duplicate.",
                message.getMessageId());
          } else {
            asqSend(connectStr, toQueue, String.valueOf(message.getBody()));
            seenIds.add(message.getMessageId());
            copied++;
            foundNew = true;
          }
        }
        // Stop conditions:
        //   - Batch was empty: queue is drained.
        //   - Batch had messages but they were all already-seen IDs: we've cycled through every
        //     message at least once. Stopping here prevents an infinite loop on a queue that
        //     keeps replaying the same set.
        if (!foundNew) {
          break;
        }
      }
    } catch (QueueStorageException e) {
      LOG.error("An error occurred during asqCopyAll", e);
    }
    LOG.info("asqCopyAll copied {} messages from {} to {}.", copied, fromQueue, toQueue);
    return copied;
  }

  /**
   * Move messages from source to destination, but only those whose body matches the predicate.
   * Non-matching messages are released back to the source queue immediately. Useful when a partner
   * queue has mixed traffic and you only want to migrate a subset (e.g., messages of a particular
   * type, or matching a specific identifier).
   *
   * <p>This is the canonical use of the explicit-release pattern. A plain {@code receiveMessage()}
   * hides the message for 30 sec by default — if we don't want it, that 30 sec is wasted
   * invisibility for no reason. Calling {@code updateMessage(id, popReceipt, body, Duration.ZERO)}
   * resets the visibility timeout to zero and puts the message back in play immediately, so the
   * partner's normal consumers can keep handling it.
   *
   * <p>Stop condition: we track IDs of non-matches we've already released. When a batch contains
   * nothing but non-matches we've already seen (or comes back empty), we're done. The ID tracking
   * is essential here: because we release non-matches with Duration.ZERO, they reappear on the
   * queue immediately and would be received again forever without this guard. Matched messages are
   * deleted from source, so they can't reappear and don't need tracking.
   *
   * @return count of messages that matched and were moved
   */
  public static int asqMoveMatching(
      String connectStr,
      String fromQueue,
      String toQueue,
      java.util.function.Predicate<String> matcher) {
    final int batchSize = 32;
    final Duration visibilityTimeout = Duration.ofSeconds(30);
    var fromClient =
        new QueueClientBuilder().connectionString(connectStr).queueName(fromQueue).buildClient();
    Set<String> releasedNonMatchIds = new HashSet<>();
    int moved = 0;
    try {
      while (true) {
        var batch = fromClient.receiveMessages(batchSize, visibilityTimeout, null, null);
        boolean foundUnseen = false;
        for (QueueMessageItem message : batch) {
          String body = String.valueOf(message.getBody());
          if (matcher.test(body)) {
            // Match: copy to destination and delete from source.
            asqSend(connectStr, toQueue, body);
            fromClient.deleteMessage(message.getMessageId(), message.getPopReceipt());
            moved++;
            foundUnseen = true;
          } else if (!releasedNonMatchIds.contains(message.getMessageId())) {
            // First time seeing this non-match. Release immediately so partner consumers can
            // see it without waiting 30 sec, and remember its ID so we don't loop forever
            // re-receiving it.
            fromClient.updateMessage(
                message.getMessageId(), message.getPopReceipt(), body, Duration.ZERO);
            releasedNonMatchIds.add(message.getMessageId());
            foundUnseen = true;
          } else {
            // Already-seen non-match. Re-release with the fresh popReceipt so it stays visible,
            // but don't count it as new work — it doesn't keep the loop alive.
            fromClient.updateMessage(
                message.getMessageId(), message.getPopReceipt(), body, Duration.ZERO);
          }
        }
        if (!foundUnseen) {
          break;
        }
      }
    } catch (QueueStorageException e) {
      LOG.error("An error occurred during asqMoveMatching", e);
    }
    LOG.info(
        "asqMoveMatching moved {} matching messages from {} to {}.", moved, fromQueue, toQueue);
    return moved;
  }
}
