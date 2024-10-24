package forest.colver.datatransfer.azure;

import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.models.PeekedMessageItem;
import com.azure.storage.queue.models.QueueMessageItem;
import com.azure.storage.queue.models.QueueStorageException;
import java.util.List;
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
      LOG.error("An error occurred while sending the message: {}", e.getMessage(), e);
    }
  }

  /**
   * Peeks at the first message in the queue. Peeked messages don't contain the necessary
   * information needed to interact with the message nor will it hide messages from other operations
   * on the queue. I've been using it ot ensure a message arrived on the queue.
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
      LOG.error("An error occurred while peeking at the message: {}", e.getMessage(), e);
    }
    return body;
  }

  public static QueueMessageItem asqConsume(String connectStr, String queueName) {
    QueueMessageItem message = null;
    var queueClient =
        new QueueClientBuilder().connectionString(connectStr).queueName(queueName).buildClient();
    try {
      message = queueClient.receiveMessage();
      if (null != message) {
        queueClient.deleteMessage(message.getMessageId(), message.getPopReceipt());
      } else {
        LOG.warn("No visible messages in {} queue", queueName);
      }
    } catch (QueueStorageException e) {
      LOG.error("An error occurred while receiving the message: {}", e.getMessage(), e);
    }
    return message;
  }

  public static int asqQueueDepth(String connectStr, String queueName) {
    int queueDepth = 0;
    var queueClient =
        new QueueClientBuilder().connectionString(connectStr).queueName(queueName).buildClient();
    try {
      queueDepth = queueClient.getProperties().getApproximateMessagesCount();
    } catch (QueueStorageException e) {
      LOG.error("An error occurred while getting message properties: {}", e.getMessage(), e);
    }
    return queueDepth;
  }

  public static void asqPurge(String connectStr, String queueName) {
    try {
      QueueClient queueClient = new QueueClientBuilder()
          .connectionString(connectStr)
          .queueName(queueName)
          .buildClient();
      queueClient.clearMessages();
    } catch (QueueStorageException e) {
      e.printStackTrace();
    }
    LOG.info("Purged {}", queueName);
  }

  public static void asqSendMultipleUniqueMessages(String connectStr, String queueName,
      List<String> payloads) {
    try {
      QueueClient queueClient = new QueueClientBuilder()
          .connectionString(connectStr)
          .queueName(queueName)
          .buildClient();
      for (String payload : payloads) {
        queueClient.sendMessage(payload);
      }
    } catch (QueueStorageException e) {
      e.printStackTrace();
    }
  }

  public static void asqMove(String connectStr, String queue1, String queue2) {
    var message = asqConsume(connectStr, queue1);
    if (message != null) {
      asqSend(connectStr, queue2, String.valueOf(message.getBody()));
    } else {
      LOG.error("ERROR: message is null.");
    }
  }

  public static void asqMoveAll(String connectStr, String queue1, String queue2) {
    while (asqQueueDepth(connectStr, queue1) > 0) {
      var message = asqConsume(connectStr, queue1);
      if (message != null) {
        asqSend(connectStr, queue2, String.valueOf(message.getBody()));
      } else {
        LOG.error("ERROR: message is null.");
      }
    }
  }

  //todo: can I make a copyAll method?
  public static void asqCopy(String connectStr, String queue1, String queue2) {
    QueueMessageItem message;
    try {
      QueueClient queueClient = new QueueClientBuilder()
          .connectionString(connectStr)
          .queueName(queue1)
          .buildClient();
      message = queueClient.receiveMessage(); // default visibilityTimeout=30 seconds
      asqSend(connectStr, queue2, String.valueOf(message.getBody()));
    } catch (QueueStorageException e) {
      e.printStackTrace();
    }
  }
}
