package forest.colver.datatransfer.azure;

import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.models.PeekedMessageItem;
import com.azure.storage.queue.models.QueueMessageItem;
import com.azure.storage.queue.models.QueueStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageQueueOperations {

  private static final Logger LOG = LoggerFactory.getLogger(StorageQueueOperations.class);

  public static void asqSend(String connectStr, String queueName, String messageText) {
    try {
      QueueClient queueClient = new QueueClientBuilder()
          .connectionString(connectStr)
          .queueName(queueName)
          .buildClient();

      queueClient.sendMessage(messageText);
    } catch (QueueStorageException e) {
      e.printStackTrace();
    }
  }

  public static String asqPeek(String connectStr, String queueName) {
    String body = "";
    try {
      QueueClient queueClient = new QueueClientBuilder()
          .connectionString(connectStr)
          .queueName(queueName)
          .buildClient();

      PeekedMessageItem peekedMessageItem = queueClient.peekMessage();
      body = peekedMessageItem.getBody().toString();
    } catch (QueueStorageException e) {
      e.printStackTrace();
    }
    return body;
  }

  public static QueueMessageItem asqConsume(String connectStr, String queueName) {
    QueueMessageItem message = null;
    try {
      QueueClient queueClient = new QueueClientBuilder()
          .connectionString(connectStr)
          .queueName(queueName)
          .buildClient();
      message = queueClient.receiveMessage();

      if (null != message) {
        queueClient.deleteMessage(message.getMessageId(), message.getPopReceipt());
      } else {
        LOG.warn("No visible messages in {} queue", queueName);
      }
    } catch (QueueStorageException e) {
      e.printStackTrace();
    }
    return message;
  }

  public static int asqQueueDepth(String connectStr, String queueName) {
    int queueDepth = 0;
    try {
      QueueClient queueClient = new QueueClientBuilder()
          .connectionString(connectStr)
          .queueName(queueName)
          .buildClient();
      queueDepth = queueClient.getProperties().getApproximateMessagesCount();
    } catch (QueueStorageException e) {
      e.printStackTrace();
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
  }

}
