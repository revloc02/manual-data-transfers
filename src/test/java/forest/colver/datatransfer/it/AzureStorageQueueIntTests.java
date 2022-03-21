package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.StorageQueueOperations.asqConsume;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqPeek;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqPurge;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqQueueDepth;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqSend;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_STORAGE_ACCOUNT_CONNECTION_STRING;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.jms.JMSException;
import javax.jms.TextMessage;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for Azure Storage Queue (ASQ) operations.
 */
public class AzureStorageQueueIntTests {

  private static final Logger LOG = LoggerFactory.getLogger(AzureStorageQueueIntTests.class);
  public static final String CONNECT_STR = EMX_SANDBOX_STORAGE_ACCOUNT_CONNECTION_STRING;
  public static final String QUEUE_NAME = "forest-test-storage-queue";
  public static final String PAYLOAD = "this is the body";

  /**
   * Test Azure Storage Queue Send
   */
  @Test
  public void testAsqSend() {
    asqSend(CONNECT_STR, QUEUE_NAME, PAYLOAD);
    assertThat(asqPeek(CONNECT_STR, QUEUE_NAME)).isEqualTo(PAYLOAD);
    //cleanup
    asqConsume(CONNECT_STR, QUEUE_NAME);
  }

  /**
   * Test Azure Storage Queue Consume
   */
  @Test
  public void testAsqConsume() {
    // send message
    asqSend(CONNECT_STR, QUEUE_NAME, PAYLOAD);

    // make sure message arrived
    assertThat(asqPeek(CONNECT_STR, QUEUE_NAME)).isEqualTo(PAYLOAD);

    // consume the message
    var message = asqConsume(CONNECT_STR, QUEUE_NAME);
    assertThat(message.getBody().toString()).isEqualTo(PAYLOAD);
  }

  /**
   * Test Azure Storage Queue Purge
   */
  @Test
  public void testAsqPurge() {
    var num = 3;
    // send messages
    for (var i = 0; i < num; i++) {
      asqSend(CONNECT_STR, QUEUE_NAME, PAYLOAD);
    }
    // ensure messages are on queue
    assertThat(asqQueueDepth(CONNECT_STR, QUEUE_NAME)).isEqualTo(num);
    // clear the queue
    asqPurge(CONNECT_STR, QUEUE_NAME);
    // assert the queue is cleared
    assertThat(asqQueueDepth(CONNECT_STR, QUEUE_NAME)).isEqualTo(0);
  }
}
