package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.StorageQueueOperations.asqConsume;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqCopy;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqMove;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqMoveAll;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqPeek;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqPurge;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqQueueDepth;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqSend;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqSendMultipleUniqueMessages;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_SA_CONN_STR;
import static forest.colver.datatransfer.config.Utils.generateUniqueStrings;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.azure.storage.queue.models.QueueMessageItem;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration tests for Azure Storage Queue (ASQ) operations. */
public class AzureStorageQueueIntTests {

  public static final String CONNECT_STR = EMX_SANDBOX_SA_CONN_STR;
  public static final String QUEUE_NAME = "forest-test-storage-queue";
  public static final String QUEUE2_NAME = "forest-test-storage-queue2";
  public static final String PAYLOAD = "this is the body";
  private static final Logger LOG = LoggerFactory.getLogger(AzureStorageQueueIntTests.class);

  /** Test Azure Storage Queue Send */
  @Test
  void testAsqSend() {
    asqSend(CONNECT_STR, QUEUE_NAME, PAYLOAD);
    assertThat(asqPeek(CONNECT_STR, QUEUE_NAME)).isEqualTo(PAYLOAD);
    // cleanup
    asqConsume(CONNECT_STR, QUEUE_NAME);
  }

  /** Test Azure Storage Queue Consume */
  @Test
  void testAsqConsume() {
    // send message
    asqSend(CONNECT_STR, QUEUE_NAME, PAYLOAD);

    // make sure message arrived
    assertThat(asqPeek(CONNECT_STR, QUEUE_NAME)).isEqualTo(PAYLOAD);

    // consume the message
    var message = asqConsume(CONNECT_STR, QUEUE_NAME);
    assertThat(message.getBody()).hasToString(PAYLOAD);
  }

  /** Test Azure Storage Queue Purge */
  @Test
  void testAsqPurge() {
    var num = 3;
    // send messages
    for (var i = 0; i < num; i++) {
      asqSend(CONNECT_STR, QUEUE_NAME, PAYLOAD);
    }
    // ensure messages are on queue
    assertThat(asqQueueDepth(CONNECT_STR, QUEUE_NAME)).isGreaterThanOrEqualTo(num);
    // clear the queue
    asqPurge(CONNECT_STR, QUEUE_NAME);
    // assert the queue is cleared
    assertThat(asqQueueDepth(CONNECT_STR, QUEUE_NAME)).isZero();
  }

  /**
   * The goal is to test that the queue allows competing consumers. Sets up a queue with a bunch of
   * unique messages. Then creates a number of threads to consume each of those messages and compare
   * them against the master list of unique messages to ensure everything got consumed correctly.
   */
  @Test
  void testCompetingConsumer() throws ExecutionException, InterruptedException {
    asqPurge(CONNECT_STR, QUEUE_NAME);

    var numMsgs = 300;
    var uuids = generateUniqueStrings(numMsgs);
    asqSendMultipleUniqueMessages(CONNECT_STR, QUEUE_NAME, uuids);
    LOG.info("Sent {} messages.", numMsgs);

    var threads = 30;
    var es = Executors.newFixedThreadPool(threads);
    List<Future<QueueMessageItem>> futuresList = new ArrayList<>();
    for (var task = 0; task < numMsgs; task++) {
      futuresList.add(es.submit(() -> asqConsume(CONNECT_STR, QUEUE_NAME)));
    }
    LOG.info("Tasks submitted: futuresList.size={}", futuresList.size());

    for (Future<QueueMessageItem> future : futuresList) {
      // remember, future.get() blocks execution until the task is complete
      uuids.remove(future.get().getBody().toString());
      LOG.info("removed {}", future.get().getBody());
    }
    assertThat(uuids).isEmpty();
  }

  @Test
  void testAsqMove() {
    // place a message
    asqSend(CONNECT_STR, QUEUE_NAME, PAYLOAD);
    // ensure it arrived
    assertThat(asqPeek(CONNECT_STR, QUEUE_NAME)).isEqualTo(PAYLOAD);
    // move the message
    asqMove(CONNECT_STR, QUEUE_NAME, QUEUE2_NAME);
    // ensure it moved
    assertThat(asqPeek(CONNECT_STR, QUEUE2_NAME)).isEqualTo(PAYLOAD);
    // cleanup
    asqConsume(CONNECT_STR, QUEUE2_NAME);
  }

  @Test
  void testAsqCopy() {
    // place a message
    asqSend(CONNECT_STR, QUEUE_NAME, PAYLOAD);
    // ensure it arrived
    assertThat(asqPeek(CONNECT_STR, QUEUE_NAME)).isEqualTo(PAYLOAD);
    // move the message
    asqCopy(CONNECT_STR, QUEUE_NAME, QUEUE2_NAME);
    // ensure it moved
    assertThat(asqPeek(CONNECT_STR, QUEUE2_NAME)).isEqualTo(PAYLOAD);
    // cleanup
    asqConsume(CONNECT_STR, QUEUE_NAME);
    asqConsume(CONNECT_STR, QUEUE2_NAME);
  }

  @Test
  void testAsqMoveAll() {
    // place messages
    var numMsgs = 6;
    for (var i = 0; i < numMsgs; i++) {
      asqSend(CONNECT_STR, QUEUE_NAME, PAYLOAD);
    }
    // verify messages are on the source queue
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () ->
                assertThat(asqQueueDepth(CONNECT_STR, QUEUE_NAME)).isGreaterThanOrEqualTo(numMsgs));
    // move the messages
    asqMoveAll(CONNECT_STR, QUEUE_NAME, QUEUE2_NAME);
    // verify messages are on the source queue
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () ->
                assertThat(asqQueueDepth(CONNECT_STR, QUEUE2_NAME))
                    .isGreaterThanOrEqualTo(numMsgs));
    // cleanup
    asqPurge(CONNECT_STR, QUEUE2_NAME);
  }
}
