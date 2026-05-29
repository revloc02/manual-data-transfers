package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_SA_CONN_STR;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqConsume;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqCopy;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqCopyAll;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqMove;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqMoveAll;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqMoveMatching;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqPeek;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqPurge;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqQueueDepth;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqSend;
import static forest.colver.datatransfer.azure.StorageQueueOperations.asqSendMultipleUniqueMessages;
import static forest.colver.datatransfer.config.ConfigUtils.generateUniqueStrings;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.azure.storage.queue.models.QueueMessageItem;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
    var message = asqConsume(CONNECT_STR, QUEUE_NAME).orElseThrow();
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
    List<Future<Optional<QueueMessageItem>>> futuresList = new ArrayList<>();
    for (var task = 0; task < numMsgs; task++) {
      futuresList.add(es.submit(() -> asqConsume(CONNECT_STR, QUEUE_NAME)));
    }
    LOG.info("Tasks submitted: futuresList.size={}", futuresList.size());

    for (Future<Optional<QueueMessageItem>> future : futuresList) {
      // remember, future.get() blocks execution until the task is complete
      uuids.remove(future.get().orElseThrow().getBody().toString());
      LOG.info("removed {}", future.get().orElseThrow().getBody());
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

  @Test
  void testAsqCopyAll() {
    // start clean so depth assertions are exact
    asqPurge(CONNECT_STR, QUEUE_NAME);
    asqPurge(CONNECT_STR, QUEUE2_NAME);

    // send unique messages so we can verify each one was copied (and no duplicates)
    var numMsgs = 5;
    var uuids = generateUniqueStrings(numMsgs);
    asqSendMultipleUniqueMessages(CONNECT_STR, QUEUE_NAME, uuids);

    // verify messages landed on the source queue
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(asqQueueDepth(CONNECT_STR, QUEUE_NAME)).isEqualTo(numMsgs));

    // copy all messages — should leave source intact
    var copied = asqCopyAll(CONNECT_STR, QUEUE_NAME, QUEUE2_NAME);
    assertThat(copied).isEqualTo(numMsgs);

    // verify destination has the copies
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () -> assertThat(asqQueueDepth(CONNECT_STR, QUEUE2_NAME)).isEqualTo(numMsgs));

    // verify source still has the originals (after the visibility timeout has expired)
    await()
        .pollInterval(Duration.ofSeconds(5))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(asqQueueDepth(CONNECT_STR, QUEUE_NAME)).isEqualTo(numMsgs));

    // cleanup
    asqPurge(CONNECT_STR, QUEUE_NAME);
    asqPurge(CONNECT_STR, QUEUE2_NAME);
  }

  @Test
  void testAsqMoveMatching() {
    // start clean
    asqPurge(CONNECT_STR, QUEUE_NAME);
    asqPurge(CONNECT_STR, QUEUE2_NAME);

    // send a mix of matches and non-matches
    asqSend(CONNECT_STR, QUEUE_NAME, "MATCH-alpha");
    asqSend(CONNECT_STR, QUEUE_NAME, "skip-beta");
    asqSend(CONNECT_STR, QUEUE_NAME, "MATCH-gamma");
    asqSend(CONNECT_STR, QUEUE_NAME, "skip-delta");

    // verify all four arrived
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(asqQueueDepth(CONNECT_STR, QUEUE_NAME)).isEqualTo(4));

    // move only messages whose body starts with "MATCH-"
    var moved =
        asqMoveMatching(CONNECT_STR, QUEUE_NAME, QUEUE2_NAME, body -> body.startsWith("MATCH-"));
    assertThat(moved).isEqualTo(2);

    // destination should have exactly the two matches
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(asqQueueDepth(CONNECT_STR, QUEUE2_NAME)).isEqualTo(2));

    // source should retain the two non-matches — and they should be visible immediately
    // (this is the explicit-release pattern: non-matches were released with Duration.ZERO,
    // so they don't sit invisible for 30 sec). Use a tight timeout to verify that property.
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(asqQueueDepth(CONNECT_STR, QUEUE_NAME)).isEqualTo(2));

    // cleanup
    asqPurge(CONNECT_STR, QUEUE_NAME);
    asqPurge(CONNECT_STR, QUEUE2_NAME);
  }
}
