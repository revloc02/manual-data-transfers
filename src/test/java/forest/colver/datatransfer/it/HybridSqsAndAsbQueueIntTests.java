package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.AwsUtils.EMX_SANDBOX_TEST_SQS1;
import static forest.colver.datatransfer.aws.AwsUtils.getEmxSbCreds;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDeleteMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDepth;
import static forest.colver.datatransfer.aws.SqsOperations.sqsPurge;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_FOREST_QUEUE;
import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_NAMESPACE;
import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY;
import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY;
import static forest.colver.datatransfer.azure.AzureUtils.buildAsbConnectionString;
import static forest.colver.datatransfer.azure.AzureUtils.createServiceBusMessage;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbConsume;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbQueuePurge;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbRead;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbSend;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.messageCount;
import static forest.colver.datatransfer.config.ConfigUtils.defaultPayload;
import static forest.colver.datatransfer.config.ConfigUtils.getDefaultPayload;
import static forest.colver.datatransfer.config.ConfigUtils.getTimeStampFormatted;
import static forest.colver.datatransfer.hybrid.SqsAndAsbQueue.copyAllSqsToAsbQueue;
import static forest.colver.datatransfer.hybrid.SqsAndAsbQueue.copyOneAsbQueueToSqs;
import static forest.colver.datatransfer.hybrid.SqsAndAsbQueue.copyOneSqsToAsbQueue;
import static forest.colver.datatransfer.hybrid.SqsAndAsbQueue.moveAllAsbQueueToSqs;
import static forest.colver.datatransfer.hybrid.SqsAndAsbQueue.moveAllSqsToAsbQueue;
import static forest.colver.datatransfer.hybrid.SqsAndAsbQueue.moveOneAsbQueueToSqs;
import static forest.colver.datatransfer.hybrid.SqsAndAsbQueue.moveOneSqsToAsbQueue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

class HybridSqsAndAsbQueueIntTests {

  private static final Logger LOG = LoggerFactory.getLogger(HybridSqsAndAsbQueueIntTests.class);
  private static final String SQS1 = EMX_SANDBOX_TEST_SQS1;
  private static final AwsCredentialsProvider awsCreds = getEmxSbCreds();
  private static final String ASB_CONN_STR =
      buildAsbConnectionString(
          EMX_SANDBOX_NAMESPACE,
          EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
          EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);

  @Test
  void testMoveSqsToAsbQueue() {
    // place a message on SQS
    LOG.info("Interacting with: sqs={}", SQS1);
    var messageProps =
        Map.of("timestamp", getTimeStampFormatted(), "key2", "value2", "key3", "value3");
    var payload = getDefaultPayload();
    sqsSend(awsCreds, SQS1, payload, messageProps);

    // check that it arrived
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(awsCreds, SQS1)).isEqualTo(1));

    // move it to ASB queue
    moveOneSqsToAsbQueue(awsCreds, SQS1, ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE);

    // read that message
    var message = asbRead(ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE).orElseThrow();

    // check it
    var body = message.getBody().toString();
    assertThat(body).isEqualTo(payload);
    assertThat(message.getApplicationProperties()).containsEntry("key2", "value2");
    assertThat(message.getApplicationProperties()).containsEntry("key3", "value3");

    // clean up
    asbConsume(ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE);
  }

  @Test
  void testMoveAsbQueueToSqs() {
    // send a message to ASB queue
    Map<String, Object> properties =
        Map.of("timestamp", getTimeStampFormatted(), "specificKey", "specificValue");
    asbSend(
        ASB_CONN_STR,
        EMX_SANDBOX_FOREST_QUEUE,
        createServiceBusMessage(defaultPayload, properties));
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(1));

    moveOneAsbQueueToSqs(ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE, awsCreds, SQS1);

    // check that it arrived
    var msg = sqsReadOneMessage(awsCreds, SQS1);
    assertThat(msg).isPresent();
    assertThat(msg.get().body()).isEqualTo(defaultPayload);
    assertThat(msg.get().hasMessageAttributes()).isTrue();
    assertThat(msg.get().messageAttributes().get("specificKey").stringValue())
        .isEqualTo("specificValue");

    // cleanup
    sqsDeleteMessage(awsCreds, SQS1, msg.get());
  }

  @Test
  void testMoveAllSqsToAsbQueue() {
    LOG.info("Interacting with: sqs={} and ASB-queue={}", SQS1, EMX_SANDBOX_FOREST_QUEUE);
    // put messages on sqs
    var payload = getDefaultPayload();
    var numMsgs = 4;
    for (var i = 0; i < numMsgs; i++) {
      sqsSend(awsCreds, SQS1, payload);
    }

    // verify messages are on the sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(awsCreds, SQS1)).isEqualTo(numMsgs));

    moveAllSqsToAsbQueue(awsCreds, SQS1, ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE);

    // verify messages are on the ASB queue
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () ->
                assertThat(messageCount(ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE))
                    .isEqualTo(numMsgs));

    // cleanup
    asbQueuePurge(ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE);
  }

  /**
   * Tests the method that moves all messages from an ASB queue to an SQS. Note: the assertThat() in
   * both of the await() items uses .isGreaterThanOrEqualTo instead of .isEqualTo because if there
   * happens to be a network glitch, a subsequent run will still run successfully and in that
   * process clean the queues up.
   */
  @Test
  void testMoveAllAsbQueueToSqs() {
    // send messages to ASB queue
    var numMsgs = 7;
    for (var i = 0; i < numMsgs; i++) {
      asbSend(ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE, createServiceBusMessage(defaultPayload));
    }
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () ->
                assertThat(messageCount(ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE))
                    .isGreaterThanOrEqualTo(numMsgs));

    moveAllAsbQueueToSqs(ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE, SQS1, awsCreds);

    // verify messages are on the target sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(awsCreds, SQS1)).isGreaterThanOrEqualTo(numMsgs));

    // cleanup
    sqsPurge(awsCreds, SQS1);
  }

  @Test
  void testCopyOneSqsToAsbQueue() {
    // place a message on SQS
    LOG.info("Interacting with: sqs={}", SQS1);
    var messageProps =
        Map.of("timestamp", getTimeStampFormatted(), "key2", "value2", "key3", "value3");
    var payload = getDefaultPayload();
    sqsSend(awsCreds, SQS1, payload, messageProps);

    // check that it is on SQS
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(awsCreds, SQS1)).isEqualTo(1));

    // copy it to ASB queue
    copyOneSqsToAsbQueue(awsCreds, SQS1, ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE);

    // read that message
    var message = asbRead(ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE).orElseThrow();

    // check it
    var body = message.getBody().toString();
    assertThat(body).isEqualTo(payload);
    assertThat(message.getApplicationProperties()).containsEntry("key2", "value2");
    assertThat(message.getApplicationProperties()).containsEntry("key3", "value3");

    // clean up
    asbConsume(ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE);
    sqsDeleteMessage(awsCreds, SQS1, sqsReadOneMessage(awsCreds, SQS1).orElseThrow());
  }

  @Test
  void testCopyOneAsbQueueToSqs() {
    // send a message to ASB queue
    Map<String, Object> properties =
        Map.of("timestamp", getTimeStampFormatted(), "specificKey", "specificValue");
    asbSend(
        ASB_CONN_STR,
        EMX_SANDBOX_FOREST_QUEUE,
        createServiceBusMessage(defaultPayload, properties));
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(1));

    // copy the message to SQS
    copyOneAsbQueueToSqs(ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE, awsCreds, SQS1);

    // check that it arrived on SQS
    var msg = sqsReadOneMessage(awsCreds, SQS1);
    assertThat(msg).isPresent();
    assertThat(msg.get().body()).isEqualTo(defaultPayload);
    assertThat(msg.get().hasMessageAttributes()).isTrue();
    assertThat(msg.get().messageAttributes().get("specificKey").stringValue())
        .isEqualTo("specificValue");

    // cleanup
    sqsDeleteMessage(awsCreds, SQS1, msg.get());
    asbConsume(ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE);
  }

  @Test
  void testCopyAllSqsToAsbQueue() {
    LOG.info("Interacting with: sqs={} and ASB-queue={}", SQS1, EMX_SANDBOX_FOREST_QUEUE);
    // put messages on sqs
    var payload = getDefaultPayload();
    var numMsgs = 4;
    for (var i = 0; i < numMsgs; i++) {
      sqsSend(awsCreds, SQS1, payload);
    }

    // verify messages are on the sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(awsCreds, SQS1)).isEqualTo(numMsgs));

    copyAllSqsToAsbQueue(awsCreds, SQS1, ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE);

    // verify messages are on the ASB queue
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () ->
                assertThat(messageCount(ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE))
                    .isEqualTo(numMsgs));

    // verify messages are still on the sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(awsCreds, SQS1)).isEqualTo(numMsgs));

    // cleanup
    asbQueuePurge(ASB_CONN_STR, EMX_SANDBOX_FOREST_QUEUE);
    sqsPurge(awsCreds, SQS1);
  }
}
