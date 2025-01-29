package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.SqsOperations.sqsDeleteMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDepth;
import static forest.colver.datatransfer.aws.SqsOperations.sqsPurge;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS1;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbConsume;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbQueuePurge;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbRead;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbSend;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.connectAsbQ;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.messageCount;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY;
import static forest.colver.datatransfer.azure.Utils.createIMessage;
import static forest.colver.datatransfer.config.Utils.defaultPayload;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStampFormatted;
import static forest.colver.datatransfer.hybrid.SqsAndAsbQueue.copyAllSqsToAsbQueue;
import static forest.colver.datatransfer.hybrid.SqsAndAsbQueue.copyOneAsbQueueToSqs;
import static forest.colver.datatransfer.hybrid.SqsAndAsbQueue.copyOneSqsToAsbQueue;
import static forest.colver.datatransfer.hybrid.SqsAndAsbQueue.moveAllAsbQueueToSqs;
import static forest.colver.datatransfer.hybrid.SqsAndAsbQueue.moveAllSqsToAsbQueue;
import static forest.colver.datatransfer.hybrid.SqsAndAsbQueue.moveOneAsbQueueToSqs;
import static forest.colver.datatransfer.hybrid.SqsAndAsbQueue.moveOneSqsToAsbQueue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
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
  private final ConnectionStringBuilder asbCreds =
      connectAsbQ(
          EMX_SANDBOX_NAMESPACE,
          EMX_SANDBOX_FOREST_QUEUE,
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
    moveOneSqsToAsbQueue(awsCreds, SQS1, asbCreds);

    // read that message
    var message = asbRead(asbCreds);

    // check it
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    assertThat(body).isEqualTo(payload);
    assertThat(message.getProperties()).containsEntry("key2", "value2");
    assertThat(message.getProperties()).containsEntry("key3", "value3");

    // clean up
    asbConsume(asbCreds);
  }

  @Test
  void testMoveAsbQueueToSqs() {
    // send a message to ASB queue
    Map<String, Object> properties =
        Map.of("timestamp", getTimeStampFormatted(), "specificKey", "specificValue");
    asbSend(asbCreds, createIMessage(defaultPayload, properties));
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(messageCount(asbCreds)).isEqualTo(1));

    moveOneAsbQueueToSqs(asbCreds, awsCreds, SQS1);

    // check that it arrived
    var msg = sqsReadOneMessage(awsCreds, SQS1);
    assert msg != null;
    assertThat(msg.body()).isEqualTo(defaultPayload);
    assertThat(msg.hasMessageAttributes()).isTrue();
    assertThat(msg.messageAttributes().get("specificKey").stringValue()).isEqualTo("specificValue");

    // cleanup
    sqsDeleteMessage(awsCreds, SQS1, msg);
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

    moveAllSqsToAsbQueue(awsCreds, SQS1, asbCreds);

    // verify messages are on the ASB queue
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(messageCount(asbCreds)).isEqualTo(numMsgs));

    // cleanup
    asbQueuePurge(asbCreds);
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
      asbSend(asbCreds, createIMessage(defaultPayload));
    }
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(messageCount(asbCreds)).isGreaterThanOrEqualTo(numMsgs));

    moveAllAsbQueueToSqs(asbCreds, SQS1, awsCreds);

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
    copyOneSqsToAsbQueue(awsCreds, SQS1, asbCreds);

    // read that message
    var message = asbRead(asbCreds);

    // check it
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    assertThat(body).isEqualTo(payload);
    assertThat(message.getProperties()).containsEntry("key2", "value2");
    assertThat(message.getProperties()).containsEntry("key3", "value3");

    // clean up
    asbConsume(asbCreds);
    var msg = sqsReadOneMessage(awsCreds, SQS1);
    sqsDeleteMessage(awsCreds, SQS1, msg);
  }

  @Test
  void testCopyOneAsbQueueToSqs() {
    // send a message to ASB queue
    Map<String, Object> properties =
        Map.of("timestamp", getTimeStampFormatted(), "specificKey", "specificValue");
    asbSend(asbCreds, createIMessage(defaultPayload, properties));
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(messageCount(asbCreds)).isEqualTo(1));

    // copy the message to SQS
    copyOneAsbQueueToSqs(asbCreds, awsCreds, SQS1);

    // check that it arrived on SQS
    var msg = sqsReadOneMessage(awsCreds, SQS1);
    assert msg != null;
    assertThat(msg.body()).isEqualTo(defaultPayload);
    assertThat(msg.hasMessageAttributes()).isTrue();
    assertThat(msg.messageAttributes().get("specificKey").stringValue()).isEqualTo("specificValue");

    // cleanup
    sqsDeleteMessage(awsCreds, SQS1, msg);
    asbConsume(asbCreds);
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

    copyAllSqsToAsbQueue(awsCreds, SQS1, asbCreds);

    // verify messages are on the ASB queue
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(messageCount(asbCreds)).isEqualTo(numMsgs));

    // verify messages are still on the sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(awsCreds, SQS1)).isEqualTo(numMsgs));

    // cleanup
    asbQueuePurge(asbCreds);
    sqsPurge(awsCreds, SQS1);
  }
}
