package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.SqsOperations.sqsDeleteMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDepth;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS1;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbConsume;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbPurge;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbRead;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbSend;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.connect;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.messageCount;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY;
import static forest.colver.datatransfer.azure.Utils.createIMessage;
import static forest.colver.datatransfer.config.Utils.defaultPayload;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStampFormatted;
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

public class HybridSqsAndAsbQueueIntTests {

  private static final Logger LOG = LoggerFactory.getLogger(HybridSqsAndAsbQueueIntTests.class);
  private static final String SQS1 = EMX_SANDBOX_TEST_SQS1;
  private static final AwsCredentialsProvider awsCreds = getEmxSbCreds();
  private final ConnectionStringBuilder asbCreds = connect(EMX_SANDBOX_NAMESPACE,
      EMX_SANDBOX_FOREST_QUEUE,
      EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);

  @Test
  public void testMoveSqsToAsbQueue() {
    // place a message on SQS
    LOG.info("Interacting with: sqs={}", SQS1);
    var messageProps = Map.of("timestamp", getTimeStampFormatted(), "key2", "value2", "key3",
        "value3");
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
    assertThat(message.getProperties().get("key2")).isEqualTo("value2");
    assertThat(message.getProperties().get("key3")).isEqualTo("value3");

    // clean up
    asbConsume(asbCreds);
  }

  @Test
  public void testMoveAsbQueueToSqs() {
    // send a message
    Map<String, Object> properties = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    asbSend(asbCreds, createIMessage(defaultPayload, properties));
    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(messageCount(asbCreds)).isEqualTo(1));

    moveOneAsbQueueToSqs(asbCreds, awsCreds, SQS1);

    // check that it arrived
    var msg = sqsReadOneMessage(awsCreds, SQS1);
    assert msg != null;
    assertThat(msg.body()).isEqualTo(defaultPayload);
    assertThat(msg.hasMessageAttributes()).isEqualTo(true);
    assertThat(msg.messageAttributes().get("specificKey").stringValue()).isEqualTo("specificValue");

    // cleanup
    sqsDeleteMessage(awsCreds, SQS1, msg);
  }

  @Test
  public void testMoveAllSqsToAsbQueue() {
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
        .untilAsserted(
            () -> assertThat(messageCount(asbCreds)).isEqualTo(numMsgs));

    // cleanup
    asbPurge(asbCreds);
  }
}
