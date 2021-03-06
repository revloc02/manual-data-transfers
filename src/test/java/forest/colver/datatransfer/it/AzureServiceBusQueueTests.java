package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbConsume;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbDlq;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbMove;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbPurge;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbRead;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbSend;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.connect;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.messageCount;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE2;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE_W_DLQ;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE_W_FORWARD;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE_W_TTL;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY;
import static forest.colver.datatransfer.azure.Utils.createIMessage;
import static forest.colver.datatransfer.config.Utils.defaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStampFormatted;
import static forest.colver.datatransfer.config.Utils.pause;
import static org.assertj.core.api.Assertions.assertThat;

import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureServiceBusQueueTests {

  private static final Logger LOG = LoggerFactory.getLogger(AzureServiceBusQueueTests.class);
  private final ConnectionStringBuilder creds = connect(EMX_SANDBOX_NAMESPACE,
      EMX_SANDBOX_FOREST_QUEUE,
      EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);

  @Test
  public void testSend() {
    // send a message
    Map<String, Object> properties = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    asbSend(creds, createIMessage(defaultPayload, properties));
    pause(2);
    assertThat(messageCount(creds, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(1);

    // read that message
    var message = asbRead(creds);

    // check it
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getProperties().get("specificKey")).isEqualTo("specificValue");

    // clean up
    asbConsume(creds);
  }

  @Test
  public void testMove() throws ServiceBusException, InterruptedException {
    // send a message
    Map<String, Object> properties = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    asbSend(creds, createIMessage(defaultPayload, properties));
    pause(2);
    assertThat(messageCount(creds, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(1);

    // move that message
    var toCreds = connect(EMX_SANDBOX_NAMESPACE, EMX_SANDBOX_FOREST_QUEUE2,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    asbMove(creds, toCreds);
    pause(2);
    assertThat(messageCount(toCreds, EMX_SANDBOX_FOREST_QUEUE2)).isEqualTo(1);

    // check it
    var message = asbRead(toCreds);
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getProperties().get("specificKey")).isEqualTo("specificValue");

    // clean up
    asbPurge(toCreds);
    asbPurge(creds);
  }

  @Test
  public void testConsume() {
    // send a message
    Map<String, Object> properties = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    asbSend(creds, createIMessage(defaultPayload, properties));
    pause(2);

    // retrieve that message
    var message = asbConsume(creds);
    assertThat(messageCount(creds, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(0);

    // check it
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getProperties().get("specificKey")).isEqualTo("specificValue");
  }

  @Test
  public void testPurge() {
    var message = createIMessage(defaultPayload);

    // send a bunch of messages
    var num = 3;
    // todo: there's gotta be a faster, better way to do this
    for (var i = 0; i < num; i++) {
      asbSend(creds, message);
    }
    pause(3);
    assertThat(messageCount(creds, EMX_SANDBOX_FOREST_QUEUE)).isGreaterThanOrEqualTo(num);

    // purge the queue
    assertThat(asbPurge(creds)).isEqualTo(3);
    assertThat(messageCount(creds, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(0);
  }

  @Test
  public void testDeadLetterQueue() {
    var dlqName = EMX_SANDBOX_FOREST_QUEUE_W_DLQ + "/$DeadLetterQueue";
    // connection string to the queue with a DLQ configured
    var credsQwDlq = connect(EMX_SANDBOX_NAMESPACE,
        EMX_SANDBOX_FOREST_QUEUE_W_DLQ,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    // connection string to the actual DLQ of the queue (with a DLQ configured)
    var credsQwDlq_Dlq = connect(EMX_SANDBOX_NAMESPACE, dlqName,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);

    // ensure the queue and the DLQ are clean
    asbPurge(credsQwDlq);
    asbPurge(credsQwDlq_Dlq);
    pause(3);

    // send a message to the queue
    Map<String, Object> properties = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    asbSend(credsQwDlq, createIMessage(defaultPayload, properties));
    pause(3);
    assertThat(messageCount(credsQwDlq, EMX_SANDBOX_FOREST_QUEUE_W_DLQ)).isEqualTo(1);

    // have the message on the queue move to the DLQ
    asbDlq(credsQwDlq);

    // check queue to see if main queue is empty
    pause(6);
    assertThat(messageCount(credsQwDlq, EMX_SANDBOX_FOREST_QUEUE_W_DLQ)).isEqualTo(0);

    // check the DLQ message
    var message = asbConsume(credsQwDlq_Dlq);
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getProperties().get("specificKey")).isEqualTo("specificValue");
  }

  /**
   * This should send a message to the queue and check that it arrived. Then it should wait until
   * the message expires and goes to the Dead-letter sub-queue and then check that it arrived there.
   * **WARNING:** This test is extremely unreliable, my theory for this is that we are demanding
   * these actions in seconds (within the time of a unit test running), and Azure Service Bus Queues
   * just was not built for that.
   */
  @Test
  public void testExpireToDeadLetterQueue() {
    var dlqName = EMX_SANDBOX_FOREST_QUEUE_W_TTL + "/$DeadLetterQueue";
    // connection string to the queue with a DLQ configured
    var credsQwTtl = connect(EMX_SANDBOX_NAMESPACE,
        EMX_SANDBOX_FOREST_QUEUE_W_TTL,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    // connection string to the actual DLQ of the queue (with a DLQ configured)
    var credsQwTtl_Dlq = connect(EMX_SANDBOX_NAMESPACE, dlqName,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);

    // ensure the queue and the DLQ are clean
    asbPurge(credsQwTtl);
    asbPurge(credsQwTtl_Dlq);
    pause(3);

    // send a message to the queue
    Map<String, Object> properties = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    asbSend(credsQwTtl, createIMessage(defaultPayload, properties));
    pause(3); // message expires in 10 seconds so don't wait too long
    assertThat(messageCount(credsQwTtl, EMX_SANDBOX_FOREST_QUEUE_W_TTL)).isEqualTo(1);

    // not sure what the poll cycle is when checking for expired messages, but my guess is >60 sec
    pause(100);
    // check queue to see if main queue is empty
    assertThat(messageCount(credsQwTtl, EMX_SANDBOX_FOREST_QUEUE_W_TTL)).isEqualTo(0);

    // check the DLQ message
    var message = asbConsume(credsQwTtl_Dlq);
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getProperties().get("specificKey")).isEqualTo("specificValue");
  }

  @Test
  public void testSendAutoForwarding() {
    // connection string to the queue with a forward_to configured
    var credsQwForward = connect(EMX_SANDBOX_NAMESPACE,
        EMX_SANDBOX_FOREST_QUEUE_W_FORWARD,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY, EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    var toCreds = connect(EMX_SANDBOX_NAMESPACE, EMX_SANDBOX_FOREST_QUEUE2,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);

    // send a message
    Map<String, Object> properties = Map.of("timestamp", getTimeStampFormatted(), "specificKey",
        "specificValue");
    asbSend(credsQwForward, createIMessage(defaultPayload, properties));

    // check message is not on original queue as it was forwarded
    pause(6);
    assertThat(messageCount(credsQwForward, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(0);

    // read the message on the other queue
    var message = asbRead(toCreds);

    // check it
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    assertThat(body).isEqualTo(defaultPayload);
    assertThat(message.getProperties().get("specificKey")).isEqualTo("specificValue");

    // clean up
    asbConsume(toCreds);
  }
}
