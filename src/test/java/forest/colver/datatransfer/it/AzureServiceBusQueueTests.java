package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbConsume;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbMove;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbPurge;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbRead;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbSend;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.connect;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.messageCount;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_FOREST_QUEUE2;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY;
import static forest.colver.datatransfer.azure.Utils.createIMessage;
import static forest.colver.datatransfer.config.Utils.defaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStamp;
import static forest.colver.datatransfer.config.Utils.sleepo;
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
    Map<String, Object> properties = Map.of("timestamp", getTimeStamp(), "specificKey",
        "specificValue");
    asbSend(creds, createIMessage(defaultPayload, properties));
    sleepo(2_000);
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
    Map<String, Object> properties = Map.of("timestamp", getTimeStamp(), "specificKey",
        "specificValue");
    asbSend(creds, createIMessage(defaultPayload, properties));
    sleepo(2_000);
    assertThat(messageCount(creds, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(1);

    // move that message
    var toCreds = connect(EMX_SANDBOX_NAMESPACE, EMX_SANDBOX_FOREST_QUEUE2,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
        EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
    asbMove(creds, toCreds);
    sleepo(2_000);
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
    Map<String, Object> properties = Map.of("timestamp", getTimeStamp(), "specificKey",
        "specificValue");
    asbSend(creds, createIMessage(defaultPayload, properties));
    sleepo(2_000);

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
    sleepo(3_000);
    assertThat(messageCount(creds, EMX_SANDBOX_FOREST_QUEUE)).isGreaterThanOrEqualTo(num);

    // purge the queue
    assertThat(asbPurge(creds)).isEqualTo(3);
    assertThat(messageCount(creds, EMX_SANDBOX_FOREST_QUEUE)).isEqualTo(0);
  }

}
