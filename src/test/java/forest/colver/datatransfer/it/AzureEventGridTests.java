package forest.colver.datatransfer.it;

//import static lds.emx.mdt.azure.EventGridOperations.receiveMessage;

import static forest.colver.datatransfer.azure.EventGridOperations.aegSendMessage;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbConsume;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.connect;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_EVENTGRID_HOST;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_EVENTGRID_TOPIC_KEY;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_EVENTGRID_SUBSCRIPTION_QUEUE;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY;
import static forest.colver.datatransfer.azure.Utils.createEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureEventGridTests {

  private static final Logger LOG = LoggerFactory.getLogger(AzureEventGridTests.class);
  private static final String EVENTGRID_HOST = EMX_SANDBOX_EVENTGRID_HOST;
  private static final String EVENTGRID_TOPIC_KEY = EMX_SANDBOX_EVENTGRID_TOPIC_KEY;

  /**
   * Currently this test requires a sandbox account with resources allocated from
   * devops/terraform/azure/main.tf
   */
  @Test
  public void testSend() throws IOException {
    var id = "test";
    var subject = "AppEventA";
    var body = "this is the body";
    var eventType = "test.event.type";
    var dataVersion = "0.0.1";
    // todo: currently the timestamp is a hack, not sure how to fix that yet
    aegSendMessage(EVENTGRID_HOST, EVENTGRID_TOPIC_KEY, createEvent(id, subject, body, eventType,
        "2017-08-10T21:03:07+00:00", dataVersion));

    // asb queue is subscribed to event grid, retrieve the message from the queue and check it
    var msg = asbConsume(
        connect(EMX_SANDBOX_NAMESPACE, EMX_SANDBOX_EVENTGRID_SUBSCRIPTION_QUEUE,
            EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
            EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY));
    LOG.info("message body type: {}", msg.getMessageBody().getBodyType().name());
    var message =
        new ObjectMapper()
            .readValue(msg.getMessageBody().getBinaryData().get(0), HashMap.class);
    assertEquals(body, message.get("data"));
    assertEquals(eventType, message.get("eventType"));
    assertEquals(subject, message.get("subject"));
  }

}
