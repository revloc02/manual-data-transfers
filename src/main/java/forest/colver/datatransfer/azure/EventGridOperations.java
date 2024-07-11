package forest.colver.datatransfer.azure;

import com.azure.core.credential.AzureKeyCredential;
import com.azure.core.util.BinaryData;
import com.azure.messaging.eventgrid.EventGridPublisherClientBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventGridOperations {

  private static final Logger LOG = LoggerFactory.getLogger(EventGridOperations.class);

  /**
   * Azure Event Grid Send Message
   *
   * @param eventGridHost The host name of the topic, e.g. topic1.westus2-1.eventgrid.azure.net
   * @param eventGridTopicKey The event-grid-topic key
   * @param events a map of event properties to send to the Event Grid
   */
  public static void aegSendMessage(String eventGridHost, String eventGridTopicKey,
      List<BinaryData> events) {
    var client2 = new EventGridPublisherClientBuilder()
        .endpoint(eventGridHost)
        .credential(new AzureKeyCredential(eventGridTopicKey))
        .buildCustomEventPublisherClient();
    client2.sendEvents(events);
    LOG.info("=====Sent message to Event Grid: {}", eventGridHost);
  }

}
