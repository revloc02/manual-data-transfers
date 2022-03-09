package forest.colver.datatransfer.azure;

import static forest.colver.datatransfer.config.Utils.userCreds;

import com.azure.core.util.BinaryData;
import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.Message;
import com.microsoft.azure.servicebus.MessageBodyType;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Azure specific utils
 */
public class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  // Azure personal sandbox resources, currently Azure sandboxes are not working because they are not allowing creation of a Resource Group
  public static final String EMX_SANDBOX_FOREST_QUEUE = "forest-test-servicebus-queue";
  public static final String EMX_SANDBOX_FOREST_QUEUE2 = "forest-test-servicebus-queue2";

  // Team Azure Sandbox event grid resources
  public static final URI EMX_SANDBOX_NAMESPACE = URI.create(userCreds.getProperty("azure-emx-sandbox-forest-namespace"));
  public static final String EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY = userCreds.getProperty("azure-emx-sandbox-forest-ns-shared-access-policy");
  public static final String EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY = userCreds.getProperty("azure-emx-sandbox-forest-ns-shared-access-key");
  public static final String EMX_SANDBOX_EVENTGRID_SUBSCRIPTION_QUEUE = "emx-core-test-eventgrid-subscription-queue";
  public static final String EMX_SANDBOX_EVENTGRID_HOST = userCreds.getProperty("azure-emx-sandbox-eventgrid-host");
  public static final String EMX_SANDBOX_EVENTGRID_TOPIC_KEY = userCreds.getProperty("azure-emx-sandbox-eventgrid-topic-key");

  public static IMessage createIMessage(String payload, String label, String id,
      Map<String, Object> properties) {
    Message message = new Message(payload);
    message.setLabel(label);
    message.setMessageId(id);
    if (!properties.isEmpty()) {
      message.setProperties(properties);
    }
    return message;
  }

  public static IMessage createIMessage(String payload, Map<String, Object> properties) {
    return createIMessage(payload, "testMessage", "123", properties);
  }

  public static IMessage createIMessage(String payload) {
    return createIMessage(payload, Collections.emptyMap());
  }

  public static void displayIMessage(IMessage msg) {
    if (msg != null && msg.getMessageId() != null) {
      LOG.info("message body type: {}", msg.getMessageBody().getBodyType().name());
      if (msg.getMessageBody().getBodyType().equals(MessageBodyType.BINARY)) {
        List<byte[]> body = msg.getMessageBody().getBinaryData();
        body.forEach(bytes -> LOG.info("body: {}", new String(bytes, StandardCharsets.UTF_8)));
      }
    }
  }

  //"2017-08-10T21:03:07+00:00"
  public static List<BinaryData> createEvent() {
    // todo: eventTime is hard coded--a little hacky
    return createEvent("test", "AppEventA", "this is the body", "test.event.type",
        "2017-08-10T21:03:07+00:00", "0.0.1");
  }

  public static List<BinaryData> createEvent(String id, String subject, Object body,
      String eventType, String eventTime, String dataVersion) {
    var eventMap = Map.of("id", id, "data", body, "subject", subject,
        "eventTime", eventTime, "eventType", eventType, "dataVersion",
        dataVersion);
    List<BinaryData> events = new ArrayList<>();
    events.add(BinaryData.fromObject(eventMap));
    return events;
  }
}
