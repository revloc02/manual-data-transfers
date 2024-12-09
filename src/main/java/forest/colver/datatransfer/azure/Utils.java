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

/** Azure specific utils */
public class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  // Azure personal sandbox resources, currently Azure sandboxes are not working because they are
  // not allowing creation of a Resource Group
  public static final String EMX_SANDBOX_FOREST_QUEUE = "forest-test-servicebus-queue";
  public static final String EMX_SANDBOX_FOREST_QUEUE2 = "forest-test-servicebus-queue2";
  public static final String EMX_SANDBOX_FOREST_QUEUE_WITH_DLQ = "forest-test-servicebus-queue-dlq";
  public static final String EMX_SANDBOX_FOREST_QUEUE_WITH_FORWARD =
      "forest-test-servicebus-queue-forward";
  public static final String EMX_SANDBOX_FOREST_TTL_QUEUE =
      "forest-test-servicebus-queue-ttl-expiration";

  // Team Azure Sandbox event grid resources
  public static final URI EMX_SANDBOX_NAMESPACE =
      URI.create(userCreds.getProperty("azure-emx-sandbox-forest-namespace"));
  public static final String EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY =
      userCreds.getProperty("azure-emx-sandbox-forest-ns-shared-access-policy");
  public static final String EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY =
      userCreds.getProperty("azure-emx-sandbox-forest-ns-shared-access-key");
  public static final String EMX_SANDBOX_EVENTGRID_SUBSCRIPTION_QUEUE =
      userCreds.getProperty("azure-emx-sandbox-test-eventgrid-subscription-queue");
  public static final String EMX_SANDBOX_EVENTGRID_HOST =
      userCreds.getProperty("azure-emx-sandbox-eventgrid-host");
  public static final String EMX_SANDBOX_EVENTGRID_TOPIC_KEY =
      userCreds.getProperty("azure-emx-sandbox-eventgrid-topic-key");
  public static final String EMX_SANDBOX_SA_CONN_STR =
      "DefaultEndpointsProtocol=https;AccountName=emxsandbox;AccountKey="
          + userCreds.getProperty("azure-emx-sandbox-emxsandbox-sa-account-key")
          + ";EndpointSuffix=core.windows.net";
  public static final String EMX_PROD_EXT_EMCOR_PROD_SA_CONN_STR =
      userCreds.getProperty("azure-emx-prod-extemcorprod-sa-conn-str");
  public static final String EMX_EXTEMCORNP_SA_EXT_EMCOR_NP_SAS_TOKEN =
      userCreds.getProperty("azure-extemcornp-sa-ext-emcor-np-source-sas-token");
  public static final String EMX_SANDBOX_SA_FOREST_TEST_BLOB_SAS =
      userCreds.getProperty("azure-emx-sandbox-emxsandbox-sa-forest-test-blob-sas-token");
  public static final String EMX_SANDBOX_SA_FOREST_TEST_BLOB2_SAS =
      userCreds.getProperty("azure-emx-sandbox-emxsandbox-sa-forest-test-blob2-sas-token");
  public static final String EMX_SANDBOX_ASB_FOREST_TEST_QUEUE_CONN_STR =
      userCreds.getProperty("azure-emx-sandbox-forest-test-servicebus-queue-conn-str");

  public static IMessage createIMessage(
      String payload, String label, String id, Map<String, Object> properties) {
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

  // "2017-08-10T21:03:07+00:00"
  public static List<BinaryData> createEvent() {
    // todo: eventTime is hard coded--a little hacky
    return createEvent(
        "test",
        "AppEventA",
        "this is the body",
        "test.event.type",
        "2017-08-10T21:03:07+00:00",
        "0.0.1");
  }

  public static List<BinaryData> createEvent(
      String id,
      String subject,
      Object body,
      String eventType,
      String eventTime,
      String dataVersion) {
    var eventMap =
        Map.of(
            "id",
            id,
            "data",
            body,
            "subject",
            subject,
            "eventTime",
            eventTime,
            "eventType",
            eventType,
            "dataVersion",
            dataVersion);
    List<BinaryData> events = new ArrayList<>();
    events.add(BinaryData.fromObject(eventMap));
    return events;
  }
}
