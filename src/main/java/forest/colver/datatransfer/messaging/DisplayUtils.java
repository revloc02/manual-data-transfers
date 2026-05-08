package forest.colver.datatransfer.messaging;

import static forest.colver.datatransfer.messaging.JmsUtils.extractJmsHeaders;
import static forest.colver.datatransfer.messaging.JmsUtils.extractMsgProperties;
import static forest.colver.datatransfer.messaging.JmsUtils.getJmsMsgPayload;

import jakarta.jms.BytesMessage;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.ObjectMessage;
import jakarta.jms.StreamMessage;
import jakarta.jms.TextMessage;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DisplayUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DisplayUtils.class);
  private static final int DEFAULT_PAYLOAD_OUTPUT_LEN = 100;

  private DisplayUtils() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

  public static String stringFromMessage(Message message) {
    return stringFromMessage(message, DEFAULT_PAYLOAD_OUTPUT_LEN, false);
  }

  public static void displayMessage(Message message) {
    LOG.info(stringFromMessage(message));
  }

  public static String stringFromMessage(
      Message message, int payloadOutputTrunc, boolean listJmsProps) {
    var sb = new StringBuilder("\n");
    sb.append("Message Type: ").append(resolveMessageType(message)).append("\n");

    var tab = "  ";
    var fmt = "%s%s%-20s = %s%n";
    if (listJmsProps) {
      var jmsHeaders = extractJmsHeaders(message);
      sb.append(tab).append("JMS Properties:\n");
      for (Map.Entry<String, String> entry : jmsHeaders.entrySet()) {
        sb.append(String.format(fmt, tab, tab, entry.getKey(), entry.getValue()));
      }
    }

    var customProps = extractMsgProperties(message);
    sb.append(tab).append("Custom Properties:\n");
    for (Map.Entry<String, String> entry : customProps.entrySet()) {
      sb.append(String.format(fmt, tab, tab, entry.getKey(), entry.getValue()));
    }

    var payload = getJmsMsgPayload(message);
    appendPayloadString(payload, payloadOutputTrunc, tab, sb);

    return sb.toString();
  }

  public static String createStringFromMessage(Message message) {
    return createStringFromMessage(message, DEFAULT_PAYLOAD_OUTPUT_LEN, true);
  }

  public static StringBuilder stringFromTextMessage(
      Message message, int payloadOutputTrunc, boolean listJmsProps) {
    var sb = new StringBuilder();
    sb.append("Message Type: TextMessage\n");
    appendMessageProps(message, listJmsProps, sb);
    var payload = getJmsMsgPayload(message);
    appendPayloadString(payload, payloadOutputTrunc, "", sb);
    return sb;
  }

  public static StringBuilder stringFromBytesMessage(
      Message message, int payloadOutputTrunc, boolean listJmsProps) {
    var sb = new StringBuilder();
    sb.append("Message Type: BytesMessage\n");
    appendMessageProps(message, listJmsProps, sb);
    var payload = getJmsMsgPayload(message);
    appendPayloadString(payload, payloadOutputTrunc, "", sb);
    return sb;
  }

  public static String createStringFromMessage(
      Message message, int payloadOutputTrunc, boolean listJmsProps) {
    var sb = new StringBuilder("\n");
    if (message != null) {
      if (message instanceof TextMessage) {
        sb.append(stringFromTextMessage(message, payloadOutputTrunc, listJmsProps));
      } else if (message instanceof BytesMessage) {
        sb.append(stringFromBytesMessage(message, payloadOutputTrunc, listJmsProps));
      } else if (message instanceof StreamMessage) {
        sb.append("Message Type: StreamMessage\n");
        appendMessageProps(message, listJmsProps, sb);
      } else {
        sb.append("Message Type: Unknown\n");
      }
    } else {
      sb.append("Message is null.");
    }
    sb.append("\n");
    return sb.toString();
  }

  private static String resolveMessageType(Message message) {
    if (message instanceof TextMessage) {
      return "Text";
    } else if (message instanceof BytesMessage) {
      return "Bytes";
    } else if (message instanceof ObjectMessage) {
      return "Object";
    } else if (message instanceof StreamMessage) {
      return "Stream";
    } else if (message instanceof MapMessage) {
      return "Map";
    }
    return "unknown";
  }

  private static void appendPayloadString(
      String payload, int payloadOutputTrunc, String prefix, StringBuilder sb) {
    sb.append(
        String.format(
            prefix
                + "Payload (truncated to "
                + payloadOutputTrunc
                + " chars): %1."
                + payloadOutputTrunc
                + "s",
            payload));
  }

  private static void appendMessageProps(Message message, boolean listJmsProps, StringBuilder sb) {
    var tab = "  ";
    var fmt = "%s%-20s = %s%n";
    if (listJmsProps) {
      var jmsHeaders = extractJmsHeaders(message);
      sb.append("JMS Properties:\n");
      for (Map.Entry<String, String> entry : jmsHeaders.entrySet()) {
        sb.append(String.format(fmt, tab, entry.getKey(), entry.getValue()));
      }
    }
    var customProps = extractMsgProperties(message);
    sb.append("Custom Properties:\n");
    for (Map.Entry<String, String> entry : customProps.entrySet()) {
      sb.append(String.format(fmt, tab, entry.getKey(), entry.getValue()));
    }
  }
}
