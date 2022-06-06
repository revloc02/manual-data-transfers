package forest.colver.datatransfer.messaging;

import java.util.Enumeration;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

public class DisplayUtils {

  private static final int DEFAULT_PAYLOAD_OUTPUT_LEN = 100;

  public static String stringFromMessage(Message message) {
    var md = new MessageDisplayer(message);
    return md.createString();
  }

  public static String stringFromMessage(Message message, int payloadOutputTrunc, boolean listJmsProps) {
    var md = new MessageDisplayer(message);
    return md.createString(payloadOutputTrunc, listJmsProps);
  }

  public static String createStringFromMessage(Message message) {
    return createStringFromMessage(message, DEFAULT_PAYLOAD_OUTPUT_LEN, true);
  }

  public static StringBuilder stringFromTextMessage(
      Message message, int payloadOutputTrunc, boolean listJmsProps) {
    var sb = new StringBuilder();
    sb.append("Message Type: TextMessage\n");
    appendMessageProps(message, listJmsProps, sb);
    var payload = "null";
    try {
      if (message.getBody(Object.class) != null) {
        payload = ((TextMessage) message).getText();
      }
    } catch (JMSException e) {
      e.printStackTrace();
    }
    appendPayoadString(payload, payloadOutputTrunc, sb);
    return sb;
  }

  public static StringBuilder stringFromBytesMessage(
      Message message, int payloadOutputTrunc, boolean listJmsProps) {
    var sb = new StringBuilder();
    sb.append("Message Type: BytesMessage\n");
    appendMessageProps(message, listJmsProps, sb);
    byte[] bytes = null;
    BytesMessage bytesMessage = (BytesMessage) message;
    try {
      // When the message is first created the body of the message is in write-only mode. After
      // the first call to the reset method has been made, the message is in read-only mode.
      bytesMessage.reset();
      bytes = new byte[(int) bytesMessage.getBodyLength()];
      bytesMessage.readBytes(bytes);
    } catch (JMSException e) {
      e.printStackTrace();
    }
    appendPayoadString(new String(bytes), payloadOutputTrunc, sb);
    return sb;
  }

  /**
   * Creates a multi-lined string of the message properties, headers and payload
   *
   * @param payloadOutputTrunc the number of characters of the payload that will be output
   * @param listJmsProps include JMSMessage properties in the output String
   */
  public static String createStringFromMessage(
      Message message, int payloadOutputTrunc, boolean listJmsProps) {
    var sb = new StringBuilder("\n"); // always start with a newline
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
    sb.append("\n"); // always end with a newline
    return sb.toString();
  }

  private static void appendPayoadString(String payload, int payloadOutputTrunc, StringBuilder sb) {
    sb.append(
        String.format(
            "Payload (truncated to "
                + payloadOutputTrunc
                + " chars): %1."
                + payloadOutputTrunc
                + "s",
            payload));
  }

  private static void appendMessageProps(Message message, boolean listJmsProps, StringBuilder sb) {
    try {
      appendJmsProps(message, listJmsProps, sb);
      appendCustomProps(message, sb);
    } catch (JMSException e) {
      e.printStackTrace();
    }
  }

  private static void appendCustomProps(Message message, StringBuilder sb) throws JMSException {
    var tab = "  ";
    sb.append("Custom Properties:\n");
    for (Enumeration<String> e = message.getPropertyNames(); e.hasMoreElements(); ) {
      var s = e.nextElement();
      sb.append(String.format("%s%-20s = %s%n", tab, s, message.getObjectProperty(s)));
    }
  }

  private static void appendJmsProps(Message message, boolean listJmsProps, StringBuilder sb)
      throws JMSException {
    var tab = "  ";
    final String msgPrefix = "%s%-16s = %s%n";
    if (listJmsProps) {
      sb.append("JMS Properties:\n")
          .append(String.format(msgPrefix, tab, "JMSMessageID", message.getJMSMessageID()))
          .append(String.format(msgPrefix, tab, "JMSPriority", message.getJMSPriority()))
          .append(String.format(msgPrefix, tab, "JMSRedelivered", message.getJMSRedelivered()))
          .append(String.format(msgPrefix, tab, "JMSDestination", message.getJMSDestination()))
          .append(String.format(msgPrefix, tab, "JMSDeliveryTime", message.getJMSDeliveryTime()))
          .append(String.format(msgPrefix, tab, "JMSExpiration", message.getJMSExpiration()))
          .append(String.format(msgPrefix, tab, "JMSCorrelationID", message.getJMSCorrelationID()));
    }
  }
}
