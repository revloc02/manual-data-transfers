package forest.colver.datatransfer.messaging;

import static forest.colver.datatransfer.messaging.Utils.extractJmsHeaders;
import static forest.colver.datatransfer.messaging.Utils.extractMsgProperties;
import static forest.colver.datatransfer.messaging.Utils.getJmsMsgPayload;

import java.util.Map;
import java.util.Objects;
import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The purpose of this object is to make displaying the details (various headers and payload) of the
 * message easy.
 */
public class MessageDisplayer {

  private static final Logger LOG = LoggerFactory.getLogger(MessageDisplayer.class);
  private static final int DEFAULT_PAYLOAD_OUTPUT_LEN = 100;

  private final Message message;
  private final String messageType;
  private final String payload;
  private final Map<String, String> jmsHeaders;
  private final Map<String, String> customHeaders;

  public MessageDisplayer(Message message) {
    Objects.requireNonNull(message, "message cannot be null.");

    this.message = message;
    this.payload = getJmsMsgPayload(message);
    this.jmsHeaders = extractJmsHeaders(message);
    this.customHeaders = extractMsgProperties(message);

    if (message instanceof TextMessage textMessage) {
      this.messageType = "Text";
    } else if (message instanceof BytesMessage bytesMessage) {
      this.messageType = "Bytes";
    } else if (message instanceof ObjectMessage objectMessage) {
      this.messageType = "Object";
    } else if (message instanceof StreamMessage) {
      this.messageType = "Stream";
    } else if (message instanceof MapMessage) {
      this.messageType = "Map";
    } else {
      this.messageType = "unknown";
    }
    // todo: can I display the size of the message?
  }

  public String createString() {
    return createString(DEFAULT_PAYLOAD_OUTPUT_LEN, false);
  }

  /**
   * Creates a string from the message payload and header properties.
   *
   * @param payloadOutputTrunc Payloads can be big, this limits the payload output to the number of
   * characters given in this int.
   * @param listJmsProps Whether to include the JMS properties as a part of the output.
   * @return A multi-line string of the message properties and payload.
   */
  public String createString(int payloadOutputTrunc, boolean listJmsProps) {
    var sb = new StringBuilder("\n"); // always start with a newline
    sb.append("Message Type: ").append(messageType).append("\n");

    var tab = "  ";
    final String fmt = "%s%s%-20s = %s%n";
    if (listJmsProps) {
      sb.append(tab).append("JMS Properties:\n")
          .append(String.format(fmt, tab, tab, "JMSMessageID", jmsHeaders.get("JMSMessageID")))
          .append(String.format(fmt, tab, tab, "JMSPriority", jmsHeaders.get("JMSPriority")))
          .append(String.format(fmt, tab, tab, "JMSRedelivered", jmsHeaders.get("JMSRedelivered")))
          .append(String.format(fmt, tab, tab, "JMSDestination", jmsHeaders.get("JMSDestination")))
          .append(
              String.format(fmt, tab, tab, "JMSDeliveryTime", jmsHeaders.get("JMSDeliveryTime")))
          .append(String.format(fmt, tab, tab, "JMSExpiration", jmsHeaders.get("JMSExpiration")))
          .append(
              String.format(fmt, tab, tab, "JMSCorrelationID", jmsHeaders.get("JMSCorrelationID")));
    }

    sb.append(tab).append("Custom Properties:\n");
    for (Map.Entry<String, String> entry : customHeaders.entrySet()) {
      sb.append(String.format(fmt, tab, tab, entry.getKey(), entry.getValue()));
    }

    sb.append(
        String.format(tab +
                "Payload (truncated to "
                + payloadOutputTrunc
                + " chars): %1."
                + payloadOutputTrunc
                + "s",
            payload));

    return sb.toString();
  }

  public void displayMessage() {
    LOG.info(createString());
  }

  public Message getMessage() {
    return message;
  }

  public String getMessageType() {
    return messageType;
  }

  public String getPayload() {
    return payload;
  }

  public Map<String, String> getJmsHeaders() {
    return jmsHeaders;
  }

  public Map<String, String> getCustomHeaders() {
    return customHeaders;
  }
}
