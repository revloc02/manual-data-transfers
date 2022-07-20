package forest.colver.datatransfer.messaging;

import java.util.Enumeration;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
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

  private Message message;
  private String messageType;
  private String payload;
  private Map<String, String> jmsHeaders;
  private Map<String, String> customHeaders;

  public MessageDisplayer(Message message) {
    // todo: note that much of this is copied into messaging.Utils and as such is duplicated, that should be reconciled.
    if (message != null) {
      this.message = message;
      // todo: can I display the size of the message?
      if (message instanceof TextMessage textMessage) {
        this.messageType = "Text";
        this.payload = payloadFromTextMessage(textMessage);
      } else if (message instanceof BytesMessage bytesMessage) {
        this.messageType = "Bytes";
        this.payload = payloadFromBytesMessage(bytesMessage);
      } else if (message instanceof ObjectMessage objectMessage) {
        this.messageType = "Object";
        this.payload = payloadFromObjectMessage(objectMessage);
      } else if (message instanceof StreamMessage) {
        this.messageType = "Stream";
        this.payload = "ERROR: Extracting payload from a StreamMessage is not implemented yet.";
      } else if (message instanceof MapMessage) {
        this.messageType = "Map";
        this.payload = "ERROR: Extracting payload from a MapMessage is not implemented yet.";
      } else {
        this.messageType = "unknown";
      }
      this.jmsHeaders = constructJmsProps(message);
      this.customHeaders = constructCustomHeaders(message);
    } else {
      LOG.info("Message is null.");
    }
  }

  private String payloadFromTextMessage(TextMessage textMessage) {
    var payload = "";
    try {
      payload = textMessage.getText();
    } catch (JMSException e) {
      e.printStackTrace();
    }
    return payload;
  }

  private String payloadFromBytesMessage(BytesMessage bytesMessage) {
    byte[] bytes = null;
    try {
      // When the message is first created the body of the message is in write-only mode. After
      // the first call to the reset method has been made, the message is in read-only mode.
      bytesMessage.reset();
      bytes = new byte[(int) bytesMessage.getBodyLength()];
      bytesMessage.readBytes(bytes);
    } catch (JMSException e) {
      e.printStackTrace();
    }
    assert bytes != null;
    return new String(bytes);
  }

  private String payloadFromObjectMessage(ObjectMessage objectMessage) {
    var payload = "";
    try {
      payload = objectMessage.getObject().toString();
    } catch (JMSException e) {
      e.printStackTrace();
    }
    return payload;
  }

  private Map<String, String> constructJmsProps(Message message) {
    Map<String, String> map = new java.util.HashMap<>(Map.of());
    try {
      map.put("JMSMessageID", message.getJMSMessageID());
      map.put("JMSPriority", String.valueOf(message.getJMSPriority()));
      map.put("JMSRedelivered", String.valueOf(message.getJMSRedelivered()));
      map.put("JMSDestination", String.valueOf(message.getJMSDestination()));
      map.put("JMSDeliveryTime", String.valueOf(message.getJMSDeliveryTime()));
      map.put("JMSExpiration", String.valueOf(message.getJMSExpiration()));
      map.put("JMSCorrelationID", message.getJMSCorrelationID());
    } catch (JMSException e) {
      e.printStackTrace();
    }
    return map;
  }

  private Map<String, String> constructCustomHeaders(Message message) {
    Map<String, String> map = new java.util.HashMap<>(Map.of());
    try {
      for (Enumeration<String> e = message.getPropertyNames(); e.hasMoreElements(); ) {
        var s = e.nextElement();
//        LOG.info("key={}; val={}", s, message.getObjectProperty(s));
        map.put(s, message.getObjectProperty(s).toString());
      }
    } catch (JMSException e) {
      e.printStackTrace();
    }
    return map;
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
