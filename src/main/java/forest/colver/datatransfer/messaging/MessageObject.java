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

public class MessageObject {

  private static final Logger LOG = LoggerFactory.getLogger(MessageObject.class);

  private Message message;
  private String messageType;
  private String payload;
  private Map<String, Object> jmsHeaders;
  private Map<String, String> customHeaders;

  public MessageObject(Message message) {
    if (message != null) {
      this.message = message;
      if (message instanceof TextMessage textMessage) {
        this.messageType = "Text";
        try {
          this.payload = textMessage.getText();
        } catch (JMSException e) {
          e.printStackTrace();
        }
      } else if (message instanceof BytesMessage bytesMessage) {
        this.messageType = "Bytes";
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
        this.payload = new String(bytes);
      } else if (message instanceof ObjectMessage) {
        this.messageType = "Object";
      } else if (message instanceof StreamMessage) {
        this.messageType = "Stream";
      } else if (message instanceof MapMessage) {
        this.messageType = "Map";
      } else {
        this.messageType = "unknown";
      }
      getJmsProps(message);
      this.customHeaders = getCustomHeaders(message);
    } else {
      LOG.info("Message is null.");
    }
  }

  private void getJmsProps(Message message) {
    try {
      this.jmsHeaders.put("JMSMessageID", message.getJMSMessageID());
      this.jmsHeaders.put("JMSPriority", message.getJMSPriority());
      this.jmsHeaders.put("JMSRedelivered", message.getJMSRedelivered());
      this.jmsHeaders.put("JMSDestination", message.getJMSDestination());
      this.jmsHeaders.put("JMSDeliveryTime", message.getJMSDeliveryTime());
      this.jmsHeaders.put("JMSExpiration", message.getJMSExpiration());
      this.jmsHeaders.put("JMSCorrelationID", message.getJMSCorrelationID());
    } catch (JMSException e) {
      e.printStackTrace();
    }
  }

  private Map<String, String> getCustomHeaders(Message message) {
    Map<String, String> map = new java.util.HashMap<>(Map.of());
    try {
      for (Enumeration<String> e = message.getPropertyNames(); e.hasMoreElements(); ) {
        var s = e.nextElement();
        map.put(s, (String) message.getObjectProperty(s));
      }
    } catch (JMSException e) {
      e.printStackTrace();
    }
    return map;
  }

  public String displayMessage() {
    var sb = new StringBuilder("\n"); // always start with a newline
    sb.append("Message Type: ").append(messageType).append("\n");

    var tab = "  ";
    final String fmt = "%s%-20s = %s%n";
    sb.append("JMS Properties:\n")
        .append(String.format(fmt, tab, "JMSMessageID", jmsHeaders.get("JMSMessageID")))
        .append(String.format(fmt, tab, "JMSPriority", jmsHeaders.get("JMSPriority")))
        .append(String.format(fmt, tab, "JMSRedelivered", jmsHeaders.get("JMSRedelivered")))
        .append(String.format(fmt, tab, "JMSDestination", jmsHeaders.get("JMSDestination")))
        .append(String.format(fmt, tab, "JMSDeliveryTime", jmsHeaders.get("JMSDeliveryTime")))
        .append(String.format(fmt, tab, "JMSExpiration", jmsHeaders.get("JMSExpiration")))
        .append(String.format(fmt, tab, "JMSCorrelationID", jmsHeaders.get("JMSCorrelationID")));

    sb.append("Custom Properties:\n");
    for (Map.Entry<String, String> entry : customHeaders.entrySet()) {
      sb.append(String.format(fmt, tab, entry.getKey(), entry.getValue()));
    }

    int payloadOutputTrunc = 100;
    sb.append(
        String.format(
            "Payload (truncated to "
                + payloadOutputTrunc
                + " chars): %1."
                + payloadOutputTrunc
                + "s",
            payload));

    return sb.toString();
  }
}
