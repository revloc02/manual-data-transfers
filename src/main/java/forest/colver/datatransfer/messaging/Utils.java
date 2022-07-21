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
 * JMS specific utilities.
 */
public class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  /**
   * Retrieves the payload of the message as a String. The message type is checked, and then the
   * payload is extracted accordingly.
   *
   * @param message A javax.jms.Message.
   * @return The payload of the message as a String.
   */
  public static String getMessagePayload(Message message) {
    var payload = "";
    if (message != null) {
      if (message instanceof TextMessage textMessage) {
        payload = payloadFromTextMessage(textMessage);
      } else if (message instanceof BytesMessage bytesMessage) {
        payload = payloadFromBytesMessage(bytesMessage);
      } else if (message instanceof ObjectMessage objectMessage) {
        payload = payloadFromObjectMessage(objectMessage);
      } else if (message instanceof StreamMessage) {
        payload = "ERROR: Extracting payload from a StreamMessage is not implemented yet.";
      } else if (message instanceof MapMessage) {
        payload = "ERROR: Extracting payload from a MapMessage is not implemented yet.";
      } else {
        payload = "ERROR: Unknown message type.";
      }
    } else {
      LOG.info("Message is null.");
    }
    return payload;
  }

  /**
   * Retrieves the user added properties (not JMS properties) from a message.
   *
   * @param message A javax.jms.Message.
   * @return A HashMap of the custom properties.
   */
  public static Map<String, String> getCustomProperties(Message message) {
    Map<String, String> properties = new java.util.HashMap<>(Map.of());
    try {
      for (Enumeration<String> e = message.getPropertyNames(); e.hasMoreElements(); ) {
        var s = e.nextElement();
        properties.put(s, message.getObjectProperty(s).toString());
      }
    } catch (JMSException e) {
      e.printStackTrace();
    }
    LOG.info("Number of properties (map size)={}", properties.size());
    return properties;
  }

  private static String payloadFromTextMessage(TextMessage textMessage) {
    var payload = "";
    try {
      payload = textMessage.getText();
    } catch (JMSException e) {
      e.printStackTrace();
    }
    return payload;
  }

  private static String payloadFromBytesMessage(BytesMessage bytesMessage) {
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

  private static String payloadFromObjectMessage(ObjectMessage objectMessage) {
    var payload = "";
    try {
      payload = objectMessage.getObject().toString();
    } catch (JMSException e) {
      e.printStackTrace();
    }
    return payload;
  }
}
