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
  private String payload;
  private Map<String, Object> jmsHeaders;
  private Map<String, String> customHeaders;

  public MessageObject(Message message) {
    if (message != null) {
      this.message = message;
      if (message instanceof TextMessage) {
        LOG.info("Message Type: TextMessage");
        try {
          this.payload = ((TextMessage) message).getText();
        } catch (JMSException e) {
          e.printStackTrace();
        }
      } else if (message instanceof BytesMessage) {
        LOG.info("Message Type: BytesMessage");
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
        assert bytes != null;
        this.payload = new String(bytes);
      } else if (message instanceof ObjectMessage) {
        LOG.info("Message Type: ObjectMessage");
      } else if (message instanceof StreamMessage) {
        LOG.info("Message Type: StreamMessage");
      } else if (message instanceof MapMessage) {
        LOG.info("Message Type: MapMessage");
      } else {
        LOG.info("Message Type: Unknown");
      }
      getJmsProps(message);
      getCustomHeaders(message);
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

  private void getCustomHeaders(Message message) {
    try {
      for (Enumeration<String> e = message.getPropertyNames(); e.hasMoreElements(); ) {
        var s = e.nextElement();
        this.customHeaders.put(s, (String) message.getObjectProperty(s));
      }
    } catch (JMSException e) {
      e.printStackTrace();
    }
  }

  public void displayMessage() {

  }
}
