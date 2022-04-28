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
      } else if (message instanceof BytesMessage) {
      } else if (message instanceof ObjectMessage) {
      } else if (message instanceof StreamMessage) {
      } else if (message instanceof MapMessage) {
      } else {
        LOG.info("Message Type: Unknown\n");
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
