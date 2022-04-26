package forest.colver.datatransfer.messaging;

import java.util.Map;
import javax.jms.BytesMessage;
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
  private Map<String, String> headers;

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
    } else {
      LOG.info("Message is null.");
    }
  }

  public void displayMessage() {

  }
}
