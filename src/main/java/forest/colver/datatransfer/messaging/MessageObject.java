package forest.colver.datatransfer.messaging;

import java.util.Map;
import javax.jms.Message;

public class MessageObject {

  private Message message;
  private String payload;
  private Map<String, String> headers;

  public MessageObject(Message message) {
    this.message = message;
  }

  public void displayMessage() {

  }
}
