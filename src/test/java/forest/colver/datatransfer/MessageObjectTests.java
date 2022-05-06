package forest.colver.datatransfer;

import forest.colver.datatransfer.it.MessagingIntTests;
import forest.colver.datatransfer.messaging.MessageObject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageObjectTests {
  private static final Logger LOG = LoggerFactory.getLogger(MessageObjectTests.class);


  @Test
  public void testDisplayMessage() {
    var message = MessagingIntTests.createMessage();
    var mo = new MessageObject(message);
    LOG.info(mo.displayMessage());
  }
}
