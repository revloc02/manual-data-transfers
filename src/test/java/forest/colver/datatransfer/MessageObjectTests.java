package forest.colver.datatransfer;

import static org.assertj.core.api.Assertions.assertThat;

import forest.colver.datatransfer.it.MessagingIntTests;
import forest.colver.datatransfer.messaging.MessageObject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageObjectTests {
  private static final Logger LOG = LoggerFactory.getLogger(MessageObjectTests.class);


  @Test
  public void testTextMessageCreateString() {
    var message = MessagingIntTests.createMessage();
    var mo = new MessageObject(message);
    var s = mo.createString();
    LOG.info(s);
    assertThat(s).contains("Message Type: Text");
    assertThat(s).contains("Payload (truncated to 100 chars): Default Payload:");
    // whitespace in this assert depends on string format in object
    assertThat(s).contains("key2                 = value2");

  }
}
