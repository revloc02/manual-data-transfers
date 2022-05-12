package forest.colver.datatransfer;

import static forest.colver.datatransfer.config.Utils.getPassword;
import static forest.colver.datatransfer.config.Utils.getTimeStamp;
import static forest.colver.datatransfer.config.Utils.getUsername;
import static forest.colver.datatransfer.messaging.Environment.STAGE;
import static org.assertj.core.api.Assertions.assertThat;

import forest.colver.datatransfer.it.MessagingIntTests;
import forest.colver.datatransfer.messaging.MessageObject;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import org.apache.qpid.jms.JmsConnectionFactory;
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
    mo.displayMessage();
    assertThat(s).contains("Message Type: Text");
    assertThat(s).contains("Payload (truncated to 100 chars): Default Payload:");
    // whitespace in this assert depends on string format in object
    assertThat(s).contains("key2                 = value2");
  }

  @Test
  public void testCreateStringTruncatePayload() {
    var message = MessagingIntTests.createMessage();
    var mo = new MessageObject(message);
    var s = mo.createString(10,false);
    LOG.info(s);
    assertThat(s).contains("Message Type: Text");
    assertThat(s).contains("Payload (truncated to 10 chars): Default Pa"); //todo this isn't really a good truncation test because a trunc val of >10 would also pass this test
    // whitespace in this assert depends on string format in object
    assertThat(s).contains("key2                 = value2");
  }

  // todo: have one test the exact output. have another test a message with no custom properties. Also test JMS props output on and off.

  @Test
  public void testDisplayMessage() {
    var message = MessagingIntTests.createMessage();
    var mo = new MessageObject(message);
    // nothing really to assert here, but I do want to test display message buy displaying it
    mo.displayMessage();
  }

  @Test
  public void testBytesMessageCreateString() throws JMSException {
    byte[] bytes = "payload".getBytes();
    BytesMessage message;
    var messageProps = Map.of("timestamp", getTimeStamp(), "key", "value", "datatype", "test.message");
    var cf = new JmsConnectionFactory(STAGE.url());
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      message = ctx.createBytesMessage();
      message.writeBytes(bytes);
      for (Map.Entry<String, String> entry : messageProps.entrySet()) {
        message.setStringProperty(entry.getKey(), entry.getValue());
      }
    }
    var mo = new MessageObject(message);
    var s = mo.createString();
    assertThat(s).contains("Message Type: Bytes");
    assertThat(s).contains("Payload (truncated to 100 chars): payload");
    // whitespace in this assert depends on string format in object
    assertThat(s).contains("datatype             = test.message");
  }
}
