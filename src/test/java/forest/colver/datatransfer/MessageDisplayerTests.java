package forest.colver.datatransfer;

import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getPassword;
import static forest.colver.datatransfer.config.Utils.getTimeStamp;
import static forest.colver.datatransfer.config.Utils.getUsername;
import static forest.colver.datatransfer.it.MessagingIntTests.createMessage;
import static forest.colver.datatransfer.messaging.Environment.STAGE;
import static forest.colver.datatransfer.messaging.JmsSend.createTextMessage;
import static org.assertj.core.api.Assertions.assertThat;

import forest.colver.datatransfer.messaging.MessageDisplayer;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageDisplayerTests {

  private static final Logger LOG = LoggerFactory.getLogger(MessageDisplayerTests.class);

  @Test
  public void testCreateStringTextMessage() {
    var message = createMessage();
    var md = new MessageDisplayer(message);
    var s = md.createString();
    md.displayMessage();
    assertThat(s).contains("Message Type: Text");
    assertThat(s).contains("Payload (truncated to 100 chars): Default Payload:");
    // whitespace in this assert depends on string format in object
    assertThat(s).contains("key2                 = value2");
  }

  @Test
  public void testCreateStringSimpleTextMessage() {
    TextMessage message;
    var payload = getDefaultPayload();
    var cf = new JmsConnectionFactory(STAGE.url());
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      message = ctx.createTextMessage(payload);
    }
    var md = new MessageDisplayer(message);
    assertThat(md.createString(10, false)).isEqualTo("\n"
        + "Message Type: Text\n"
        + "  Custom Properties:\n"
        + "    JMSXDeliveryCount    = 1\n"
        + "  Payload (truncated to 10 chars): Default Pa");
  }


  @Test
  public void testCreateStringTruncatePayload() {
    var message = createTextMessage("This is the body, and part of it should be truncated.",
        Map.of());
    var md = new MessageDisplayer(message);
    var s = md.createString(16, false);
    LOG.info(s);
    assertThat(s).contains("Message Type: Text");
    assertThat(s).contains("Payload (truncated to 16 chars): This is the body");
    assertThat(s).doesNotContain(", and part of it should be truncated.");
    assertThat(s).doesNotContain("JMSMessageID");
    assertThat(s).doesNotContain("JMSPriority");
    assertThat(s).doesNotContain("JMSRedelivered");
    assertThat(s).doesNotContain("JMSDestination");
  }

  @Test
  public void testCreateStringWithJmsProps() {
    var message = createMessage();
    var md = new MessageDisplayer(message);
    var s = md.createString(100, true);
    LOG.info(s);
    assertThat(s).contains("Message Type: Text");
    assertThat(s).contains("Payload (truncated to 100 chars): Default Payload:");
    assertThat(s).contains("JMSPriority          = 4");
    assertThat(s).contains("JMSRedelivered       = false");
    assertThat(s).contains("JMSExpiration        = 0");
  }

  @Test
  public void testDisplayMessage() {
    var message = createMessage();
    var md = new MessageDisplayer(message);
    // nothing really to assert here, but I do want to test display message by displaying it
    md.displayMessage();
  }

  @Test
  public void testCreateStringBytesMessage() throws JMSException {
    byte[] bytes = "payload".getBytes();
    BytesMessage message;
    var messageProps = Map.of("timestamp", getTimeStamp(), "key", "value", "datatype",
        "test.message");
    var cf = new JmsConnectionFactory(STAGE.url());
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      message = ctx.createBytesMessage();
      message.writeBytes(bytes);
      for (Map.Entry<String, String> entry : messageProps.entrySet()) {
        message.setStringProperty(entry.getKey(), entry.getValue());
      }
    }
    var md = new MessageDisplayer(message);
    var s = md.createString();
    assertThat(s).contains("Message Type: Bytes");
    assertThat(s).contains("Payload (truncated to 100 chars): payload");
    // whitespace in this assert depends on string format in object
    assertThat(s).contains("datatype             = test.message");
  }

  @Test
  public void testCreateStringObjectMessage() throws JMSException {
    var payload = "using a String for the payload in this case";
    ObjectMessage message;
    var messageProps = Map.of("timestamp", getTimeStamp(), "key", "value", "datatype",
        "test.message");
    var cf = new JmsConnectionFactory(STAGE.url());
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      message = ctx.createObjectMessage(payload);
      for (Map.Entry<String, String> entry : messageProps.entrySet()) {
        message.setStringProperty(entry.getKey(), entry.getValue());
      }
    }
    var md = new MessageDisplayer(message);
    var s = md.createString();
    assertThat(s).contains("Message Type: Object");
    assertThat(s).contains("Payload (truncated to 100 chars): " + payload);
    // whitespace in this assert depends on string format in object
    assertThat(s).contains("datatype             = test.message");
  }
}
