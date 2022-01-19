package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.messaging.DisplayUtils.createStringFromMessage;
import static forest.colver.datatransfer.messaging.Environment.STAGE;
import static forest.colver.datatransfer.messaging.JmsConsume.consumeOneMessage;
import static forest.colver.datatransfer.messaging.JmsSend.sendDefaultMessage;
import static org.assertj.core.api.Assertions.assertThat;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagingIntegrationTests {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingIntegrationTests.class);

  @Test
  public void testDefaultSend() throws JMSException {
    var env = STAGE;
    var fromQueueName = "forest-test";
    sendDefaultMessage();
    var message = consumeOneMessage(env, fromQueueName);
    LOG.info(
        "Consumed from Host={} Queue={}, Message->{}",
        env.name(),
        fromQueueName,
        createStringFromMessage(message));
    assertThat(((TextMessage) message).getText()).contains("Default Payload");
    assertThat(message.getStringProperty("defaultKey")).isEqualTo("defaultValue");
  }

}
