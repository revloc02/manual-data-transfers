package forest.colver.datatransfer.messaging;

import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getPassword;
import static forest.colver.datatransfer.config.Utils.getUsername;
import static forest.colver.datatransfer.messaging.Environment.STAGE;
import static java.util.UUID.randomUUID;

import forest.colver.datatransfer.config.Utils;
import java.util.ArrayList;
import java.util.Map;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsSend {

  private static final Logger LOG = LoggerFactory.getLogger(JmsSend.class);

  // EMX
  public static void sendDefaultMessage() {
    sendMessage(
        STAGE, "forest-test",
        createTextMessage(getDefaultPayload(), Map.of("defaultKey", "defaultValue")));
  }

  public static TextMessage createDefaultMessage() {
    return createTextMessage(getDefaultPayload(), null);
  }

  public static TextMessage createTextMessage(String body, Map<String, String> properties) {
    TextMessage message = null;
    var cf = new JmsConnectionFactory(STAGE.url());
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      // todo: yeah so, is there a better way to create a message without using a cf and ctx?
      message = ctx.createTextMessage(body);
      if (properties != null) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          message.setStringProperty(entry.getKey(), entry.getValue());
        }
      }
    } catch (JMSException e) {
      e.printStackTrace();
    }
    return message;
  }

  public static void sendMessage(Environment env, String queueName, Message message) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      var queue = ctx.createQueue(queueName);
      var producer = ctx.createProducer();
      producer.send(queue, message);
      LOG.info(
          "Message sent to Host={}, Queue={}, Message->{}",
          env.name(),
          queueName,
          DisplayUtils.createStringFromMessage(message));
    }
  }

  public static void sendMultipleSameMessage(
      Environment env, String queueName, Message message, int num) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      var queue = ctx.createQueue(queueName);
      for (int i = 0; i < num; i++) {
        ctx.createProducer()
            .setProperty("sentTimestamp", Utils.TIME_STAMP)
            .setProperty("datatype", "moreTesting")
            .setProperty("messageNumber", i)
            .send(queue, message);
        LOG.info("Message {} sent to Host={}, Queue={}", i, env.name(), queueName);
      }
    }
  }

  public static ArrayList<String> sendMultipleUniqueUuidMessages(
      Environment env, String queueName, int num) {
    var guids = new ArrayList<String>();
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      var queue = ctx.createQueue(queueName);
      for (int i = 0; i < num; i++) {
        var guid = randomUUID().toString();
        guids.add(guid);
        ctx.createProducer()
            .setProperty("sentTimestamp", Utils.TIME_STAMP)
            .setProperty("datatype", "moreTesting")
            .setProperty("messageNumber", i)
            .send(queue, ctx.createTextMessage(guid));
      }
    }
    return guids;
  }
}
