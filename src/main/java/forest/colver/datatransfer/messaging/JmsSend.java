package forest.colver.datatransfer.messaging;

import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getPassword;
import static forest.colver.datatransfer.config.Utils.getUsername;
import static forest.colver.datatransfer.messaging.Environment.STAGE;

import forest.colver.datatransfer.config.Utils;
import java.util.ArrayList;
import java.util.Map;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsSend {

  private static final Logger LOG = LoggerFactory.getLogger(JmsSend.class);

  // EMX
  public static void sendDefaultMessage() {
    sendMessageAutoAck(
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

  public static void sendMessageAutoAck(Environment env, String queueName, Message message) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword(), JmsContext.AUTO_ACKNOWLEDGE)) {
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

  public static void sendMessageClientAck(Environment env, String queueName, Message message) {
    var cf = new JmsConnectionFactory(env.url());
    // todo: I have a fundamental problem with my JMS code in that I am not using CLIENT_ACKNOWLEDGE, which could result in data loss. Fix it.
    // JmsContext.AUTO_ACKNOWLEDGE is the default Acknowledgement mode. If I were to throw an exception while transferring data, and I retrieved data from a queue using AUTO_ACKNOWLEDGE, the data would be lost.
    // here's a reference for CLIENT_ACKNOWLEDGE: https://jstobigdata.com/jms/guaranteed-delivery-using-jms-message-acknowledgement/
    try (var ctx = cf.createContext(getUsername(), getPassword(), JmsContext.AUTO_ACKNOWLEDGE)) {
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

  /**
   * Pass in an ArrayList of payloads and send a message for each one.
   *
   * @param env Which environment the queue is in.
   * @param queueName Queue to send to.
   * @param payloads An ArrayList of payloads. If the payloads are not unique just use {@link
   * #sendMultipleSameMessage(Environment, String, Message, int)}
   */
  public static void sendMultipleUniqueMessages(
      Environment env, String queueName, ArrayList<String> payloads) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      var queue = ctx.createQueue(queueName);
      for (String payload : payloads) {
        ctx.createProducer()
            .setProperty("sentTimestamp", Utils.TIME_STAMP)
            .setProperty("datatype", "moreTesting")
            .send(queue, ctx.createTextMessage(payload));
      }
    }
  }
}
