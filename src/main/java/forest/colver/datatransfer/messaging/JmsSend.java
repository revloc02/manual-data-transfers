package forest.colver.datatransfer.messaging;

import static forest.colver.datatransfer.config.Utils.TIME_STAMP_FORMATTED;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getPassword;
import static forest.colver.datatransfer.config.Utils.getUsername;
import static forest.colver.datatransfer.messaging.Environment.STAGE;
import static forest.colver.datatransfer.messaging.Utils.createTextMessage;
import static javax.jms.JMSContext.AUTO_ACKNOWLEDGE;

import java.util.ArrayList;
import java.util.Map;
import javax.jms.Message;
import org.apache.qpid.jms.JmsConnectionFactory;
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

  /**
   * Sends a message to a queue. The AUTO_ACKNOWLEDGE is the default, but set explicitly here as a
   * reminder. Acknowledgement on sending is not as critical as when receiving messages from the
   * broker. https://jstobigdata.com/jms/guaranteed-delivery-using-jms-message-acknowledgement/
   *
   * @param env The environment of the Qpid broker.
   * @param queueName The name of the queue.
   * @param message The message to send.
   */
  public static void sendMessageAutoAck(Environment env, String queueName, Message message) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword(), AUTO_ACKNOWLEDGE)) {
      var queue = ctx.createQueue(queueName);
      var producer = ctx.createProducer();
      producer.send(queue, message);
      LOG.info(
          "Message sent to Queue={}:{}, Message->{}",
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
            .setProperty("sentTimestamp", TIME_STAMP_FORMATTED)
            .setProperty("datatype", "moreTesting")
            .setProperty("messageNumber", i)
            .send(queue, message);
        LOG.info("Message {} sent to Queue={}:{}", i, env.name(), queueName);
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
            .setProperty("sentTimestamp", TIME_STAMP_FORMATTED)
            .setProperty("datatype", "moreTesting")
            .send(queue, ctx.createTextMessage(payload));
      }
    }
  }
}
