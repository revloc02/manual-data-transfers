package forest.colver.datatransfer.hybrid;

import static forest.colver.datatransfer.aws.SqsOperations.sqsConsume;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.config.Utils.getPassword;
import static forest.colver.datatransfer.config.Utils.getUsername;
import static forest.colver.datatransfer.messaging.JmsConsume.consumeOneMessage;
import static forest.colver.datatransfer.messaging.JmsSend.sendMessageAutoAck;
import static forest.colver.datatransfer.messaging.Utils.createTextMessage;
import static forest.colver.datatransfer.messaging.Utils.getJmsMsgPayload;
import static forest.colver.datatransfer.messaging.Utils.extractMsgProperties;
import static javax.jms.JMSContext.CLIENT_ACKNOWLEDGE;

import forest.colver.datatransfer.messaging.Environment;
import java.util.Map;
import java.util.Map.Entry;
import javax.jms.JMSException;
import javax.jms.Message;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

/**
 * This is for methods that perform operations between Qpid and SQS.
 */
public class JmsAndSqs {

  private static final Logger LOG = LoggerFactory.getLogger(JmsAndSqs.class);

  /**
   * This is a hybrid method that picks up a message from Qpid and moves it over to AWS SQS.
   *
   * @param qpidEnv The Qpid environment.
   * @param qpidQ The Qpid queue.
   * @param awsCreds Credentials for the AWS environment.
   * @param sqs The AWS SQS.
   */
  public static void moveOneJmsToSqs(Environment qpidEnv, String qpidQ,
      AwsCredentialsProvider awsCreds, String sqs) {
    Message msg = consumeOneMessage(qpidEnv, qpidQ);

    // SQS messages are limited to 10 attributes of up to 256 characters each
    sqsSend(awsCreds, sqs, getJmsMsgPayload(msg), extractMsgProperties(msg));
  }

  public static void moveOneSqsToJms(AwsCredentialsProvider awsCreds, String sqs, Environment env,
      String queue) {
    var receiveMessageResponse = sqsConsume(awsCreds, sqs);
    var payload = receiveMessageResponse.messages().get(0).body();
    Map<String, String> properties = new java.util.HashMap<>(Map.of());
    var props = receiveMessageResponse.messages().get(0).messageAttributes().entrySet();
    for (Entry<String, MessageAttributeValue> prop : props) {
      properties.put(prop.getKey(), prop.getValue().stringValue());
    }
    var message = createTextMessage(payload, properties);
    sendMessageAutoAck(env, queue, message);
  }

  public static void moveAllSpecificMessagesFromJmsToSqs(Environment env, String queue,
      String selector, AwsCredentialsProvider awsCreds, String sqs) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword(), CLIENT_ACKNOWLEDGE)) {
      var fromQ = ctx.createQueue(queue);
      var counter = 0;
      try (var consumer = ctx.createConsumer(fromQ, selector)) {
        var moreMessages = true;
        while (moreMessages) {
          var message = consumer.receive(2_000L);
          if (message != null) {
            counter++;
            sqsSend(awsCreds, sqs, getJmsMsgPayload(message), extractMsgProperties(message));
            message.acknowledge();
            LOG.info(
                "Moved from Queue={}:{} to SQS={}, counter={}",
                env.name(),
                queue,
                sqs,
                counter);
          } else {
            moreMessages = false;
          }
        }
      } catch (JMSException e) {
        e.printStackTrace();
      }
      LOG.info("Moved {} messages for selector={}.", counter, selector);
    }
  }

  public static void moveAllMessagesFromSqsToJms(AwsCredentialsProvider awsCreds, String sqs,
      Environment env,
      String queue) {
    var moreMessages = true;
    var counter = 0;
    while (moreMessages) {
      var response = sqsConsume(awsCreds, sqs);
      if (response.hasMessages()) {
        counter++;
        var payload = response.messages().get(0).body();
        Map<String, String> properties = new java.util.HashMap<>(Map.of());
        var props = response.messages().get(0).messageAttributes().entrySet();
        for (Entry<String, MessageAttributeValue> prop : props) {
          properties.put(prop.getKey(), prop.getValue().stringValue());
        }
        var message = createTextMessage(payload, properties);
        sendMessageAutoAck(env, queue, message);
        LOG.info(
            "Moved from SQS={} to, Queue={}:{} counter={}",
            sqs,
            env.name(),
            queue,
            counter);
      } else {
        moreMessages = false;
      }
    }
    LOG.info("Moved {} messages from SQS={} to, Queue={}:{}.", counter, sqs, env.name(), queue);
  }
}
