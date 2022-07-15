package forest.colver.datatransfer.hybrid;

import static forest.colver.datatransfer.aws.SqsOperations.sqsConsume;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.messaging.JmsConsume.consumeOneMessage;
import static forest.colver.datatransfer.messaging.JmsSend.createTextMessage;
import static forest.colver.datatransfer.messaging.JmsSend.sendMessageAutoAck;

import forest.colver.datatransfer.messaging.Environment;
import java.util.Enumeration;
import java.util.Map;
import java.util.Map.Entry;
import javax.jms.JMSException;
import javax.jms.TextMessage;
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
  public static void moveJmsToSqs(Environment qpidEnv, String qpidQ,
      AwsCredentialsProvider awsCreds, String sqs) {
    TextMessage msg = (TextMessage) consumeOneMessage(qpidEnv, qpidQ);
    var payload = "";
    Map<String, String> properties = new java.util.HashMap<>(Map.of());
    try {
      payload = msg.getText();
      for (Enumeration<String> e = msg.getPropertyNames(); e.hasMoreElements(); ) {
        var s = e.nextElement();
        properties.put(s, msg.getObjectProperty(s).toString());
      }
    } catch (JMSException e) {
      e.printStackTrace();
    }
    // SQS messages are limited to 10 attributes of up to 256 characters each
    LOG.info("Number of properties (map size)={}", properties.size());
    sqsSend(awsCreds, sqs, payload, properties);
  }

  public static void moveSqsToJms(AwsCredentialsProvider awsCreds, String sqs, Environment env,
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
  //todo: probably should have a move all, and a move specific
}
