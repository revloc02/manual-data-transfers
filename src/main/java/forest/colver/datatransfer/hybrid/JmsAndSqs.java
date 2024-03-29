package forest.colver.datatransfer.hybrid;

import static forest.colver.datatransfer.aws.SqsOperations.sqsConsumeOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.aws.Utils.convertSqsMessageAttributesToStrings;
import static forest.colver.datatransfer.config.Utils.getPassword;
import static forest.colver.datatransfer.config.Utils.getUsername;
import static forest.colver.datatransfer.messaging.JmsConsume.consumeOneMessage;
import static forest.colver.datatransfer.messaging.JmsSend.sendMessageAutoAck;
import static forest.colver.datatransfer.messaging.Utils.createTextMessage;
import static forest.colver.datatransfer.messaging.Utils.extractMsgProperties;
import static forest.colver.datatransfer.messaging.Utils.getJmsMsgPayload;
import static jakarta.jms.JMSContext.CLIENT_ACKNOWLEDGE;

import forest.colver.datatransfer.messaging.Environment;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

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
    var sqsMsg = sqsConsumeOneMessage(awsCreds, sqs);
    if (sqsMsg != null) {
      TextMessage message = createTextMessage(sqsMsg.body(),
          convertSqsMessageAttributesToStrings(sqsMsg.messageAttributes()));
      sendMessageAutoAck(env, queue, message);
    } else {
      LOG.error("ERROR: SQS message was null.");
    }
  }

  public static void moveAllSpecificMessagesFromJmsToSqs(Environment env, String queue,
      String selector, AwsCredentialsProvider awsCreds, String sqs) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword(), CLIENT_ACKNOWLEDGE)) {
      var fromQ = ctx.createQueue(queue);
      try (var consumer = ctx.createConsumer(fromQ, selector)) {
        jmsToSqsMessageMover(consumer, awsCreds, sqs, env, queue);
      } catch (JMSException e) {
        e.printStackTrace();
      }
    }
  }

  public static void moveAllMessagesFromJmsToSqs(Environment env, String queue,
      AwsCredentialsProvider awsCreds, String sqs) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword(), CLIENT_ACKNOWLEDGE)) {
      var fromQ = ctx.createQueue(queue);
      try (var consumer = ctx.createConsumer(fromQ)) {
        jmsToSqsMessageMover(consumer, awsCreds, sqs, env, queue);
      } catch (JMSException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * A private helper method that moves messages from a JMS queue (Qpid) to an SQS queue. It counts
   * the messages moved and logs the operation.
   *
   * @param consumer The JMS consumer for retrieving the messages.
   * @param awsCreds AWS credentials.
   * @param sqs The SQS destination.
   * @param env The environment for the Qpid queue. This var is only used for the log statement.
   * @param queue The Qpid queue name. This var is only used for the log statement.
   */
  private static void jmsToSqsMessageMover(JMSConsumer consumer, AwsCredentialsProvider awsCreds,
      String sqs, Environment env, String queue)
      throws JMSException {
    var counter = 0;
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
    LOG.info("Moved {} messages.", counter);
  }

  public static void moveAllMessagesFromSqsToJms(AwsCredentialsProvider awsCreds, String sqs,
      Environment env,
      String queue) {
    var moreMessages = true;
    var counter = 0;
    while (moreMessages) {
      var sqsMsg = sqsConsumeOneMessage(awsCreds, sqs);
      if (sqsMsg != null) {
        counter++;
        TextMessage message = createTextMessage(sqsMsg.body(),
            convertSqsMessageAttributesToStrings(sqsMsg.messageAttributes()));
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
