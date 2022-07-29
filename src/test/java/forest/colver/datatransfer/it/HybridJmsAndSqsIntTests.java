package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.SqsOperations.sqsConsume;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDelete;
import static forest.colver.datatransfer.aws.SqsOperations.sqsPurge;
import static forest.colver.datatransfer.aws.SqsOperations.sqsRead;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS1;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStampFormatted;
import static forest.colver.datatransfer.hybrid.JmsAndSqs.moveAllSpecificMessagesFromJmsToSqs;
import static forest.colver.datatransfer.hybrid.JmsAndSqs.moveOneJmsToSqs;
import static forest.colver.datatransfer.hybrid.JmsAndSqs.moveOneSqsToJms;
import static forest.colver.datatransfer.messaging.Environment.STAGE;
import static forest.colver.datatransfer.messaging.JmsConsume.consumeOneMessage;
import static forest.colver.datatransfer.messaging.JmsConsume.deleteAllMessagesFromQueue;
import static forest.colver.datatransfer.messaging.JmsSend.sendMessageAutoAck;
import static forest.colver.datatransfer.messaging.JmsSend.sendMultipleSameMessage;
import static forest.colver.datatransfer.messaging.Utils.createDefaultMessage;
import static forest.colver.datatransfer.messaging.Utils.createTextMessage;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import javax.jms.JMSException;
import javax.jms.TextMessage;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HybridJmsAndSqsIntTests {

  private static final Logger LOG = LoggerFactory.getLogger(HybridJmsAndSqsIntTests.class);
  private static final String SQS1 = EMX_SANDBOX_TEST_SQS1;

  /**
   * This tests moving a message from Qpid to AWS SQS.
   */
  @Test
  public void testMoveOneJmsToSqs() {
    // place a message on Qpid
    var payload = "this is the payload";
    var messageProps = Map.of("timestamp", getTimeStampFormatted(), "key2", "value2", "key3",
        "value3");
    var queueName = "forest-test";
    sendMessageAutoAck(STAGE, queueName, createTextMessage(payload, messageProps));

    // move it to SQS
    var creds = getEmxSbCreds();
    moveOneJmsToSqs(STAGE, queueName, creds, SQS1);

    // check that it arrived
    var response = sqsRead(creds, SQS1);
    assertThat(response.messages().get(0).body()).isEqualTo(payload);
    assertThat(response.messages().get(0).hasMessageAttributes()).isEqualTo(true);
    assertThat(response.messages().get(0).messageAttributes().get("key2").stringValue()).isEqualTo(
        "value2");
    assertThat(response.messages().get(0).messageAttributes().get("key3").stringValue()).isEqualTo(
        "value3");

    // cleanup
    sqsDelete(creds, response, SQS1);
  }

  @Test
  public void testMoveOneSqsToJms() throws JMSException {
    // place a message on SQS
    LOG.info("Interacting with: sqs={}", SQS1);
    var creds = getEmxSbCreds();
    var messageProps = Map.of("timestamp", getTimeStampFormatted(), "key2", "value2", "key3",
        "value3");
    var payload = getDefaultPayload();
    sqsSend(creds, SQS1, payload, messageProps);

    // move it to Qpid
    var queue = "forest-test";
    moveOneSqsToJms(creds, SQS1, STAGE, queue);

    // assert the SQS was cleared
    var messages = sqsRead(creds, SQS1);
    assertThat(messages.hasMessages()).isFalse();

    // check that it arrived
    var message = consumeOneMessage(STAGE, queue);
    assertThat(((TextMessage) message).getText()).contains(payload);
    assertThat(message.getStringProperty("key2")).isEqualTo("value2");
    assertThat(message.getStringProperty("key3")).isEqualTo("value3");
  }

  @Test
  public void testMoveAllSpecificMessagesJmsToSqs() {
    var env = STAGE;
    var queue = "forest-test";
    var creds = getEmxSbCreds();

    // send one kind of message to Qpid
    var numMessagesFrom = 5;
    sendMultipleSameMessage(env, queue, createDefaultMessage(), numMessagesFrom);

    // send some messages of a different kind to Qpid
    var messageProps = Map.of("timestamp", getTimeStampFormatted(), "specificKey", "specificValue");
    var numMessagesToMove = 3;
    var payload = "payload";
    for (var i = 0; i < numMessagesToMove; i++) {
      sendMessageAutoAck(env, queue, createTextMessage(payload, messageProps));
    }

    // move all the different kind of messages to SQS
    moveAllSpecificMessagesFromJmsToSqs(env, queue, "specificKey='specificValue'", creds, SQS1);

    // check that each arrived on SQS
    for (var i = 0; i < numMessagesToMove; i++) {
      var response = sqsConsume(creds, SQS1);
      assertThat(response.messages().get(0).body()).isEqualTo(payload);
      assertThat(response.messages().get(0).hasMessageAttributes()).isEqualTo(true);
      assertThat(
          response.messages().get(0).messageAttributes().get("specificKey")
              .stringValue()).isEqualTo(
          "specificValue");
    }

    // cleanup and check that the Qpid queue had the correct number of messages after the move
    var deletedFrom = deleteAllMessagesFromQueue(env, queue);
    assertThat(deletedFrom).isEqualTo(numMessagesFrom);

    // cleanup SQS (should be empty already, but just being thorough)
    sqsPurge(creds, SQS1);
  }

  // todo: test to move all specific messages from SQS to JMS
}
