package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.SqsOperations.sqsDelete;
import static forest.colver.datatransfer.aws.SqsOperations.sqsGet;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS1;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.config.Utils.getTimeStampFormatted;
import static forest.colver.datatransfer.hybrid.JmsAndSqs.moveJmsToSqs;
import static forest.colver.datatransfer.messaging.Environment.STAGE;
import static forest.colver.datatransfer.messaging.JmsSend.createTextMessage;
import static forest.colver.datatransfer.messaging.JmsSend.sendMessageAutoAck;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class HybridJmsAndSqsIntTests {

  /**
   * This tests moving a message from Qpid to AWS SQS.
   */
  @Test
  public void testMoveJmsToSqs() {
    var payload = "this is the payload";
    var messageProps = Map.of("timestamp", getTimeStampFormatted(), "key2", "value2", "key3",
        "value3");
    var queueName = "forest-test";
    sendMessageAutoAck(STAGE, queueName, createTextMessage(payload, messageProps));

    var creds = getEmxSbCreds();
    moveJmsToSqs(STAGE, queueName, creds, EMX_SANDBOX_TEST_SQS1);

    // check that it arrived
    var response = sqsGet(creds, EMX_SANDBOX_TEST_SQS1);
    assertThat(response.messages().get(0).body()).isEqualTo(payload);
    assertThat(response.messages().get(0).hasMessageAttributes()).isEqualTo(true);
    assertThat(response.messages().get(0).messageAttributes().get("key2").stringValue()).isEqualTo(
        "value2");
    assertThat(response.messages().get(0).messageAttributes().get("key3").stringValue()).isEqualTo(
        "value3");

    // cleanup
    sqsDelete(creds, response, EMX_SANDBOX_TEST_SQS1);
  }
}
