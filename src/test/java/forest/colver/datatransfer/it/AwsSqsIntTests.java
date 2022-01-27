package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.SqsOperations.sqsCopy;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDelete;
import static forest.colver.datatransfer.aws.SqsOperations.sqsGet;
import static forest.colver.datatransfer.aws.SqsOperations.sqsGetQueueAttributes;
import static forest.colver.datatransfer.aws.SqsOperations.sqsMove;
import static forest.colver.datatransfer.aws.SqsOperations.sqsPurge;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.aws.Utils.getSbCreds;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStamp;
import static forest.colver.datatransfer.config.Utils.readFile;
import static forest.colver.datatransfer.config.Utils.sleepo;
import static org.assertj.core.api.Assertions.assertThat;

import forest.colver.datatransfer.aws.Utils;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration Tests for AWS SQS
 */
public class AwsSqsIntTests {

  public static final String QUEUE = Utils.getSqs();
  public static final String FROM_QUEUE = Utils.getFromSqs();
  public static final String TO_QUEUE = Utils.getToSqs();
  private static final Logger LOG = LoggerFactory.getLogger(AwsSqsIntTests.class);

  @Test
  public void testSqsPurge() {
    // place some messages
    var creds = getSbCreds();
    for (var i = 0; i < 5; i++) {
      sqsSend(
          creds,
          QUEUE,
          readFile("src/main/resources/1test.txt", StandardCharsets.UTF_8));
    }
    sleepo(1_000);

    // check that the messages are where we think they are
    var attributes = sqsGetQueueAttributes(creds, QUEUE);
    sqsPurge(creds, QUEUE); // purge before asserting depth in case it's wrong, thus a rerun will work
    assertThat(attributes.attributesAsStrings().get("ApproximateNumberOfMessages")).isEqualTo("5");

    // assert the queue was cleared
    var messages = sqsGet(creds, QUEUE);
    assertThat(messages.hasMessages()).isFalse();
  }

  @Test
  public void testSqsCopy() {
    // put message on queue
    var creds = getSbCreds();
    var payload = getDefaultPayload();
    sqsSend(creds, FROM_QUEUE, payload);

    // verify message is on the queue
    var fromQResponse = sqsGet(creds, FROM_QUEUE);
    var body = fromQResponse.messages().get(0).body();
    assertThat(body).isEqualTo(payload);

    // copy the message
    sleepo(4_000); // waiting for the visibility timeout from the sqsGet()
    sqsCopy(creds, FROM_QUEUE, TO_QUEUE);

    // remove message from source queue
    sqsDelete(creds, fromQResponse, FROM_QUEUE);

    // verify the message is on the other queue
    var toQResponse = sqsGet(creds, TO_QUEUE);
    body = toQResponse.messages().get(0).body();
    assertThat(body).isEqualTo(payload);

    // cleanup
    sqsDelete(creds, toQResponse, TO_QUEUE);
  }

  @Test
  public void testSqsSend() {
    // send some stuff
    var creds = getSbCreds();
    var payload = "message with payload only, no headers";
    sqsSend(creds, QUEUE, payload);
    // check that it arrived
    var response = sqsGet(creds, QUEUE);
    assertThat(response.messages().get(0).body()).isEqualTo(payload);
    // cleanup
    sqsDelete(creds, response, QUEUE);
  }

  @Test
  public void testSqsSendWithProperties() {
    // send some stuff
    var creds = getSbCreds();
    var messageProps = Map.of("timestamp", getTimeStamp(), "key2", "value2", "key3", "value3");
    var payload = getDefaultPayload();
    sqsSend(creds, QUEUE, payload, messageProps);
    // check that it arrived
    var response = sqsGet(creds, QUEUE);
    assertThat(response.messages().get(0).body()).isEqualTo(payload);
    assertThat(response.messages().get(0).hasMessageAttributes()).isEqualTo(true);
    assertThat(response.messages().get(0).messageAttributes().get("key2").stringValue()).isEqualTo(
        "value2");
    assertThat(response.messages().get(0).messageAttributes().get("key3").stringValue()).isEqualTo(
        "value3");
    assertThat(response.messages().get(0).body()).isEqualTo(payload);
    // cleanup
    sqsDelete(creds, response, QUEUE);
  }

  @Test
  public void testSqsMove() {
    // put message on queue
    var creds = getSbCreds();
    var payload = getDefaultPayload();
    sqsSend(creds, FROM_QUEUE, payload);

    // verify message is on the queue
    var fromQResponse = sqsGet(creds, FROM_QUEUE);
    var body = fromQResponse.messages().get(0).body();
    assertThat(body).isEqualTo(payload);

    // move the message
    sleepo(4_000); // waiting for the visibility timeout from the sqsGet()
    sqsMove(creds, FROM_QUEUE, TO_QUEUE);

    // verify the message is on the other queue
    var toQResponse = sqsGet(creds, TO_QUEUE);
    body = toQResponse.messages().get(0).body();
    assertThat(body).isEqualTo(payload);

    // cleanup
    sqsDelete(creds, toQResponse, TO_QUEUE);

  }
}
