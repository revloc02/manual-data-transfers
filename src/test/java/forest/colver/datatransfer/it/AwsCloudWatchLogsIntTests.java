package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.CloudWatchLogsOps.putCWLogEvents;
import static forest.colver.datatransfer.aws.Utils.getPrsnlSbCreds;
import static forest.colver.datatransfer.config.Utils.getRandomNumber;
import static forest.colver.datatransfer.config.Utils.getTimeStamp;
import static forest.colver.datatransfer.config.Utils.getUuid;

import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsCloudWatchLogsIntTests {

  public static final String LOG_GROUP_NAME = "archive-trace-data-test";
  public static final String STREAM_PREFIX = "streamPrefix";
  private static final Logger LOG = LoggerFactory.getLogger(AwsCloudWatchLogsIntTests.class);

  /**
   * Sends a log message to CloudWatch in a stream that is created. This isn't really asserting
   * anything like usual unit tests. The log stream name is generated fresh each invocation. Note:
   * It is required to have run the AWS Terraform at least once to set up the log group first.
   * Furthermore, each day run aws-azure-login for fresh credentials.
   */
  @Test
  public void testCloudWatchLogEvents() {
    var message = "This is the log message.";

    var streamName = STREAM_PREFIX + "-" + getUuid();
    LOG.info("streamName={}", streamName);
    putCWLogEvents(getPrsnlSbCreds(), LOG_GROUP_NAME, streamName, List.of(message));
  }

  @Test
  public void testTwoCallsCloudWatchLogEvents() {
    var message = "testMultipleCloudWatchLogEvents: This is the log message.";
    var anotherMessage = "{ \"key1\": \"value1\", \"key2\": \"value2\" }";

    var streamName = STREAM_PREFIX + "-" + getUuid();
    LOG.info("streamName={}", streamName);
    var putLogEventsResponse = putCWLogEvents(getPrsnlSbCreds(), LOG_GROUP_NAME, streamName,
        List.of(message));
    putCWLogEvents(getPrsnlSbCreds(), LOG_GROUP_NAME, streamName,
        putLogEventsResponse.nextSequenceToken(),
        List.of(anotherMessage));
  }

  @Test
  public void testMultipleCallsCloudWatchLogEvents() {
    var messagePrefix = "Multi-Calls message prefix: ";

    var streamName = STREAM_PREFIX + "-" + getUuid();
    LOG.info("streamName={}", streamName);
    var message = messagePrefix + getUuid();
    var putLogEventsResponse = putCWLogEvents(getPrsnlSbCreds(), LOG_GROUP_NAME, streamName,
        List.of(message));

    for (int i = 0; i < 35; i++) {
      message = messagePrefix + getUuid();
      putLogEventsResponse = putCWLogEvents(getPrsnlSbCreds(), LOG_GROUP_NAME, streamName,
          putLogEventsResponse.nextSequenceToken(),
          List.of(message));
    }
  }

  @Test
  public void testMultipleCloudWatchLogEvents() {
    var messagePrefix = "Multiple-messages-one-call, message generated from manual-data-transfers";

    var streamName = STREAM_PREFIX + "-" + getUuid();
    LOG.info("streamName={}", streamName);

    List<String> messages = new ArrayList<>();
    for (int i = 0; i < 2_000; i++) {
      var jsonString = new JSONObject()
          .put("name", messagePrefix)
          .put("trace", getUuid())
          .put("time", getTimeStamp())
          .put("key1", "value1")
          .put("kind", getRandomNumber(1, 5))
          .put("attr", new JSONObject()
              .put("key2", "value2")
              .put("messaging.message_id", getUuid())
              .put("size", getRandomNumber(20, 45)))
          .toString();
      messages.add(jsonString);
    }
    putCWLogEvents(getPrsnlSbCreds(), LOG_GROUP_NAME, streamName, messages);
  }
}
