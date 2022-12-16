package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.CloudWatchLogsOps.putCWLogEvents;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.aws.Utils.getPersonalSbCreds;
import static forest.colver.datatransfer.config.Utils.getRandomNumber;
import static forest.colver.datatransfer.config.Utils.getTimeStampFormatted;
import static forest.colver.datatransfer.config.Utils.getUuid;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsCloudWatchLogsIntTests {

  public static final String LOG_GROUP_NAME = "/emx-trace-data-sandbox";
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
    putCWLogEvents(getPersonalSbCreds(), LOG_GROUP_NAME, streamName, List.of(message));
  }

  @Test
  public void testTwoCallsCloudWatchLogEvents() {
    var message = "testMultipleCloudWatchLogEvents: This is the log message.";
    var anotherMessage = "{ \"key1\": \"value1\", \"key2\": \"value2\" }";

    var streamName = STREAM_PREFIX + "-" + getUuid();
    LOG.info("streamName={}", streamName);
    var putLogEventsResponse = putCWLogEvents(getPersonalSbCreds(), LOG_GROUP_NAME, streamName,
        List.of(message));
    putCWLogEvents(getPersonalSbCreds(), LOG_GROUP_NAME, streamName,
        putLogEventsResponse.nextSequenceToken(),
        List.of(anotherMessage));
  }

  @Test
  public void testMultipleCallsCloudWatchLogEvents() {
    var messagePrefix = "Multi-Calls message prefix: ";

    var streamName = STREAM_PREFIX + "-" + getUuid();
    LOG.info("streamName={}", streamName);
    var message = messagePrefix + getUuid();
    var putLogEventsResponse = putCWLogEvents(getPersonalSbCreds(), LOG_GROUP_NAME, streamName,
        List.of(message));

    for (int i = 0; i < 35; i++) {
      message = messagePrefix + getUuid();
      putLogEventsResponse = putCWLogEvents(getPersonalSbCreds(), LOG_GROUP_NAME, streamName,
          putLogEventsResponse.nextSequenceToken(),
          List.of(message));
    }
  }

  @Test
  public void testMultipleCloudWatchLogEvents() {
    var messagePrefix = "Multiple-messages-one-call, message generated from manual-data-transfers";

    for (int i = 0; i < 2; i++) {
      var streamName = STREAM_PREFIX + "-" + getUuid();
      LOG.info("stream #{}; streamName={}", i + 1, streamName);
      List<String> messages = generateLogs(messagePrefix, 1_200, 20, 45);
      putCWLogEvents(getPersonalSbCreds(), "/emx-trace-data-sandbox", streamName, messages);
    }
  }

  private List<String> generateLogs(String messagePrefix, int numOfLogs, int sizeMin, int sizeMax) {
    List<String> messages = new ArrayList<>();
    for (int i = 0; i < numOfLogs; i++) {
      var jsonString = new JSONObject()
          .put("name", messagePrefix)
          .put("trace", getUuid())
          .put("id", getTimeStampFormatted())
          .put("parent", "mom")
          .put("start", Instant.now().toEpochMilli())
          .put("end", Instant.now().toEpochMilli())
          .put("kind", getRandomNumber(1, 5))
          .put("attr", new JSONObject()
              .put("messaging.destination", "cubs-pucker-skim")
              .put("messaging.message_id", getUuid())
              .put("route.id", getUuid())
              .put("size", getRandomNumber(sizeMin, sizeMax))
              .put("time", Instant.now().toEpochMilli()))
          .put("status", new JSONObject()
              .put("code", "200")
              .put("desc", "hey mom"))
          .put("resource", new JSONObject()
              .put("attr", new JSONObject()
                  .put("app.emx_env", "prod")
                  .put("app.env", "prod")
                  .put("app.interchange", "cars")))
          .put("events", new JSONArray()
              .put(new JSONObject()
                  .put("attr", new JSONObject()
                      .put("exception.stacktrace",
                          "l.e.Destination$EmxSendException: Bad request returned status 400, see logs for response body")
                      .put("exception.type", "lds.emx.Destination$EmxSendException"))
                  .put("name", "exception")
                  .put("time", Instant.now().toEpochMilli())))
          .toString();
      messages.add(jsonString);
    }
    return messages;
  }
}
