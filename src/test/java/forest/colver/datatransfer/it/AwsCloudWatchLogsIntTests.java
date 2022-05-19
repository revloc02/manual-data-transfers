package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.CloudWatchLogsOps.putCWLogEvents;
import static forest.colver.datatransfer.aws.Utils.getPrsnlSbCreds;
import static forest.colver.datatransfer.config.Utils.getUuid;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsCloudWatchLogsIntTests {

  private static final Logger LOG = LoggerFactory.getLogger(AwsCloudWatchLogsIntTests.class);

  /**
   * Sends a log message to CloudWatch in a stream that is created. This isn't really asserting
   * anything like usual unit tests. The log stream name is generated fresh each invocation. Note:
   * It is required to have run the AWS Terraform at least once to set up the log group first.
   * Furthermore, each day run aws-azure-login for fresh credentials.
   */
  @Test
  public void testCloudWatchLogEvents() {
    var logGroupName = "archive_test";
    var streamPrefix = "streamPrefix";
    var message = "This is the log message.";
    var creds = getPrsnlSbCreds();

    var streamName = streamPrefix + "-" + getUuid();
    LOG.info("streamName={}", streamName);
    putCWLogEvents(creds, logGroupName, streamName, message);
  }

  @Test
  public void testMultipleCloudWatchLogEvents() {
    var logGroupName = "archive_test";
    var streamPrefix = "streamPrefix";
    var message = "testMultipleCloudWatchLogEvents: This is the log message.";
    var anotherMessage = "{ \"key1\": \"value1\", \"key2\": \"value2\" }";
    var creds = getPrsnlSbCreds();

    var streamName = streamPrefix + "-" + getUuid();
    LOG.info("streamName={}", streamName);
    var putLogEventsResponse = putCWLogEvents(creds, logGroupName, streamName, message);
    putCWLogEvents(creds, logGroupName, streamName, putLogEventsResponse.nextSequenceToken(),
        anotherMessage);
  }
}
