package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.CloudWatchLogsOps.putCWLogEvents;
import static forest.colver.datatransfer.aws.Utils.getCloudWatchLogsClient;
import static forest.colver.datatransfer.aws.Utils.getPrsnlSbCreds;

import forest.colver.datatransfer.aws.CloudWatchLogsOps;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;

public class AwsCloudWatchLogsIntTests {
  private static final Logger LOG = LoggerFactory.getLogger(AwsCloudWatchLogsIntTests.class);

  @Test
  public void testCloudWatchLogs() {
    var logGroupName = "archive_test";
    var streamName = "stream1235";
    var creds = getPrsnlSbCreds();
    CloudWatchLogsClient logsClient = getCloudWatchLogsClient(creds);
    CreateLogStreamRequest createLogStreamRequest = CreateLogStreamRequest.builder().logGroupName(logGroupName).logStreamName(streamName).build();
    var createLogStreamResponse = logsClient.createLogStream(createLogStreamRequest);
    LOG.info("createLogStreamResponse=", createLogStreamResponse.sdkHttpResponse().statusCode());

    putCWLogEvents(logsClient, logGroupName, streamName) ;
    logsClient.close(); //todo: just wondering, do I close my other aws clients for other aws resources I am connecting to in other code?
  }
}
