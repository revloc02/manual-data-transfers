package forest.colver.datatransfer.aws;

import static forest.colver.datatransfer.aws.Utils.getCloudWatchLogsClient;

import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

public class CloudWatchLogsOps {

  private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsOps.class);

  public static PutLogEventsResponse putCWLogEvents(AwsCredentialsProvider awsCp,
      String logGroupName,
      String streamName, String message) {
    PutLogEventsResponse response;
    try (var logsClient = getCloudWatchLogsClient(awsCp)) {
      createLogStream(logsClient, logGroupName, streamName);
      var sequenceToken = getSequenceToken(logsClient, logGroupName, streamName);

      // Build an input log message to put to CloudWatch.
      InputLogEvent inputLogEvent = InputLogEvent.builder()
          .message(message)
          .timestamp(System.currentTimeMillis())
          .build();

      // Specify the request parameters.
      // Sequence token is required so that the log can be written to the
      // latest location in the stream.
      PutLogEventsRequest putLogEventsRequest = PutLogEventsRequest.builder()
          .logEvents(Arrays.asList(inputLogEvent))
          .logGroupName(logGroupName)
          .logStreamName(streamName)
          .sequenceToken(sequenceToken)
          .build();

      response = logsClient.putLogEvents(putLogEventsRequest);
      LOG.info("Successfully put CloudWatch log event");
    }
    return response;
  }

  private static String getSequenceToken(CloudWatchLogsClient logsClient, String logGroupName,
      String streamName) {
    DescribeLogStreamsRequest logStreamRequest = DescribeLogStreamsRequest.builder()
        .logGroupName(logGroupName)
        .logStreamNamePrefix(streamName)
        .build();
    DescribeLogStreamsResponse describeLogStreamsResponse = logsClient.describeLogStreams(
        logStreamRequest);

    LOG.info("describeLogStreamsResponse=", describeLogStreamsResponse.toString());
    // Assume that a single stream is returned since a specific stream name was specified in the previous request.
    String sequenceToken = describeLogStreamsResponse.logStreams().get(0).uploadSequenceToken();
    return sequenceToken;
  }

  private static void createLogStream(CloudWatchLogsClient logsClient, String logGroupName,
      String streamName) {
    CreateLogStreamRequest createLogStreamRequest = CreateLogStreamRequest.builder().logGroupName(
        logGroupName).logStreamName(streamName).build();
    var createLogStreamResponse = logsClient.createLogStream(createLogStreamRequest);
    LOG.info("createLogStreamResponse=", createLogStreamResponse.sdkHttpResponse().statusCode());
  }
}
