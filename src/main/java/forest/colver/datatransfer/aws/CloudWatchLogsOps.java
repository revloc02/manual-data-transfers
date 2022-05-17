package forest.colver.datatransfer.aws;

import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

public class CloudWatchLogsOps {

  private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsOps.class);
  public static PutLogEventsResponse putCWLogEvents(CloudWatchLogsClient logsClient, String logGroupName,
      String streamName) {

//    try {
      DescribeLogStreamsRequest logStreamRequest = DescribeLogStreamsRequest.builder()
          .logGroupName(logGroupName)
          .logStreamNamePrefix(streamName)
          .build();
      DescribeLogStreamsResponse describeLogStreamsResponse = logsClient.describeLogStreams(
          logStreamRequest);

      LOG.info("describeLogStreamsResponse=", describeLogStreamsResponse.toString());
      // Assume that a single stream is returned since a specific stream name was specified in the previous request.
      String sequenceToken = describeLogStreamsResponse.logStreams().get(0).uploadSequenceToken();

      // Build an input log message to put to CloudWatch.
      InputLogEvent inputLogEvent = InputLogEvent.builder()
          .message("{ \"key1\": \"value1\", \"key2\": \"value2\" }")
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

      var response = logsClient.putLogEvents(putLogEventsRequest);
      LOG.info("Successfully put CloudWatch log event");

//    } catch (CloudWatchException e) {
//      System.err.println(e.awsErrorDetails().errorMessage());
//      System.exit(1);
//    }
    return response;
  }
}
