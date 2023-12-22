package forest.colver.datatransfer.aws;

import static forest.colver.datatransfer.aws.Utils.getSnsClient;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sns.model.GetTopicAttributesRequest;
import software.amazon.awssdk.services.sns.model.GetTopicAttributesResponse;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

public class SnsOperations {

  private SnsOperations() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

  private static final Logger LOG = LoggerFactory.getLogger(SnsOperations.class);

  public static Map<String, String> getSnsTopicAttributes(AwsCredentialsProvider awsCp,
      String topicArn) {

    try (var snsClient = getSnsClient(awsCp)) {
      GetTopicAttributesRequest request = GetTopicAttributesRequest.builder()
          .topicArn(topicArn)
          .build();
      GetTopicAttributesResponse result = snsClient.getTopicAttributes(request);
      LOG.info("Status is {}\nAttributes: {}\n", result.sdkHttpResponse().statusCode(),
          result.attributes());
      return result.attributes();
    }
  }

  public static void publishTopic(AwsCredentialsProvider awsCp, String topicArn, String message) {
    try (var snsClient = getSnsClient(awsCp)) {
      PublishRequest request = PublishRequest.builder().message(message).topicArn(topicArn).build();
      PublishResponse response = snsClient.publish(request);
      LOG.info("SNS topic published. Status: {}", response.sdkHttpResponse().statusCode());
    }
  }
}
