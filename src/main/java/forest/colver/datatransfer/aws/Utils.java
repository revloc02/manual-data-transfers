package forest.colver.datatransfer.aws;

import static forest.colver.datatransfer.config.Utils.userCreds;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

/**
 * AWS specific utils
 */
public class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  public static Map<String, String> personalSandboxRole;
  public static final String PERSONAL_SANDBOX_KEY_ID = userCreds.getProperty(
      "aws-access-personal-sandbox.access-key-id");
  public static final String PERSONAL_SANDBOX_SECRET = userCreds.getProperty(
      "aws-access-personal-sandbox.secret-access-key");
  public static final String NP_KEY_ID = userCreds.getProperty("aws-access-np.access-key-id");
  public static final String NP_SECRET = userCreds.getProperty("aws-access-np.secret-access-key");
  public static final String PROD_KEY_ID = userCreds.getProperty("aws-access-prod.access-key-id");
  public static final String PROD_SECRET = userCreds.getProperty("aws-access-prod.secret-access-key");

  // todo: see if I can make a config file separate from the creds file to contain these queue names
  public static final String EMX_SANDBOX_TEST_SQS1 = userCreds.getProperty("aws-emx-sandbox-test-sqs1");
  public static final String EMX_SANDBOX_TEST_SQS2 = userCreds.getProperty("aws-emx-sandbox-test-sqs2");
  public static final String S3_INTERNAL = userCreds.getProperty("aws-enterprise-sandbox-test-s3-internal");
  public static final String S3_TARGET_CUSTOMER = userCreds.getProperty("aws-enterprise-sandbox-test-s3-target-customer");
  public static final String S3_SOURCE_CACHE = userCreds.getProperty("aws-enterprise-sandbox-test-s3-source-cache");

  public static final String PERSONAL_SANDBOX_TEST_SNS_TOPIC_ARN = userCreds.getProperty("aws-personal-sandbox-test-sns-topic-arn");
  public static final String PERSONAL_SANDBOX_SQS_SUB_SNS = "sub_demo_adv_queue";

  public static AwsCredentialsProvider getEmxNpCreds() {
    return ProfileCredentialsProvider.create("enterprise-np");
  }

  public static AwsCredentialsProvider getEmxSbCreds() { // enterprise sandbox
    return ProfileCredentialsProvider.create("enterprise-sb");
  }

  public static AwsCredentialsProvider getEmxProdCreds() {
    return ProfileCredentialsProvider.create("enterprise-prod");
  }

  public static AwsCredentialsProvider getPersonalSbCreds() {
    return ProfileCredentialsProvider.create("personal-sandbox");
  }

  public static SqsClient getSqsClient(AwsCredentialsProvider awsCredentialsProvider) {
    return SqsClient.builder()
        .region(US_EAST_1)
        .credentialsProvider(awsCredentialsProvider)
        .build();
  }

  public static SnsClient getSnsClient(AwsCredentialsProvider awsCredentialsProvider) {
    return SnsClient.builder()
        .region(US_EAST_1)
        .credentialsProvider(awsCredentialsProvider)
        .build();
  }

  public static S3Client getS3Client(AwsCredentialsProvider awsCredentialsProvider) {
    return S3Client.builder()
        .region(Region.US_EAST_1)
        .credentialsProvider(awsCredentialsProvider)
        .build();
  }

  public static LambdaClient getLambdaClient(AwsCredentialsProvider awsCredentialsProvider) {
    return LambdaClient.builder()
        .region(Region.US_EAST_1)
        .credentialsProvider(awsCredentialsProvider)
        .build();
  }

  public static CloudWatchLogsClient getCloudWatchLogsClient(AwsCredentialsProvider awsCredentialsProvider) {
    return CloudWatchLogsClient.builder()
        .region(Region.US_EAST_1)
        .credentialsProvider(awsCredentialsProvider)
        .build();
  }

  /**
   * A helper method to take a Map<String, String> can convert it into a Map<String,
   * MessageAttributeValue> (SQS message.messageAttributes) so it can be used as an input value for
   * something like an SQS SendMessageRequest.
   *
   * @param properties A map of the message attributes.
   * @return A map of message attributes converted to the format an SQS Request object needs.
   */
  public static Map<String, MessageAttributeValue> createSqsMessageAttributes(
      Map<String, String> properties) {
    final Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      var val =
          MessageAttributeValue.builder().dataType("String").stringValue(entry.getValue()).build();
      messageAttributes.put(entry.getKey(), val);
    }
    return messageAttributes;
  }

  /**
   * A helper method that takes Map<String, MessageAttributeValue>, usually associated with an SQS
   * message, and converts it to a Map<String, String>, which is typical more usable.
   *
   * @param messageAttributes software.amazon.awssdk.services.sqs.model.message.messageAttributes()
   * @return A map of message attributes that are just Strings.
   */
  public static Map<String, String> convertSqsMessageAttributesToStrings(
      Map<String, MessageAttributeValue> messageAttributes) {
    Map<String, String> map = new HashMap<>();
    for (Map.Entry<String, MessageAttributeValue> entry : messageAttributes.entrySet()) {
      map.put(entry.getKey(), entry.getValue().stringValue());
    }
    return map;
  }

  // todo: this also needs a javadoc
  public static void awsResponseValidation(AwsResponse response) {
    var responseCode = response.sdkHttpResponse().statusCode();
    if (responseCode >= 300) {
      LOG.info("ERROR: {}, responseMetadata={}", responseCode, response.responseMetadata().toString());
      throw new IllegalStateException(
          "Unsuccessful AWS request. Status Code: " + responseCode);
    }
  }
}
