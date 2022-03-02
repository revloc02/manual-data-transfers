package forest.colver.datatransfer.aws;

import static forest.colver.datatransfer.config.Utils.userCreds;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

/**
 * AWS specific utils
 */
public class Utils {

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
  public static final String SQS1 = userCreds.getProperty("aws-enterprise-sandbox-test-sqs1");
  public static final String SQS2 = userCreds.getProperty("aws-enterprise-sandbox-test-sqs2");
  // todo: can these s3 refs be consolidated? can we just use 1 or 2
  public static final String S3_INTERNAL = userCreds.getProperty("aws-enterprise-sandbox-test-s3-internal");
  public static final String S3_TARGET_CUSTOMER = userCreds.getProperty("aws-enterprise-sandbox-test-s3-target-customer");
  public static final String S3_SOURCE_CACHE = userCreds.getProperty("aws-enterprise-sandbox-test-s3-source-cache");

  public static AwsCredentialsProvider getNpCreds() {
    return ProfileCredentialsProvider.create("enterprise-np");
  }

  public static AwsCredentialsProvider getSbCreds() { // enterprise sandbox
    return ProfileCredentialsProvider.create("enterprise-sb");
  }

  public static AwsCredentialsProvider getProdCreds() {
    return ProfileCredentialsProvider.create("enterprise-prod");
  }

  public static AwsCredentialsProvider getPrsnlSbCreds() {
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

  public static Map<String, MessageAttributeValue> createMessageAttributes(
      Map<String, String> properties) {
    final Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      var val =
          MessageAttributeValue.builder().dataType("String").stringValue(entry.getValue()).build();
      messageAttributes.put(entry.getKey(), val);
    }
    return messageAttributes;
  }

  public static void awsResponseValidation(AwsResponse response) {
    var responseCode = response.sdkHttpResponse().statusCode();
    if (responseCode >= 300) {
      throw new IllegalStateException(
          "Unsuccessful S3 object request. Status Code: " + responseCode);
    }
  }
}
