package forest.colver.datatransfer.aws;

import static forest.colver.datatransfer.config.Utils.userCreds;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
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
  static String personalSandboxAccessKeyId = userCreds.getProperty(
      "aws-access-personal-sandbox.access-key-id");
  static String personalSandboxSecretAccessKey = userCreds.getProperty(
      "aws-access-personal-sandbox.secret-access-key");
  static String npKeyId = userCreds.getProperty("aws-access-np.access-key-id");
  static String npSecret = userCreds.getProperty("aws-access-np.secret-access-key");
  static String prodKeyId = userCreds.getProperty("aws-access-prod.access-key-id");
  static String prodSecret = userCreds.getProperty("aws-access-prod.secret-access-key");

  // todo: see if I can make a config file separate from the creds file to contain these queue names
  private static final String sqs1 = userCreds.getProperty("aws-test-sqs1");
  private static final String sqs2 = userCreds.getProperty("aws-test-sqs2");
  // todo: can these s3 refs be consolidated? can we just use 1 or 2
  private static final String s3Internal = userCreds.getProperty("aws-test-s3-internal");
  private static final String s3TargetCustomer = userCreds.getProperty("aws-test-s3-target-customer");
  private static final String s3SourceCache = userCreds.getProperty("aws-test-s3-source-cache");

  public static AwsCredentialsProvider getNpCreds() {
    return ProfileCredentialsProvider.create("enterprise-np");
  }

  public static AwsCredentialsProvider getSbCreds() { // enterprise sandbox
    return ProfileCredentialsProvider.create("enterprise-sb");
  }

  public static AwsCredentialsProvider getProdCreds() {
    return StaticCredentialsProvider.create(
        AwsBasicCredentials.create(getProdKeyId(), getProdSecret()));
  }

  public static AwsCredentialsProvider getPrsnlSbCreds() {
    return ProfileCredentialsProvider.create("personal-sandbox");
//    AwsBasicCredentials awsCredentials =
//        AwsBasicCredentials.create(personalSandboxAccessKeyId, personalSandboxSecretAccessKey);
//    StaticCredentialsProvider awsCredentialsProvider =
//        StaticCredentialsProvider.create(awsCredentials);
//    AwsCredentialsProvider credentialsProvider;
//
//    Profile awsProfile =
//        Profile.builder().name("assume_role").properties(personalSandboxRole).build();
//    credentialsProvider =
//        new StsProfileCredentialsProviderFactory().create(awsCredentialsProvider, awsProfile);
//    return credentialsProvider;
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

  public static String getNpKeyId() {
    return npKeyId;
  }

  public static String getNpSecret() {
    return npSecret;
  }

  public static String getProdKeyId() {
    return prodKeyId;
  }

  public static String getProdSecret() {
    return prodSecret;
  }

  public static String getSqs1() {
    return sqs1;
  }

  public static String getSqs2() {
    return sqs2;
  }

  public static String getS3Internal() {
    return s3Internal;
  }

  public static String getS3TargetCustomer() {
    return s3TargetCustomer;
  }

  public static String getS3SourceCache() {
    return s3SourceCache;
  }
}
