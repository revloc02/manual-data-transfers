package forest.colver.datatransfer.aws;

import static forest.colver.datatransfer.aws.Utils.awsResponseValidation;
import static forest.colver.datatransfer.aws.Utils.getS3Client;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Operation for connecting to S3 buckets.
 */
public class S3Operations {

  private static final Logger LOG = LoggerFactory.getLogger(S3Operations.class);

  /**
   * Put an object on a desired S3 bucket.
   */
  public static void s3Put(
      AwsCredentialsProvider awsCp, String bucket, String objectKey, String payload) {
    try (var s3Client = getS3Client(awsCp)) {
      var putObjectRequest = PutObjectRequest.builder().bucket(bucket).key(objectKey).build();
      var requestBody = RequestBody.fromString(payload);
      var putObjectResponse = s3Client.putObject(putObjectRequest, requestBody);
      awsResponseValidation(putObjectResponse);
      LOG.info("S3PUT: The object {} was put on the {} bucket.\n", objectKey, bucket);
    }
  }

  /**
   * Put an object on a desired S3 bucket including some metadata.
   */
  public static void s3Put(
      AwsCredentialsProvider awsCp, String bucket, String objectKey, String payload,
      Map<String, String> metadata) {
    try (var s3Client = getS3Client(awsCp)) {
      var putObjectRequest = PutObjectRequest.builder()
          .bucket(bucket)
          .key(objectKey)
          .metadata(metadata)
          .build();
      var requestBody = RequestBody.fromString(payload);
      var putObjectResponse = s3Client.putObject(putObjectRequest, requestBody);
      awsResponseValidation(putObjectResponse);
      LOG.info("S3PUT: The object {} was put on the {} bucket.\n", objectKey, bucket);
    }
  }

  /**
   * The HEAD action retrieves metadata from an object without returning the object itself. This
   * action is useful if you're only interested in an object's metadata.
   */
  public static void s3Head(
      AwsCredentialsProvider awsCp, String bucket, String objectKey) {
    try (var s3Client = getS3Client(awsCp)) {
      var headObjectRequest = HeadObjectRequest.builder().bucket(bucket).key(objectKey).build();
      var headObjectResponse = s3Client.headObject(headObjectRequest);
      awsResponseValidation(headObjectResponse);
      LOG.info("{} Metadata:", objectKey);
      for (Map.Entry<String, String> entry : headObjectResponse.metadata().entrySet()) {
        LOG.info("  {}={}", entry.getKey(), entry.getValue());
      }
      LOG.info("  expires={}", headObjectResponse.expires());
      LOG.info("  expiration={}\n", headObjectResponse.expiration());
    }
  }

  public static void s3Copy(AwsCredentialsProvider awsCp, String sourceBucket, String sourceKey,
      String destBucket, String destKey) {
    try (var s3Client = getS3Client(awsCp)) {
      String encodedUrl = null;
      try {
        encodedUrl = URLEncoder.encode(sourceBucket + "/" + sourceKey,
            StandardCharsets.UTF_8.toString());
      } catch (UnsupportedEncodingException e) {
        System.out.println("URL could not be encoded: " + e.getMessage());
      }
      var copyObjectRequest = CopyObjectRequest.builder().copySource(encodedUrl)
          .destinationBucket(destBucket).destinationKey(destKey).build();
      var copyObjectResponse = s3Client.copyObject(copyObjectRequest);
      awsResponseValidation(copyObjectResponse);
    }
  }

  /**
   * Get an object on a desired S3 bucket.
   */
  public static void s3Get(
      AwsCredentialsProvider awsCp, String bucket, String objectKey) {
    try (var s3Client = getS3Client(awsCp)) {
      var getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(objectKey).build();
      var getObjectResponse = s3Client.getObject(getObjectRequest);
      LOG.info("Metadata:");
      for (Map.Entry<String, String> entry : getObjectResponse.response().metadata().entrySet()) {
        LOG.info("  {}={}", entry.getKey(), entry.getValue());
      }
//      LOG.info("S3GET: The object {} was got on the {} bucket.\n", getObjectResponse.readAllBytes(), bucket);
    }
  }

  /**
   * List all of the object at a certain directory (keyPrefix).
   *
   * @param keyPrefix The folder on the S3 to list.
   */
  public static List<S3Object> s3List(AwsCredentialsProvider awsCp, String bucket,
      String keyPrefix) {
    try (var s3Client = getS3Client(awsCp)) {
      var listObjectsRequest =
          ListObjectsRequest.builder().bucket(bucket).prefix(keyPrefix).build();
      var listObjectsResponse = s3Client.listObjects(listObjectsRequest);
      awsResponseValidation(listObjectsResponse);
      var objects = listObjectsResponse.contents();
      for (var object : objects) {
        LOG.info("S3LIST: The object {} is on the {} bucket.", object, bucket);
      }
      return objects;
    }
  }

  /**
   * Delete an object from an S3.
   */
  public static void s3Delete(AwsCredentialsProvider awsCp, String bucket, String objectKey) {
    try (var s3Client = getS3Client(awsCp)) {
      var deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key(objectKey).build();
      var deleteObjectResponse = s3Client.deleteObject(deleteObjectRequest);
      awsResponseValidation(deleteObjectResponse);
      LOG.info("S3DELETE: The object {} was deleted from the {} bucket.\n", objectKey, bucket);
    }
  }
}
