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
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Operation for connecting to S3 buckets.
 */
public class S3Operations {

  private static final Logger LOG = LoggerFactory.getLogger(S3Operations.class);

  /**
   * s3Put with AwsCreds. Put an object on a desired S3 bucket. Creates an S3Client--good to use
   * this for one-off S3 operations, as opposed to doing several S3 operations and passing the
   * client around.
   */
  public static void s3Put(
      AwsCredentialsProvider awsCp, String bucket, String objectKey, String payload) {
    try (var s3Client = getS3Client(awsCp)) {
      s3Put(s3Client, bucket, objectKey, payload);
    }
  }

  /**
   * s3Put with S3Client. Put an object on a desired S3 bucket. Creates PutObjectRequest. Pass in
   * the S3Client--good for stringing multiple S3 calls together so only one client is created.
   */
  public static void s3Put(S3Client s3Client, String bucket, String objectKey, String payload) {
    var putObjectRequest = PutObjectRequest.builder().bucket(bucket).key(objectKey).build();
    var requestBody = RequestBody.fromString(payload);
    var putObjectResponse = s3Client.putObject(putObjectRequest, requestBody);
    awsResponseValidation(putObjectResponse);
    LOG.info("S3PUT: The object {} was put on the {} bucket.\n", objectKey, bucket);
  }

  // todo: does this need a unit test?

  /**
   * s3Put with AwsCreds and metadata. Put an object on a desired S3 bucket including some metadata.
   * Creates an S3Client--good to use this for one-off S3 operations, as opposed to doing several S3
   * operations and passing the client around.
   */
  public static void s3Put(
      AwsCredentialsProvider awsCp, String bucket, String objectKey, String payload,
      Map<String, String> metadata) {
    try (var s3Client = getS3Client(awsCp)) {
      s3Put(s3Client, bucket, objectKey, payload, metadata);
    }
  }

  // todo: does this need a unit test?

  /**
   * s3Put with AwsCreds and metadata. Put an object on a desired S3 bucket including some metadata.
   * Creates an S3Client--good to use this for one-off S3 operations, as opposed to doing several S3
   * operations and passing the client around.
   */
  public static void s3Put(S3Client s3Client, String bucket, String objectKey, String payload,
      Map<String, String> metadata) {
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

  /**
   * s3Put with AwsCreds and PutObjectRequest. Put an object on a desired S3 bucket. Creates an
   * S3Client--good to use this for one-off S3 operations, as opposed to doing several S3 operations
   * and passing the client around.
   */
  public static void s3Put(
      AwsCredentialsProvider awsCp, String payload, PutObjectRequest putObjectRequest) {
    try (var s3Client = getS3Client(awsCp)) {
      s3Put(s3Client, payload, putObjectRequest);
    }
  }

  /**
   * s3Put with S3Client and PutObjectRequest. Put an object on a desired S3 bucket. Pass in the
   * S3Client--good for stringing multiple S3 calls together so only one client is created.
   */
  public static void s3Put(S3Client s3Client, String payload, PutObjectRequest putObjectRequest) {
    var requestBody = RequestBody.fromString(payload);
    var putObjectResponse = s3Client.putObject(putObjectRequest, requestBody);
    awsResponseValidation(putObjectResponse);
    LOG.info("S3PUT: The object {} was put on the {} bucket.\n", putObjectRequest.key(),
        putObjectRequest.bucket());
  }

  /**
   * s3Head with AwsCreds. The HEAD action retrieves metadata from an object without returning the
   * object itself. This action is useful if you're only interested in an object's metadata.
   */
  public static HeadObjectResponse s3Head(AwsCredentialsProvider awsCp, String bucket,
      String objectKey) {
    try (var s3Client = getS3Client(awsCp)) {
      return s3Head(s3Client, bucket, objectKey);
    }
  }

  /**
   * s3Head with s3Client. The HEAD action retrieves metadata from an object without returning the
   * object itself. This action is useful if you're only interested in an object's metadata.
   */
  public static HeadObjectResponse s3Head(S3Client s3Client, String bucket, String objectKey) {
    var headObjectRequest = HeadObjectRequest.builder().bucket(bucket).key(objectKey).build();
    var headObjectResponse = s3Client.headObject(headObjectRequest);
    awsResponseValidation(headObjectResponse);
    return headObjectResponse;
  }

  // todo: keep this method? does it need a unit test?
  public static void s3Copy(AwsCredentialsProvider awsCp, String sourceBucket, String sourceKey,
      String destBucket, String destKey) {
    try (var s3Client = getS3Client(awsCp)) {
      s3Copy(s3Client, sourceBucket, sourceKey, destBucket, destKey);
    }
  }

  public static void s3Copy(S3Client s3Client, String sourceBucket, String sourceKey,
      String destBucket, String destKey) {
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

  // todo: this needs a unit test

  /**
   * Get an object on a desired S3 bucket. Creates an S3Client--good to use this for one-off S3
   * operations, as opposed to doing several S3 operations and passing the client around.
   */
  public static ResponseInputStream<GetObjectResponse> s3Get(
      AwsCredentialsProvider awsCp, String bucket, String objectKey) {
    try (var s3Client = getS3Client(awsCp)) {
      return s3Get(s3Client, bucket, objectKey);
    }
  }

  // todo: this needs a unit test

  /**
   * Get an object from an S3 bucket. Pass in the S3Client--good for stringing multiple S3 calls
   * together so only one client is created.
   *
   * @param s3Client Pass in the client. It was discovered that creating a client for each S3
   * connection caused the client to be garbage collected by Java before the download was finished,
   * and errors ensued. Passing in the client allows it to stay around longer so the operation can
   * finish.
   * @param bucket S3 bucket.
   * @param objectKey Object path and name.
   * @return Input stream that provides access to the unmarshalled POJO response returned by the
   * service in addition to the streamed contents.
   */
  public static ResponseInputStream<GetObjectResponse> s3Get(S3Client s3Client, String bucket,
      String objectKey) {
    var getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(objectKey).build();
    var getObjectResponse = s3Client.getObject(getObjectRequest);
    awsResponseValidation(getObjectResponse.response());
    LOG.info("S3GET: The object {} was retrieved from the {} bucket.\n", objectKey, bucket);
    return getObjectResponse;
  }

  /**
   * S3List with AwsCreds, creates S3Client. List all the objects at a certain directory
   * (keyPrefix).
   *
   * @param keyPrefix The "folder" on the S3 to list.
   */
  public static List<S3Object> s3List(AwsCredentialsProvider awsCp, String bucket,
      String keyPrefix) {
    try (var s3Client = getS3Client(awsCp)) {
      return s3List(s3Client, bucket, keyPrefix);
    }
  }

  /**
   * S3List with S3Client. List all the objects at a certain directory (keyPrefix).
   *
   * @param keyPrefix The "folder" on the S3 to list.
   */
  public static List<S3Object> s3List(S3Client s3Client, String bucket, String keyPrefix) {
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

  /**
   * S3Delete with creds, creates S3Client. Delete an object from an S3.
   */
  public static void s3Delete(AwsCredentialsProvider awsCp, String bucket, String objectKey) {
    try (var s3Client = getS3Client(awsCp)) {
      s3Delete(s3Client, bucket, objectKey);
    }
  }

  /**
   * S3Delete with S3Client. Delete an object from an S3.
   */
  public static void s3Delete(S3Client s3Client, String bucket, String objectKey) {
    var deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key(objectKey).build();
    var deleteObjectResponse = s3Client.deleteObject(deleteObjectRequest);
    awsResponseValidation(deleteObjectResponse);
    LOG.info("S3DELETE: The object {} was deleted from the {} bucket.\n", objectKey, bucket);
  }
}
