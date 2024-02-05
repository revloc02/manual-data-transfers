package forest.colver.datatransfer.aws;

import static forest.colver.datatransfer.aws.Utils.awsResponseValidation;
import static forest.colver.datatransfer.aws.Utils.getS3Client;

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
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Operation for connecting to S3 buckets.
 */
public class S3Operations {

  private static final Logger LOG = LoggerFactory.getLogger(S3Operations.class);
  private static final String PUT_SUCCESS = "S3PUT: The object {} was put on the {} bucket.";

  private S3Operations() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

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
    LOG.info(PUT_SUCCESS, objectKey, bucket);
  }

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
    LOG.info(PUT_SUCCESS, objectKey, bucket);
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
    LOG.info(PUT_SUCCESS, putObjectRequest.key(),
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

  /**
   * Yeah, so...s3Copy with AwsCreds. Copies an object. Yeah, so...this might not actually be that
   * useful.
   */
  public static void s3Copy(AwsCredentialsProvider awsCp, String sourceBucket, String sourceKey,
      String destBucket, String destKey) {
    try (var s3Client = getS3Client(awsCp)) {
      s3Copy(s3Client, sourceBucket, sourceKey, destBucket, destKey);
    }
  }

  /**
   * s3Copy with s3Client. Copies an object from one s3 to another.
   */
  public static void s3Copy(S3Client s3Client, String sourceBucket, String sourceKey,
      String destBucket, String destKey) {
    var copyObjectRequest = CopyObjectRequest.builder().sourceBucket(sourceBucket)
        .sourceKey(sourceKey).destinationBucket(destBucket).destinationKey(destKey).build();
    var copyObjectResponse = s3Client.copyObject(copyObjectRequest);
    awsResponseValidation(copyObjectResponse);
    LOG.info("S3COPY: Copied object from {}/{} to {}/{}", sourceBucket, sourceKey, destBucket, destKey);
  }

  public static void s3CopyAll() {
    // todo: s3List only returns up to 1000 files. Is there a way to count the number of objects in an s3 directory? If there is more than 1000 abort?
  }

  /**
   * DOES NOT WORK! Use the s3Get where you pass in the s3Client.
   * <p>
   * Get an object on a desired S3 bucket, creates an S3Client. This method does not work because
   * the S3 client gets garbage collected (by Java) in the middle of the download. Error looks like
   * this: org.apache.http.ConnectionClosedException: Premature end of Content-Length delimited
   * message body (expected: 56; received: 0). See: <a
   * href="https://stackoverflow.com/a/10510365">stackoverflow.com/a/10510365</a>
   */
  public static ResponseInputStream<GetObjectResponse> s3Get(
      AwsCredentialsProvider awsCp, String bucket, String objectKey) {
    try (var s3Client = getS3Client(awsCp)) {
      return s3Get(s3Client, bucket, objectKey);
    }
  }

  /**
   * Get an object from an S3 bucket. Pass in the S3Client--good for stringing multiple S3 calls
   * together so only one client is created.
   *
   * @param s3Client Pass in the S3 client. It was discovered that passing credentials and creating
   * a client for each S3 connection/operation caused said client to be garbage collected by Java
   * before the s3Client.getObject(getObjectRequest) download was finished, and errors ensued.
   * Passing in the s3client allows it to stay around longer so that Get operation can finish.
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
   * @param keyPrefix The "folder" on the S3 to list. E.g. "revloc02/source/test/test.txt"
   */
  public static List<S3Object> s3List(AwsCredentialsProvider awsCp, String bucket,
      String keyPrefix) {
    try (var s3Client = getS3Client(awsCp)) {
      return s3List(s3Client, bucket, keyPrefix);
    }
  }

  /**
   * S3List with S3Client. Lists up to 10 of the objects at a certain directory (keyPrefix).
   *
   * @param keyPrefix The "folder" on the S3 to list.
   */
  public static List<S3Object> s3List(S3Client s3Client, String bucket, String keyPrefix) {
    // returns a list of only 10 items or fewer. If you need more, use s3List with maxKeys.
    return s3List(s3Client, bucket, keyPrefix, 10);
  }

  // todo: what happens if `maxKeys` parameter passed in is > 1000?
  /**
   * S3List with S3Client. List objects (up to 1000) at a certain directory (keyPrefix).
   *
   * @param keyPrefix The "folder" on the S3 to list.
   * @param maxKeys Sets the maximum number of keys returned in the response, max 1000.
   */
  public static List<S3Object> s3List(S3Client s3Client, String bucket, String keyPrefix, int maxKeys) {
    var listObjectsRequest =
        ListObjectsV2Request.builder().bucket(bucket).prefix(keyPrefix).maxKeys(maxKeys).build();
    var listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
    awsResponseValidation(listObjectsResponse);
    var objects = listObjectsResponse.contents();
    for (var object : objects) {
      LOG.info("S3LIST: The object {} is on the {} bucket.", object, bucket);
    }
    return objects;
  }

  /**
   * S3List with AwsCreds, creates S3Client. List up to 10 of the objects at a certain directory
   * (keyPrefix).
   *
   * @param keyPrefix The "folder" on the S3 to list. E.g. "revloc02/source/test/test.txt"
   */
  public static ListObjectsResponse s3ListResponse(AwsCredentialsProvider awsCp, String bucket,
      String keyPrefix) {
    try (var s3Client = getS3Client(awsCp)) {
      return s3ListResponse(s3Client, bucket, keyPrefix, 10);
    }
  }

  /**
   * S3List with S3Client. List objects at a certain directory (keyPrefix).
   *
   * @param keyPrefix The "folder" on the S3 to list.
   * @param maxKeys Sets the maximum number of keys returned in the response.
   */
  public static ListObjectsResponse s3ListResponse(S3Client s3Client, String bucket, String keyPrefix, int maxKeys) {
    var listObjectsRequest =
        ListObjectsRequest.builder().bucket(bucket).prefix(keyPrefix).maxKeys(maxKeys).build();
    var listObjectsResponse = s3Client.listObjects(listObjectsRequest);
    awsResponseValidation(listObjectsResponse);
    return listObjectsResponse;
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
    LOG.info("S3DELETE: The object {} was deleted from the {} bucket.", objectKey, bucket);
  }

  // todo: this needs a unit test
  /**
   * S3Delete with S3Client. Delete all objects from an S3 key prefix.
   */
  public static void s3DeleteAll(S3Client s3Client, String bucket, String keyPrefix) {
    var objects = s3List(s3Client, bucket, keyPrefix, 1000);
    while (!objects.isEmpty()) {
      for(var object : objects) {
        s3Delete(s3Client, bucket, object.key());
      }
      objects = s3List(s3Client, bucket, keyPrefix, 1000);
    }
  }

  public static void s3Move(S3Client s3Client, String sourceBucket, String sourceKey,
      String destBucket, String destKey) {
    s3Copy(s3Client, sourceBucket, sourceKey, destBucket, destKey);
    s3Delete(s3Client, sourceBucket, sourceKey);
    LOG.info("S3MOVE: Moved object from {}/{} to {}/{}", sourceBucket, sourceKey, destBucket, destKey);
  }

  public static void s3MoveAll(S3Client s3Client, String sourceBucket, String keyPrefix,
      String destBucket) {
    var objects = s3List(s3Client, sourceBucket, keyPrefix, 1000);
    while (!objects.isEmpty()) {
      for(var object : objects) {
        s3Move(s3Client, sourceBucket, object.key(), destBucket, object.key());
      }
      objects = s3List(s3Client, sourceBucket, keyPrefix, 1000);
    }
  }

  // todo: how about a copyAll? Copy everything in a keyPrefix to another s3.
  // todo: and if copyAll works how about a moveAll?
}
