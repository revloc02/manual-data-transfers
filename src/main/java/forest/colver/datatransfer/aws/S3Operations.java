package forest.colver.datatransfer.aws;

import static forest.colver.datatransfer.aws.Utils.awsResponseValidation;
import static forest.colver.datatransfer.aws.Utils.getS3Client;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteMarkerEntry;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

/** Operation for connecting to S3 buckets. */
public class S3Operations {

  private S3Operations() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

  private static final Logger LOG = LoggerFactory.getLogger(S3Operations.class);
  private static final String PUT_SUCCESS = "S3PUT: The object {} was put on the {} bucket.";

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
  public static Optional<String> s3Put(
      S3Client s3Client, String bucket, String objectKey, String payload) {
    var putObjectRequest = PutObjectRequest.builder().bucket(bucket).key(objectKey).build();
    var requestBody = RequestBody.fromString(payload);
    var putObjectResponse = s3Client.putObject(putObjectRequest, requestBody);
    awsResponseValidation(putObjectResponse);
    LOG.info(PUT_SUCCESS, objectKey, bucket);
    return Optional.ofNullable(putObjectResponse.versionId());
  }

  /**
   * s3Put with AwsCreds and metadata. Put an object on a desired S3 bucket including some metadata.
   * Creates an S3Client--good to use this for one-off S3 operations, as opposed to doing several S3
   * operations and passing the client around.
   */
  public static void s3Put(
      AwsCredentialsProvider awsCp,
      String bucket,
      String objectKey,
      String payload,
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
  public static void s3Put(
      S3Client s3Client,
      String bucket,
      String objectKey,
      String payload,
      Map<String, String> metadata) {
    var putObjectRequest =
        PutObjectRequest.builder().bucket(bucket).key(objectKey).metadata(metadata).build();
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
    LOG.info(PUT_SUCCESS, putObjectRequest.key(), putObjectRequest.bucket());
  }

  /**
   * s3Head with AwsCreds. The HEAD action retrieves metadata from an object without returning the
   * object itself. This action is useful if you're only interested in an object's metadata.
   */
  public static HeadObjectResponse s3Head(
      AwsCredentialsProvider awsCp, String bucket, String objectKey) {
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
   * useful, meaning I will probably just use the s3Copy with S3Client.
   */
  public static void s3Copy(
      AwsCredentialsProvider awsCp,
      String sourceBucket,
      String sourceKey,
      String destBucket,
      String destKey) {
    try (var s3Client = getS3Client(awsCp)) {
      s3Copy(s3Client, sourceBucket, sourceKey, destBucket, destKey);
    }
  }

  /** s3Copy with s3Client. Copies an object from one s3 to another. */
  public static void s3Copy(
      S3Client s3Client, String sourceBucket, String sourceKey, String destBucket, String destKey) {
    var copyObjectRequest =
        CopyObjectRequest.builder()
            .sourceBucket(sourceBucket)
            .sourceKey(sourceKey)
            .destinationBucket(destBucket)
            .destinationKey(destKey)
            .build();
    var copyObjectResponse = s3Client.copyObject(copyObjectRequest);
    awsResponseValidation(copyObjectResponse);
    LOG.info(
        "S3COPY: Copied object from {}/{} to {}/{}", sourceBucket, sourceKey, destBucket, destKey);
  }

  /**
   * Copies all objects from a given S3 and key-prefix to another S3 bucket. This implementation
   * uses s3List which batches 1000 objects at a time. (The challenge with a CopyAll method is that
   * since each object is not consumed there needs to be a way to iterate through all the objects on
   * the S3 without copying the same one over again, in this case, using s3List with
   * nextContinuationToken helps accomplish this.)
   *
   * @param sourceBucket Source S3.
   * @param keyPrefix aka the file path.
   * @param destBucket Target S3.
   */
  public static void s3CopyAll(
      S3Client s3Client, String sourceBucket, String keyPrefix, String destBucket) {
    var response = s3ListResponse(s3Client, sourceBucket, keyPrefix, 1000);
    for (var object : response.contents()) {
      s3Copy(s3Client, sourceBucket, object.key(), destBucket, object.key());
    }
    var keepCopying = response.isTruncated();
    while (Boolean.TRUE.equals(keepCopying)) {
      response =
          s3ListContResponse(s3Client, sourceBucket, keyPrefix, response.nextContinuationToken());
      for (var object : response.contents()) {
        s3Copy(s3Client, sourceBucket, object.key(), destBucket, object.key());
      }
      keepCopying = response.isTruncated();
    }
  }

  /**
   * Uses s3List variants to count the number of objects in an S3 directory even if it is over 1000
   * items.
   *
   * @param bucket S3.
   * @param keyPrefix S3 directory.
   * @return The number of objects in the key-prefix "directory"
   */
  public static int s3CountAll(S3Client s3Client, String bucket, String keyPrefix) {
    // build an empty response so nextContinuationToken() can be referenced even though it's null
    var response = ListObjectsV2Response.builder().build();
    var count = 0;
    var keepCounting = true;
    while (Boolean.TRUE.equals(keepCounting)) {
      response = s3ListContResponse(s3Client, bucket, keyPrefix, response.nextContinuationToken());
      count = count + response.contents().size();
      keepCounting = response.isTruncated();
      LOG.info("count={}", count);
    }
    LOG.info("S3COUNT: Counted {} objects in {}/{}", count, bucket, keyPrefix);
    return count;
  }

  /**
   * DOES NOT WORK! Use the s3Get where you pass in the s3Client.
   *
   * <p>Get an object on a desired S3 bucket, creates an S3Client. This method does not work because
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
   *     a client for each S3 connection/operation caused said client to be garbage collected by
   *     Java before the s3Client.getObject(getObjectRequest) download was finished, and errors
   *     ensued. Passing in the s3client allows it to stay around longer so that Get operation can
   *     finish.
   * @param bucket S3 bucket.
   * @param objectKey Object path and name.
   * @return Input stream that provides access to the unmarshalled POJO response returned by the
   *     service in addition to the streamed contents.
   */
  public static ResponseInputStream<GetObjectResponse> s3Get(
      S3Client s3Client, String bucket, String objectKey) {
    var getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(objectKey).build();
    var getObjectResponse = s3Client.getObject(getObjectRequest);
    awsResponseValidation(getObjectResponse.response());
    LOG.info("S3GET: The object {} was retrieved from the {} bucket.\n", objectKey, bucket);
    return getObjectResponse;
  }

  /**
   * Retrieves an object from an S3 bucket, optionally specifying a version ID.
   *
   * @param s3Client An S3Client instance to use for the operation.
   * @param bucket The name of the S3 bucket.
   * @param key The path and object name in the S3 bucket.
   * @param versionId An optional version ID for the object. If present, retrieves that specific
   *     version.
   * @return A ResponseInputStream containing the object data and metadata.
   */
  public static ResponseInputStream<GetObjectResponse> s3Retrieve(
      S3Client s3Client, String bucket, String key, Optional<String> versionId) {
    var gorBuilder = GetObjectRequest.builder().bucket(bucket).key(key);
    versionId.ifPresent(gorBuilder::versionId);
    var getObjectResponse = s3Client.getObject(gorBuilder.build());
    awsResponseValidation(getObjectResponse.response());
    LOG.info(
        "S3RETRIEVE: The object {}, version {}, was retrieved from the {} bucket.\n",
        key,
        versionId,
        bucket);
    return getObjectResponse;
  }

  /** Retrieves and deletes an object from an S3 bucket. */
  public static ResponseInputStream<GetObjectResponse> s3Consume(
      S3Client s3Client, String bucket, String objectKey) {
    var response = s3Get(s3Client, bucket, objectKey);
    s3Delete(s3Client, bucket, objectKey);
    return response;
  }

  /**
   * S3List with AwsCreds, creates S3Client. List all the objects at a certain directory
   * (keyPrefix).
   *
   * @param keyPrefix The "folder" on the S3 to list. E.g. "revloc02/source/test/test.txt"
   */
  public static List<S3Object> s3List(
      AwsCredentialsProvider awsCp, String bucket, String keyPrefix) {
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

  /**
   * S3List with S3Client. List objects (up to 1000) at a certain directory (keyPrefix).
   *
   * @param keyPrefix The "folder" on the S3 to list.
   * @param maxKeysReq Sets the maximum number of keys returned in the response, max 1000.
   */
  public static List<S3Object> s3List(
      S3Client s3Client, String bucket, String keyPrefix, int maxKeysReq) {
    int maxKeys;
    if (maxKeysReq > 1000) {
      LOG.info(
          "Request to list {} items exceeds the maximum, using max of 1000 instead.", maxKeysReq);
      maxKeys = 1000;
    } else {
      maxKeys = maxKeysReq;
    }
    var listObjectsV2Request =
        ListObjectsV2Request.builder().bucket(bucket).prefix(keyPrefix).maxKeys(maxKeys).build();
    var listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);
    awsResponseValidation(listObjectsV2Response);
    var objects = listObjectsV2Response.contents();
    for (var object : objects) {
      LOG.info("S3LIST: The object {} is on the {} bucket.", object, bucket);
    }
    LOG.info("{} items listed.", listObjectsV2Response.keyCount());
    return objects;
  }

  /**
   * Lists the versions of objects in an S3 bucket. A version could be multiple files of the same
   * name, or a file that has been deleted, which is still stored under a "Delete marker."
   */
  public static List<ObjectVersion> s3ListVersions(
      S3Client s3Client, String bucket, String keyPrefix) {
    var listRequest = ListObjectVersionsRequest.builder().bucket(bucket).prefix(keyPrefix).build();
    var listResponse = s3Client.listObjectVersions(listRequest);
    awsResponseValidation(listResponse);
    var versions = listResponse.versions();
    for (var version : versions) {
      LOG.info("S3LISTVERSIONS: The object {} is on the {} bucket.", version, bucket);
    }
    LOG.info("{} versions listed.", listResponse.versions().size());
    return versions;
  }

  /**
   * Lists the deleteMarkers of objects in an S3 bucket. A Delete Marker is a versioned object that
   * got deleted, but still exists as a version on the S3 and is marked with the delete marker. If
   * you delete a Delete Marker and versions still exist, the latest version of the object becomes
   * available again.
   */
  public static List<DeleteMarkerEntry> s3ListDeleteMarkers(
      S3Client s3Client, String bucket, String keyPrefix) {
    var listRequest = ListObjectVersionsRequest.builder().bucket(bucket).prefix(keyPrefix).build();
    var listResponse = s3Client.listObjectVersions(listRequest);
    awsResponseValidation(listResponse);
    var deleteMarkers = listResponse.deleteMarkers();
    for (var deleteMarker : deleteMarkers) {
      LOG.info("S3LISTDELETEMARKERS: The object {} is on the {} bucket.", deleteMarker, bucket);
    }
    LOG.info("{} delete markers listed.", listResponse.deleteMarkers().size());
    return deleteMarkers;
  }

  /**
   * Lists the versions of objects in an S3 bucket. A version could be multiple files of the same
   * name, or a file that has been deleted, which is still stored as a delete marker. I still don't
   * know how the Paginator is useful, but keeping this for now. Currently, this method has worked
   * exactly the same as s3ListVersions.
   */
  public static SdkIterable<ObjectVersion> s3ListVersionsPaginator(
      S3Client s3Client, String bucket, String keyPrefix) {
    var listRequest = ListObjectVersionsRequest.builder().bucket(bucket).prefix(keyPrefix).build();
    var listResponse = s3Client.listObjectVersionsPaginator(listRequest);
    var versions = listResponse.versions();
    for (var version : versions) {
      LOG.info("S3LISTVERSIONS: The object {} is on the {} bucket.", version, bucket);
    }
    LOG.info("{} versions listed.", versions.stream().count());
    return versions;
  }

  /**
   * S3List with AwsCreds, creates S3Client. List up to 10 of the objects at a certain directory
   * (keyPrefix).
   *
   * @param keyPrefix The "folder" on the S3 to list. E.g. "revloc02/source/test/test.txt"
   */
  public static ListObjectsV2Response s3ListResponse(
      AwsCredentialsProvider awsCp, String bucket, String keyPrefix) {
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
  public static ListObjectsV2Response s3ListResponse(
      S3Client s3Client, String bucket, String keyPrefix, int maxKeys) {
    var listObjectsV2Request =
        ListObjectsV2Request.builder().bucket(bucket).prefix(keyPrefix).maxKeys(maxKeys).build();
    var listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);
    awsResponseValidation(listObjectsV2Response);
    LOG.info("S3LIST: Retrieved a list of {} objects from {}/{}", maxKeys, bucket, keyPrefix);
    return listObjectsV2Response;
  }

  /**
   * S3List with S3Client. List objects at a certain directory (keyPrefix), using a
   * nextContinuationToken from a previous S3List in order to get the next batch of objects.
   *
   * @param keyPrefix The "folder" on the S3 to list.
   * @param nextContinuationToken Indicates to Amazon S3 that the list is being continued on this
   *     bucket with a token from a previous listing.
   */
  public static ListObjectsV2Response s3ListContResponse(
      S3Client s3Client, String bucket, String keyPrefix, String nextContinuationToken) {
    var listObjectsV2Request =
        ListObjectsV2Request.builder()
            .bucket(bucket)
            .prefix(keyPrefix)
            .maxKeys(1000)
            .continuationToken(nextContinuationToken)
            .build();
    var listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);
    awsResponseValidation(listObjectsV2Response);
    LOG.info(
        "S3LIST: Retrieved a list of {} objects from {}/{}",
        listObjectsV2Response.contents().size(),
        bucket,
        keyPrefix);
    return listObjectsV2Response;
  }

  /** S3Delete with creds, creates S3Client. Delete an object from an S3. */
  public static void s3Delete(AwsCredentialsProvider awsCp, String bucket, String objectKey) {
    try (var s3Client = getS3Client(awsCp)) {
      s3Delete(s3Client, bucket, objectKey);
    }
  }

  /** S3Delete with S3Client. Delete an object from an S3. */
  public static void s3Delete(S3Client s3Client, String bucket, String objectKey) {
    var deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key(objectKey).build();
    var deleteObjectResponse = s3Client.deleteObject(deleteObjectRequest);
    awsResponseValidation(deleteObjectResponse);
    LOG.info("S3DELETE: The object {} was deleted from the {} bucket.", objectKey, bucket);
  }

  /**
   * S3Delete with versionId using S3Client. Delete an object version from an S3. A versioned object
   * that has been deleted, still has a delete marker, using a versionId in the delete request will
   * then delete the delete marker.
   */
  public static void s3DeleteVersion(
      S3Client s3Client, String bucket, String versionKey, String versionId) {
    var deleteObjectRequest =
        DeleteObjectRequest.builder().bucket(bucket).key(versionKey).versionId(versionId).build();
    var deleteObjectResponse = s3Client.deleteObject(deleteObjectRequest);
    awsResponseValidation(deleteObjectResponse);
    LOG.info(
        "S3DELETEVERSION: The object {} with version {} was deleted from the {} bucket.",
        versionKey,
        versionId,
        bucket);
  }

  /** S3Delete with S3Client. Delete all objects from an S3 key prefix. */
  public static void s3DeleteAll(S3Client s3Client, String bucket, String keyPrefix) {
    var objects = s3List(s3Client, bucket, keyPrefix, 1000);
    while (!objects.isEmpty()) {
      for (var object : objects) {
        s3Delete(s3Client, bucket, object.key());
      }
      objects = s3List(s3Client, bucket, keyPrefix, 1000);
    }
  }

  public static void s3Move(
      S3Client s3Client, String sourceBucket, String sourceKey, String destBucket, String destKey) {
    s3Copy(s3Client, sourceBucket, sourceKey, destBucket, destKey);
    s3Delete(s3Client, sourceBucket, sourceKey);
    LOG.info(
        "S3MOVE: Moved object from {}/{} to {}/{}", sourceBucket, sourceKey, destBucket, destKey);
  }

  /**
   * Moves all objects from one s3 key prefix to another. Note that it moves them up to 1000 at a
   * time, but continues until they all have been moved.
   */
  public static void s3MoveAll(
      S3Client s3Client, String sourceBucket, String keyPrefix, String destBucket) {
    var objects = s3List(s3Client, sourceBucket, keyPrefix, 1000);
    while (!objects.isEmpty()) {
      for (var object : objects) {
        s3Move(s3Client, sourceBucket, object.key(), destBucket, object.key());
      }
      objects = s3List(s3Client, sourceBucket, keyPrefix, 1000);
    }
  }
}
