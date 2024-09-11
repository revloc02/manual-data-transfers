package forest.colver.datatransfer.hybrid;

import static forest.colver.datatransfer.aws.S3Operations.s3Consume;
import static forest.colver.datatransfer.aws.S3Operations.s3Get;
import static forest.colver.datatransfer.aws.S3Operations.s3List;
import static forest.colver.datatransfer.aws.Utils.getS3Client;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPut;

import java.io.IOException;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class S3AndBlobStorage {

  private S3AndBlobStorage() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

  /**
   * Move one object from S3 to Azure Blob.
   *
   * @param awsCp AWS creds.
   * @param bucket S3.
   * @param objectKey S3 object key.
   * @param connectStr Azure connection String.
   * @param endpoint Azure Blob service endpoint.
   * @param containerName Azure storage container name.
   * @throws IOException For readAllBytes() on the InputStream from the S3 object.
   */
  public static void moveOneS3toAzureBlob(AwsCredentialsProvider awsCp, String bucket,
      String objectKey, String connectStr, String endpoint, String containerName)
      throws IOException {
    try (var s3Client = getS3Client(awsCp)) {
      var response = s3Consume(s3Client, bucket, objectKey);
      var contents = new String(response.readAllBytes());
      blobPut(connectStr, endpoint, containerName, objectKey, contents);
    }
  }

  /**
   * Copy one object from S3 to Azure Blob.
   *
   * @param awsCp AWS creds.
   * @param bucket S3.
   * @param objectKey S3 object key.
   * @param connectStr Azure connection String.
   * @param endpoint Azure Blob service endpoint.
   * @param containerName Azure storage container name.
   * @throws IOException For readAllBytes() on the InputStream from the S3 object.
   */
  public static void copyOneS3toAzureBlob(AwsCredentialsProvider awsCp, String bucket,
      String objectKey, String connectStr, String endpoint, String containerName)
      throws IOException {
    try (var s3Client = getS3Client(awsCp)) {
      var response = s3Get(s3Client, bucket, objectKey);
      var contents = new String(response.readAllBytes());
      blobPut(connectStr, endpoint, containerName, objectKey, contents);
    }
  }

  /** Get all of the S3 objects from a directory and move them to an Azure Storage Container. */
  public static void moveAllS3ToAzureBlob(
      AwsCredentialsProvider awsCp,
      String bucket,
      String objectKey,
      String connectStr,
      String endpoint,
      String containerName) throws IOException {
      var objects = s3List(awsCp, bucket, objectKey);
      for (var object : objects) {
        moveOneS3toAzureBlob(awsCp, bucket, object.key(), connectStr, endpoint, containerName);
      }
  }
}
