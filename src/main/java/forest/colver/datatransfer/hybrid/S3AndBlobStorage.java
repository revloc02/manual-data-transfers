package forest.colver.datatransfer.hybrid;

import static forest.colver.datatransfer.aws.S3Operations.s3Get;
import static forest.colver.datatransfer.aws.Utils.getS3Client;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPut;

import java.io.IOException;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class S3AndBlobStorage {

  public static void moveS3toAzureBlob(AwsCredentialsProvider awsCp, String bucket, String objectKey, String connectStr, String endpoint, String containerName) {
    try (var s3Client = getS3Client(awsCp)) {
      var response = s3Get(s3Client, bucket, objectKey);
      var contents = new String(response.readAllBytes());
      blobPut(connectStr, endpoint, containerName, objectKey, contents);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  // todo: at some point make copy ops.
  /**
   * Get all of the S3 objects from a directory and move them to an Azure Storage Container.
   * @param awsCp
   * @param bucket
   * @param objectKey
   * @param connectStr
   * @param endpoint
   * @param containerName
   */
  public static void moveAllS3ToAzureBlob(AwsCredentialsProvider awsCp, String bucket, String objectKey, String connectStr, String endpoint, String containerName) {

  }
}
