package forest.colver.datatransfer.hybrid;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class S3AndBlobStorage {

  /**
   * Get all of the S3 objects from a directory and move them to an Azure Storage Container.
   * @param awsCp
   * @param bucket
   * @param objectKey
   * @param connectStr
   * @param endpoint
   * @param containerName
   */
  public static void moveAllS3ToBlobStorage(AwsCredentialsProvider awsCp, String bucket, String objectKey, String connectStr, String endpoint, String containerName) {

  }
}
