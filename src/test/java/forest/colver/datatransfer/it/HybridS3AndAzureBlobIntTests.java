package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.S3Operations.s3Delete;
import static forest.colver.datatransfer.aws.S3Operations.s3List;
import static forest.colver.datatransfer.aws.S3Operations.s3Put;
import static forest.colver.datatransfer.aws.Utils.S3_INTERNAL;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.aws.Utils.getS3Client;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobDelete;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobDeleteAll;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobGet;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobList;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_SA_FOREST_CONN_STR;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.hybrid.S3AndBlobStorage.copyOneS3toAzureBlob;
import static forest.colver.datatransfer.hybrid.S3AndBlobStorage.moveAllS3ToAzureBlob;
import static forest.colver.datatransfer.hybrid.S3AndBlobStorage.moveOneS3toAzureBlob;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HybridS3AndAzureBlobIntTests {

  private static final Logger LOG = LoggerFactory.getLogger(HybridS3AndAzureBlobIntTests.class);
  public static final String CONNECT_STR = EMX_SANDBOX_SA_FOREST_CONN_STR;

  @Test
  void testMoveS3toAzureBlob() throws IOException {
    LOG.info("...put a file on s3...");
    var objectKey = "revloc02/source/test/test.txt";
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    s3Put(creds, S3_INTERNAL, objectKey, payload);

    LOG.info("...verify the object is there on the s3...");
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isOne();
    assertThat(objects.get(0).key()).isEqualTo(objectKey);

    LOG.info("...move the object from S3 to Azure Blob...");
    var endpoint = "https://foresttestsa.blob.core.windows.net";
    var containerName = "forest-test-blob";
    moveOneS3toAzureBlob(creds, S3_INTERNAL, objectKey, CONNECT_STR, endpoint, containerName);

    LOG.info("...verify the move happened correctly...");
    var outputStream = blobGet(CONNECT_STR, endpoint, containerName, objectKey);
    String str = outputStream.toString(StandardCharsets.UTF_8);
    assertThat(str).isEqualTo(payload);

    LOG.info("...verify the file is no longer on the source s3...");
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects).isEmpty();

    LOG.info("...cleanup...");
    blobDelete(CONNECT_STR, endpoint, containerName, objectKey);
  }

  @Test
  void testCopyS3toAzureBlob() throws IOException {
    LOG.info("...put a file on s3...");
    var objectKey = "revloc02/source/test/test.txt";
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    s3Put(creds, S3_INTERNAL, objectKey, payload);

    LOG.info("...verify the object is there on the s3...");
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isOne();
    assertThat(objects.get(0).key()).isEqualTo(objectKey);

    LOG.info("...copy the object from S3 to Azure Blob...");
    var endpoint = "https://foresttestsa.blob.core.windows.net";
    var containerName = "forest-test-blob";
    copyOneS3toAzureBlob(creds, S3_INTERNAL, objectKey, CONNECT_STR, endpoint, containerName);

    LOG.info("...verify the copy happened correctly...");
    var outputStream = blobGet(CONNECT_STR, endpoint, containerName, objectKey);
    String str = outputStream.toString(StandardCharsets.UTF_8);
    assertThat(str).isEqualTo(payload);

    LOG.info("...verify the file is still on the source s3...");
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isOne();
    assertThat(objects.get(0).key()).isEqualTo(objectKey);

    LOG.info("...cleanup...");
    blobDelete(CONNECT_STR, endpoint, containerName, objectKey);
    s3Delete(creds, S3_INTERNAL, objectKey);
  }

  @Test
  void testMoveAllS3toAzureBlob() throws IOException {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      LOG.info("...place several files...");
      var numFiles = 6; // don't change this to >1000, because s3List can only list 1000 items
      var keyPrefix = "revloc02/source/test/";
      for (var i = 0; i < numFiles; i++) {
        var objectKey = keyPrefix + "test-" + i + ".txt";
        var payload = getDefaultPayload() + " " + i;
        s3Put(s3Client, S3_INTERNAL, objectKey, payload);
      }

      LOG.info("...verify the files are on the source...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(
              () ->
                  assertThat(s3List(s3Client, S3_INTERNAL, keyPrefix, numFiles)).hasSize(numFiles));

      LOG.info("...move objects from S3 to Azure Blob...");
      var endpoint = "https://foresttestsa.blob.core.windows.net";
      var containerName = "forest-test-blob";
      moveAllS3ToAzureBlob(creds, S3_INTERNAL, keyPrefix, CONNECT_STR, endpoint, containerName);

      LOG.info("...verify the files are no longer on the source s3...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(() -> assertThat(s3List(s3Client, S3_INTERNAL, keyPrefix)).isEmpty());

      LOG.info("...verify the move happened correctly...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(
              () ->
                  assertThat(blobList(CONNECT_STR, endpoint, containerName).stream().count())
                      .isGreaterThanOrEqualTo(numFiles));

      LOG.info("...cleanup...");
      blobDeleteAll(CONNECT_STR, endpoint, containerName);
    }
  }
}
