package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.S3Operations.s3List;
import static forest.colver.datatransfer.aws.S3Operations.s3Put;
import static forest.colver.datatransfer.aws.Utils.S3_INTERNAL;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobDelete;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobGet;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_SA_FOREST_CONN_STR;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.hybrid.S3AndBlobStorage.moveS3toAzureBlob;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HybridS3AndAzureBlobIntTests {
  private static final Logger LOG = LoggerFactory.getLogger(HybridS3AndAzureBlobIntTests.class);
  public static final String CONNECT_STR = EMX_SANDBOX_SA_FOREST_CONN_STR;

  @Test
  void testMoveS3toAzureBlob(){
    // put a file
    var objectKey = "revloc02/source/test/test.txt";
    var creds = getEmxSbCreds();
    var payload = getDefaultPayload();
    s3Put(creds, S3_INTERNAL, objectKey, payload);

    // verify the file is there
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isOne();
    assertThat(objects.get(0).key()).isEqualTo(objectKey);

    // move
    var endpoint = "https://foresttestsa.blob.core.windows.net";
    var containerName = "forest-test-blob";
    moveS3toAzureBlob(creds, S3_INTERNAL, objectKey, CONNECT_STR, endpoint, containerName);

    // verify the move happened
    var outputStream = blobGet(CONNECT_STR, endpoint, containerName, objectKey);
    String str = outputStream.toString(StandardCharsets.UTF_8);
    assertThat(str).isEqualTo(payload);

    // cleanup
    blobDelete(CONNECT_STR, endpoint, containerName, objectKey);
  }
}
