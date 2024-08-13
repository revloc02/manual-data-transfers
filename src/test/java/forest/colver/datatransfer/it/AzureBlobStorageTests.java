package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.BlobStorageOperations.blobCopy;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobDelete;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobDeleteSas;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobGet;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobGetSas;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPut;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPutSas;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_SA_FOREST_CONN_STR;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_SA_FOREST_FOREST_TEST_BLOB2_SAS;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_SA_FOREST_FOREST_TEST_BLOB_SAS;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Tests for Azure Blob Storage, aka Azure Storage Containers. Use 'az login' to login, these tests
 * are connecting to the EMX Enterprise Sandbox account.
 */
public class AzureBlobStorageTests {

  public static final String CONNECT_STR = EMX_SANDBOX_SA_FOREST_CONN_STR;
  public static final String SAS_TOKEN = EMX_SANDBOX_SA_FOREST_FOREST_TEST_BLOB_SAS;
  public static final String SAS_TOKEN2 = EMX_SANDBOX_SA_FOREST_FOREST_TEST_BLOB2_SAS;

  @Test
  public void testPut() {
    var endpoint = "https://foresttestsa.blob.core.windows.net";
    var containerName = "forest-test-blob";
    var filename = "filename.txt";
    var body = "Hellow Orld!";
    blobPut(CONNECT_STR, endpoint, containerName, filename, body);

    var outputStream = blobGet(CONNECT_STR, endpoint, containerName, filename);
    String str = outputStream.toString(StandardCharsets.UTF_8);
    assertThat(str).isEqualTo(body);

    // cleanup
    blobDelete(CONNECT_STR, endpoint, containerName, filename);
  }

  /**
   * Tests the Blob Put, Get, and Delete methods using an SAS Token to auth. How to get an Azure
   * Blob SAS Token: 1) Azure Portal > Storage Accounts > Container > Shared access tokens, under
   * Setting on the left menu; 2) Signing key = Key 1 (is fine); 3) Select Permissions you would
   * like; 4) Set an Expiry date some years into the future; 5) all other defaults are fine; 6)
   * Click the button Generate SAS token and URL; 7) You only get to see this SAS Token data once,
   * so copy it where you need it to go.
   */
  @Test
  public void testPutSas() {
    var endpoint = "https://foresttestsa.blob.core.windows.net";
    var containerName = "forest-test-blob";
    var filename = "filename.txt";
    var body = "Hellow Orld!";
    blobPutSas(SAS_TOKEN, endpoint, containerName, filename, body);

    var outputStream = blobGetSas(SAS_TOKEN, endpoint, containerName, filename);
    String str = outputStream.toString(StandardCharsets.UTF_8);
    assertThat(str).isEqualTo(body);

    // cleanup
    blobDeleteSas(SAS_TOKEN, endpoint, containerName, filename);
  }

  @Test
  public void testBlobCopy() {
    var endpoint = "https://foresttestsa.blob.core.windows.net";
    var containerNameSource = "forest-test-blob";
    var filename = "filename.txt";
    var body = "Hellow Orld!";
    blobPutSas(SAS_TOKEN, endpoint, containerNameSource, filename, body);
    // todo: should I have an awaitility that checks for its arrival?

    var containerNameTarget = "forest-test-blob2";
    blobCopy(SAS_TOKEN, endpoint, containerNameSource, filename, SAS_TOKEN2, containerNameTarget);

    // cleanup
    blobDeleteSas(SAS_TOKEN, endpoint, containerNameSource, filename);
    blobDeleteSas(SAS_TOKEN2, endpoint, containerNameTarget, filename);
  }
}
