package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.BlobStorageOperations.blobCopy;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobDelete;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobDeleteSas;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobGet;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobGetSas;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobListSas;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPut;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPutSas;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_SA_FOREST_CONN_STR;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_SA_FOREST_FOREST_TEST_BLOB2_SAS;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_SA_FOREST_FOREST_TEST_BLOB_SAS;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for Azure Blob Storage, aka Azure Storage Containers. Use 'az login' to login, these tests
 * are connecting to the EMX Enterprise Sandbox account.
 */
public class AzureBlobStorageTests {

  public static final String ENDPOINT = "https://foresttestsa.blob.core.windows.net";
  public static final String CONTAINER_NAME = "forest-test-blob";
  public static final String FILENAME = "filename.txt";
  public static final String BODY = "Hellow Orld!";
  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStorageTests.class);
  public static final String CONNECT_STR = EMX_SANDBOX_SA_FOREST_CONN_STR;
  public static final String SAS_TOKEN = EMX_SANDBOX_SA_FOREST_FOREST_TEST_BLOB_SAS;
  public static final String SAS_TOKEN2 = EMX_SANDBOX_SA_FOREST_FOREST_TEST_BLOB2_SAS;

  @Test
  public void testPut() {
    blobPut(CONNECT_STR, ENDPOINT, CONTAINER_NAME, FILENAME, BODY);

    var outputStream = blobGet(CONNECT_STR, ENDPOINT, CONTAINER_NAME, FILENAME);
    String str = outputStream.toString(StandardCharsets.UTF_8);
    assertThat(str).isEqualTo(BODY);

    // cleanup
    blobDelete(CONNECT_STR, ENDPOINT, CONTAINER_NAME, FILENAME);
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
    blobPutSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME, BODY);

    var outputStream = blobGetSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME);
    String str = outputStream.toString(StandardCharsets.UTF_8);
    assertThat(str).isEqualTo(BODY);

    // cleanup
    blobDeleteSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME);
  }

  @Test
  public void testBlobList() {
    blobPutSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME, BODY);

    var list = blobListSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME);
    list.forEach(blob -> LOG.info("Name={}", blob.getName()));
    assertThat(list.stream().count()).isOne();

    // cleanup
    blobDeleteSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME);
  }

  @Test
  public void testBlobCopy() {
    blobPutSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME, BODY);
    // todo: should I have an awaitility that checks for its arrival?

    var containerNameTarget = "forest-test-blob2";
    blobCopy(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME, SAS_TOKEN2, containerNameTarget);

    // todo: need an assert that the copy succeeded

    // cleanup
    blobDeleteSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME);
    blobDeleteSas(SAS_TOKEN2, ENDPOINT, containerNameTarget, FILENAME);
  }
}
