package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.BlobStorageOperations.blobDelete;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobGet;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPut;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_STORAGE_ACCOUNT_CONNECTION_STRING;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Tests for Azure Blob Storage, aka Azure Storage Containers. Use 'az login' to login, these tests
 * are connecting to the EMX Enterprise Sandbox account.
 */
public class AzureBlobStorageTests {

  public static final String CONNECT_STR = EMX_SANDBOX_STORAGE_ACCOUNT_CONNECTION_STRING;

  @Test
  public void testPut() {
    var endpoint = "https://foresttestsa.blob.core.windows.net";
    var containerName = "forest-test-blob";
    var filename = "filename.txt";
    var body = "Hellow Orld!";
    blobPut(CONNECT_STR, endpoint, containerName, filename, body);

    var outputStream = blobGet(CONNECT_STR, endpoint, containerName, filename);
    String str = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
    assertThat(str).isEqualTo(body);

    // cleanup
    blobDelete(CONNECT_STR, endpoint, containerName, filename);
  }

}
