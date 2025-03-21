package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.BlobStorageOperations.blobCopy;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobDelete;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobDeleteSas;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobGet;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobGetSas;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobListSas;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobMove;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPut;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPutSas;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_SA_CONN_STR;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_SA_FOREST_TEST_BLOB2_SAS;
import static forest.colver.datatransfer.azure.Utils.EMX_SANDBOX_SA_FOREST_TEST_BLOB_SAS;
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
  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStorageTests.class);
  public static final String ENDPOINT = "https://emxsandbox.blob.core.windows.net";
  public static final String CONTAINER_NAME = "forest-test-blob";
  public static final String FILENAME = "filename.txt";
  public static final String BODY = "Hellow Orld!";
  public static final String CONNECT_STR = EMX_SANDBOX_SA_CONN_STR;
  public static final String SAS_TOKEN = EMX_SANDBOX_SA_FOREST_TEST_BLOB_SAS;
  public static final String SAS_TOKEN2 = EMX_SANDBOX_SA_FOREST_TEST_BLOB2_SAS;

  // Note to self: In the Azure Portal, I manually added my IP address to the Security+Networking
  // Firewall settings to get this test to work. I decided to leave this a manual step so that I
  // don't have to frequently update the emx-tf-app-azure/terraform/sandbox/main.tf file in the team
  // GitHub repo code.
  @Test
  void testPut() {
    LOG.info("...place a file...");
    blobPut(CONNECT_STR, ENDPOINT, CONTAINER_NAME, FILENAME, BODY);

    LOG.info("...get the file and check it...");
    var outputStream = blobGet(CONNECT_STR, ENDPOINT, CONTAINER_NAME, FILENAME);
    String str = outputStream.toString(StandardCharsets.UTF_8);
    assertThat(str).isEqualTo(BODY);

    LOG.info("...cleanup...");
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
  void testPutSas() {
    LOG.info("...place a file...");
    blobPutSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME, BODY);

    LOG.info("...get the file and check it...");
    var outputStream = blobGetSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME);
    String str = outputStream.toString(StandardCharsets.UTF_8);
    assertThat(str).isEqualTo(BODY);

    LOG.info("...cleanup...");
    blobDeleteSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME);
  }

  @Test
  void testBlobList() {
    LOG.info("...place a file...");
    blobPutSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME, BODY);

    LOG.info("...get a list of files and assert it is the right length...");
    var list = blobListSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME);
    assertThat(list.stream().count()).isOne();

    LOG.info("...cleanup...");
    blobDeleteSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME);
  }

  @Test
  void testBlobListMoreThanOne() {
    LOG.info("...place several files...");
    blobPutSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, "file1.txt", BODY);
    blobPutSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, "file2.txt", BODY);
    blobPutSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, "file3.txt", BODY);

    LOG.info("...get a list of files and assert it is the right length...");
    var list = blobListSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME);
    assertThat(list.stream().count()).isEqualTo(3);

    LOG.info("...cleanup...");
    blobDeleteSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, "file1.txt");
    blobDeleteSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, "file2.txt");
    blobDeleteSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, "file3.txt");
  }

  @Test
  void testBlobCopy() {
    LOG.info("...place a file...");
    blobPutSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME, BODY);

    LOG.info("...verify the file arrived...");
    var list = blobListSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME);
    assertThat(list.stream().count()).isOne();

    LOG.info("...copy the file...");
    var containerNameTarget = "forest-test-blob2";
    blobCopy(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME, SAS_TOKEN2, containerNameTarget);

    LOG.info("...verify the file was copied...");
    list = blobListSas(SAS_TOKEN2, ENDPOINT, containerNameTarget);
    assertThat(list.stream().count()).isOne();
    list.forEach(blob -> assertThat(blob.getName()).isEqualTo(FILENAME));

    LOG.info("...verify the original file is still available on the source container...");
    list = blobListSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME);
    assertThat(list.stream().count()).isOne();

    LOG.info("...cleanup...");
    blobDeleteSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME);
    blobDeleteSas(SAS_TOKEN2, ENDPOINT, containerNameTarget, FILENAME);
  }

  @Test
  void testBlobMove() {
    LOG.info("...place a file...");
    blobPutSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME, BODY);

    LOG.info("...verify the file arrived...");
    var list = blobListSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME);
    assertThat(list.stream().count()).isOne();

    LOG.info("...move the file...");
    var containerNameTarget = "forest-test-blob2";
    blobMove(SAS_TOKEN, ENDPOINT, CONTAINER_NAME, FILENAME, SAS_TOKEN2, containerNameTarget);

    LOG.info("...verify the file was moved...");
    list = blobListSas(SAS_TOKEN2, ENDPOINT, containerNameTarget);
    assertThat(list.stream().count()).isOne();
    list.forEach(blob -> assertThat(blob.getName()).isEqualTo(FILENAME));

    LOG.info("...verify the original file is not available on the source container...");
    list = blobListSas(SAS_TOKEN, ENDPOINT, CONTAINER_NAME);
    assertThat(list.stream().count()).isZero();

    LOG.info("...cleanup...");
    blobDeleteSas(SAS_TOKEN2, ENDPOINT, containerNameTarget, FILENAME);
  }
}
