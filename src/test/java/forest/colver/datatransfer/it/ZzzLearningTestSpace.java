package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.BlobStorageOperations.blobGet;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobGetSas;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPut;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPutSas;
import static forest.colver.datatransfer.azure.Utils.EMX_PROD_EMXPROD_EXT_EMCOR_SAS_TOKEN;
import static forest.colver.datatransfer.azure.Utils.EMX_PROD_EMXPROD_STORAGE_ACCOUNT_CONNECTION_STRING;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZzzLearningTestSpace {
  private static final Logger LOG = LoggerFactory.getLogger(ZzzLearningTestSpace.class);
  @Test
  public void testPutEmcorProdBlobWithSas() {
    var sasToken = EMX_PROD_EMXPROD_EXT_EMCOR_SAS_TOKEN;
    var endpoint = "https://emxprod.blob.core.windows.net";
    var containerName = "ext-emcor-prod-source";
//    var filename = "Parsed/2023/August/1234567890/Receipt/filename3.txt";
    var filename = "filename6.txt";
    var body = "Hellow Orld!";
    blobPutSas(sasToken, endpoint, containerName, filename, body);

    var outputStream = blobGetSas(sasToken, endpoint, containerName, filename);
    String str = outputStream.toString(StandardCharsets.UTF_8);
    assertThat(str).isEqualTo(body);

    // cleanup
//    blobDelete(CONNECT_STR, endpoint, containerName, filename);
  }

  @Test
  public void testPutEmcorProdBlobWithKey() {
    var connectionStr = EMX_PROD_EMXPROD_STORAGE_ACCOUNT_CONNECTION_STRING;
    var endpoint = "https://emxprod.blob.core.windows.net";
    var containerName = "ext-emcor-prod-source";
    var filename = "filename5.txt";
    var body = "Hellow Orld!";
    blobPut(connectionStr, endpoint, containerName, filename, body);

    var outputStream = blobGet(connectionStr, endpoint, containerName, filename);
    String str = outputStream.toString(StandardCharsets.UTF_8);
    assertThat(str).isEqualTo(body);

    // cleanup
//    blobDelete(connectionStr, endpoint, containerName, filename);
  }
}
