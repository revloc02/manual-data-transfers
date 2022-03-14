package forest.colver.datatransfer.azure;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BlobStorageOperations {

  public static void blobPut(String connectStr, String endpoint, String containerName,
      String filename, String contents) {
    BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
        .connectionString(connectStr)
        .endpoint(endpoint)
        .buildClient();

    try (var dataStream = new ByteArrayInputStream(contents.getBytes(StandardCharsets.UTF_8))) {
      var blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
      blobContainerClient.getBlobClient(filename).getBlockBlobClient()
          .upload(dataStream, contents.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static ByteArrayOutputStream blobGet(String connectStr, String endpoint,
      String containerName,
      String filename) {
    BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
        .connectionString(connectStr)
        .endpoint(endpoint)
        .buildClient();

    ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
    try (dataStream) {
      var blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
      blobContainerClient.getBlobClient(filename).getBlockBlobClient().downloadStream(dataStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return dataStream;
  }

  public static void blobDelete(String connectStr, String endpoint, String containerName,
      String filename) {
    BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
        .connectionString(connectStr)
        .endpoint(endpoint)
        .buildClient();

    var blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
    blobContainerClient.getBlobClient(filename).delete();
  }

}
