package forest.colver.datatransfer.azure;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobStorageOperations {
  private static final Logger LOG = LoggerFactory.getLogger(BlobStorageOperations.class);

  private BlobStorageOperations() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

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

  public static void blobPutSas(String sasToken, String endpoint, String containerName,
      String filename, String contents) {
    BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
        .sasToken(sasToken)
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
      String containerName, String filename) {
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
    LOG.info("BLOBGET: The object {} was retrieved from {}.", filename, containerName);
    return dataStream;
  }

  /**
   * Authenticates with a Sas Token, and reads an object from the Storage Container.
   *
   * @param sasToken Shared Access Signature Token for auth.
   * @param endpoint Azure storage endpoint.
   * @param containerName Blob storage name.
   * @param filename Filename.
   * @return An output stream of the file.
   */
  public static ByteArrayOutputStream blobGetSas(String sasToken, String endpoint,
      String containerName,
      String filename) {
    BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
        .sasToken(sasToken)
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
    LOG.info("BLOBDELETE: The object {} was deleted from {}.", filename, containerName);
  }

  public static void blobDeleteSas(String sasToken, String endpoint, String containerName,
      String filename) {
    BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
        .sasToken(sasToken)
        .endpoint(endpoint)
        .buildClient();

    var blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
    blobContainerClient.getBlobClient(filename).delete();
  }

  // todo: can I make move and copy methods between 2 blob containers?
  // todo: can I research copying from one blob container to another without using Java?
}
