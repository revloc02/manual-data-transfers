package forest.colver.datatransfer.azure;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Azure Blob Storage Operations. This class contains methods to interact with Azure Blob Storage.
 * SAS tokens are used for authentication to an individual container within the storage account. A
 * connection string is to authenticate to the whole storage account.
 */
public class BlobStorageOperations {
  private static final Logger LOG = LoggerFactory.getLogger(BlobStorageOperations.class);

  private BlobStorageOperations() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

  public static void blobPut(
      String connectStr, String endpoint, String containerName, String filename, String contents) {
    BlobServiceClient blobServiceClient =
        new BlobServiceClientBuilder()
            .connectionString(connectStr)
            .endpoint(endpoint)
            .buildClient();

    try (var dataStream = new ByteArrayInputStream(contents.getBytes(StandardCharsets.UTF_8))) {
      var blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
      blobContainerClient
          .getBlobClient(filename)
          .getBlockBlobClient()
          .upload(dataStream, contents.length());
    } catch (IOException e) {
      LOG.error("An error occurred while uploading the blob: {}", e.getMessage(), e);
    }
  }

  public static void blobPutSas(
      String sasToken, String endpoint, String containerName, String filename, String contents) {
    BlobServiceClient blobServiceClient =
        new BlobServiceClientBuilder().sasToken(sasToken).endpoint(endpoint).buildClient();

    try (var dataStream = new ByteArrayInputStream(contents.getBytes(StandardCharsets.UTF_8))) {
      var blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
      blobContainerClient
          .getBlobClient(filename)
          .getBlockBlobClient()
          .upload(dataStream, contents.length());
    } catch (IOException e) {
      LOG.error("An error occurred while uploading the blob: {}", e.getMessage(), e);
    }
  }

  public static ByteArrayOutputStream blobGet(
      String connectStr, String endpoint, String containerName, String filename) {
    BlobServiceClient blobServiceClient =
        new BlobServiceClientBuilder()
            .connectionString(connectStr)
            .endpoint(endpoint)
            .buildClient();

    ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
    try (dataStream) {
      var blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
      blobContainerClient.getBlobClient(filename).getBlockBlobClient().downloadStream(dataStream);
    } catch (IOException e) {
      LOG.error("An error occurred while retrieving the blob: {}", e.getMessage(), e);
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
  public static ByteArrayOutputStream blobGetSas(
      String sasToken, String endpoint, String containerName, String filename) {
    BlobServiceClient blobServiceClient =
        new BlobServiceClientBuilder().sasToken(sasToken).endpoint(endpoint).buildClient();

    ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
    try (dataStream) {
      var blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
      blobContainerClient.getBlobClient(filename).getBlockBlobClient().downloadStream(dataStream);
    } catch (IOException e) {
      LOG.error("An error occurred while retrieving the blob: {}", e.getMessage(), e);
    }
    LOG.info("BLOBGETSAS: The object {} was retrieved from {}.", filename, containerName);
    return dataStream;
  }

  public static void blobDelete(
      String connectStr, String endpoint, String containerName, String filename) {
    BlobServiceClient blobServiceClient =
        new BlobServiceClientBuilder()
            .connectionString(connectStr)
            .endpoint(endpoint)
            .buildClient();

    var blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
    blobContainerClient.getBlobClient(filename).delete();
    LOG.info("BLOBDELETE: The object {} was deleted from {}.", filename, containerName);
  }

  public static void blobDeleteSas(
      String sasToken, String endpoint, String containerName, String filename) {
    BlobServiceClient blobServiceClient =
        new BlobServiceClientBuilder().sasToken(sasToken).endpoint(endpoint).buildClient();

    var blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
    blobContainerClient.getBlobClient(filename).delete();
  }

  public static void blobDeleteAll(String connectStr, String endpoint, String containerName) {
    var list = blobList(connectStr, endpoint, containerName);
    for (var blobItem : list) {
      blobDelete(connectStr, endpoint, containerName, blobItem.getName());
    }
  }

  /**
   * Lists all the blobs in a container.
   *
   * @param sasToken Shared Access Signature Token for auth that is container specific.
   * @param endpoint Azure storage account endpoint.
   * @param containerName Blob storage container name.
   * @return A list of BlobItems.
   */
  public static PagedIterable<BlobItem> blobListSas(
      String sasToken, String endpoint, String containerName) {
    BlobServiceClient blobServiceClient =
        new BlobServiceClientBuilder().sasToken(sasToken).endpoint(endpoint).buildClient();
    var blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
    return blobContainerClient.listBlobs();
  }

  /**
   * Lists all the blobs in a container.
   *
   * @param connectStr Azure connection string that for the whole storage account.
   * @param endpoint Azure storage account endpoint.
   * @param containerName Blob storage container name.
   * @return A list of BlobItems.
   */
  public static PagedIterable<BlobItem> blobList(
      String connectStr, String endpoint, String containerName) {
    BlobServiceClient blobServiceClient =
        new BlobServiceClientBuilder()
            .connectionString(connectStr)
            .endpoint(endpoint)
            .buildClient();
    var blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
    return blobContainerClient.listBlobs();
  }

  public static void blobCopy(
      String sasTokenSource,
      String endpoint,
      String containerNameSource,
      String filename,
      String sasTokenTarget,
      String containerNameTarget) {
    var outputStream = blobGetSas(sasTokenSource, endpoint, containerNameSource, filename);
    var contents = outputStream.toString(StandardCharsets.UTF_8);
    blobPutSas(sasTokenTarget, endpoint, containerNameTarget, filename, contents);
  }

  public static void blobMove(
      String sasTokenSource,
      String endpoint,
      String containerNameSource,
      String filename,
      String sasTokenTarget,
      String containerNameTarget) {
    blobCopy(
        sasTokenSource,
        endpoint,
        containerNameSource,
        filename,
        sasTokenTarget,
        containerNameTarget);
    blobDeleteSas(sasTokenSource, endpoint, containerNameSource, filename);
  }
}
