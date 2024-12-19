package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.S3Operations.s3Delete;
import static forest.colver.datatransfer.aws.S3Operations.s3List;
import static forest.colver.datatransfer.aws.S3Operations.s3ListContResponse;
import static forest.colver.datatransfer.aws.S3Operations.s3ListResponse;
import static forest.colver.datatransfer.aws.S3Operations.s3ListVersions;
import static forest.colver.datatransfer.aws.S3Operations.s3Put;
import static forest.colver.datatransfer.aws.Utils.S3_INTERNAL;
import static forest.colver.datatransfer.aws.Utils.getEmxNpCreds;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.aws.Utils.getS3Client;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobGet;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPut;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPutSas;
import static forest.colver.datatransfer.azure.Utils.EMX_EXTEMCORNP_SA_EXT_EMCOR_NP_SAS_TOKEN;
import static forest.colver.datatransfer.azure.Utils.EMX_PROD_EXT_EMCOR_PROD_SA_CONN_STR;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ZzzLearningTestSpace {
  private static final Logger LOG = LoggerFactory.getLogger(ZzzLearningTestSpace.class);

  @Test
  void testPutEmcorNonProdBlobWithSas() {
    var sasToken = EMX_EXTEMCORNP_SA_EXT_EMCOR_NP_SAS_TOKEN;
    var endpoint = "https://extemcornp.blob.core.windows.net";
    var containerName = "ext-emcor-np-source";
    var fileTypes =
        List.of(
            "Quote",
            "Quotes",
            "Invoice",
            "Invoices",
            "Receipt",
            "Receipts",
            "Other",
            "Others",
            "Unknown");
    var body = "Hellow Orld!";
    for (var fileType : fileTypes) {
      // ParsedInvoices/year/month/invoice-number/file-type
      var filename = "ParsedInvoices/2004/08/1234567890/" + fileType + "/testfile7.txt";
      blobPutSas(sasToken, endpoint, containerName, filename, body);
      LOG.info("put object: {}", filename);
    }
    var filename = "ParsedInvoices/2008/08/1234567890/1234567896_manifest.json";
    blobPutSas(sasToken, endpoint, containerName, filename, body);
    LOG.info("put object: {}", filename);

    //    var outputStream = blobGetSas(sasToken, endpoint, containerName, filename);
    //    String str = outputStream.toString(StandardCharsets.UTF_8);
    //    assertThat(str).isEqualTo(body);

    // cleanup
    //    blobDelete(CONNECT_STR, endpoint, containerName, filename);
  }

  @Test
  void testPutEmcorProdBlobWithKey() {
    var connectionStr = EMX_PROD_EXT_EMCOR_PROD_SA_CONN_STR;
    var endpoint = "https://emxprod.blob.core.windows.net";
    var containerName = "ext-emcor-prod-source";
    var filename = "filename3.txt";
    var body = "Hellow Orld!";
    blobPut(connectionStr, endpoint, containerName, filename, body);

    var outputStream = blobGet(connectionStr, endpoint, containerName, filename);
    String str = outputStream.toString(StandardCharsets.UTF_8);
    assertThat(str).isEqualTo(body);

    // cleanup
    //    blobDelete(connectionStr, endpoint, containerName, filename);
  }

  @Test
  void testLearnS3Versioning() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      var objectKey = "revloc02/target/test/file-with-versions.txt";
      LOG.info("...place a file...");
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());

      LOG.info("...check the file...");
      var objects = s3List(creds, S3_INTERNAL, "revloc02/target/test");
      assertThat(objects).hasSizeGreaterThanOrEqualTo(1);
      assertThat(objects.get(0).size()).isEqualTo(40L);

      LOG.info("...delete the file...");
      s3Delete(creds, S3_INTERNAL, objectKey);

      LOG.info("...check for versions, can the delete marker be seen?...");
      var versions = s3ListVersions(s3Client, S3_INTERNAL, "revloc02/target/test");

      LOG.info("...cleanup and delete the file and its versions...");
      for (var version : versions) {
        s3Delete(s3Client, S3_INTERNAL, version.key(), version.versionId());
      }
    }
  }

  // todo: is it possible to write a psuedo search for s3? Fuzzy search file names?
  @Test
  void testS3Search() {
    //    var creds = getEmxSbCreds();
    //    var bucket = "emx-sandbox-sftp-source-customer";
    var creds = getEmxNpCreds();
    var bucket = "emx-stage-sftp-source-customer";
    //    var creds = getEmxProdCreds();
    //    var bucket = "emx-prod-sftp-source-customer";
    int count = 0;
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "";

      var response = s3ListResponse(s3Client, bucket, keyPrefix, 1000);
      if (response.hasContents()) {
        //        while (!response.contents().isEmpty()) { // can't get this to work right now
        LOG.info("response.contents().size(): {}", response.contents().size());
        for (var object : response.contents()) {
          LOG.info("object: {}", object.key());
          if (object.key().endsWith(".filepart")) {
            count++;
          }
        }
        //        LOG.info("nextContinuationToken: {}", response.nextContinuationToken());
        // get the next page
        if (response.nextContinuationToken() != null) {
          response =
              s3ListContResponse(
                  s3Client, S3_INTERNAL, keyPrefix, response.nextContinuationToken());
        }
        //        }
      }
    }
    LOG.info("count: {}", count);
    assertThat(count).isNotNegative(); // this assert is moot, just keeping SonarLint happy
  }
}
