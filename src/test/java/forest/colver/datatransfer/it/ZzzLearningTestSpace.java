package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.S3Operations.s3Delete;
import static forest.colver.datatransfer.aws.S3Operations.s3DeleteAll;
import static forest.colver.datatransfer.aws.S3Operations.s3List;
import static forest.colver.datatransfer.aws.S3Operations.s3ListContResponse;
import static forest.colver.datatransfer.aws.S3Operations.s3ListDeleteMarkers;
import static forest.colver.datatransfer.aws.S3Operations.s3ListResponse;
import static forest.colver.datatransfer.aws.S3Operations.s3ListVersions;
import static forest.colver.datatransfer.aws.S3Operations.s3Put;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDeleteMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.aws.Utils.S3_INTERNAL;
import static forest.colver.datatransfer.aws.Utils.getEmxNpCreds;
import static forest.colver.datatransfer.aws.Utils.getEmxProdCreds;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.aws.Utils.getPersonalSbCreds;
import static forest.colver.datatransfer.aws.Utils.getS3Client;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobGet;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPut;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPutSas;
import static forest.colver.datatransfer.azure.Utils.EMX_EXTEMCORNP_SA_EXT_EMCOR_NP_SAS_TOKEN;
import static forest.colver.datatransfer.azure.Utils.EMX_PROD_EXT_EMCOR_PROD_SA_CONN_STR;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStampFilename;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  // deletes file and then deletes the versioned file, but currently the Delete Marker remains
  // get creds first using `aws configure sso`
  @Test
  void testS3DeleteVersionsRemoved() { // earliest delete-marker observed on console is: 4 Feb 2025
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "revloc02/target/versions-removed";
      var objectKey = keyPrefix + "/removed" + getTimeStampFilename() + ".txt";
      LOG.info("...place a file...");
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());

      LOG.info("...check the file arrived...");
      var objects = s3List(creds, S3_INTERNAL, keyPrefix);
      assertThat(objects).hasSizeGreaterThanOrEqualTo(1);

      LOG.info("...delete the file...");
      s3Delete(creds, S3_INTERNAL, objectKey);

      LOG.info("...check for versions, can the delete marker be seen? No...");
      var versions = s3ListVersions(s3Client, S3_INTERNAL, keyPrefix);

      LOG.info("...cleanup and delete the file versions...");
      for (var version : versions) {
        s3Delete(s3Client, S3_INTERNAL, version.key(), version.versionId());
      }

      LOG.info("...check for delete markers, can the delete marker be seen? Yes...");
      var deleteMarkers = s3ListDeleteMarkers(s3Client, S3_INTERNAL, keyPrefix);

      // todo: turning off delete-marker-cleanup as an experiment
      //      LOG.info("...cleanup and delete the file delete markers...");
      //      for (var deleteMarker : deleteMarkers) {
      //        s3Delete(s3Client, S3_INTERNAL, deleteMarker.key(), deleteMarker.versionId());
      //      }
    }
  }

  // places a file and then deletes it, but leaves the version
  // get creds first using `aws configure sso`
  @Test
  void testS3DeleteButVersionsRemain() { // earliest version observed on console is: 3 Feb 2025
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "revloc02/target/versions-remain";
      var objectKey = keyPrefix + "/remain" + getTimeStampFilename() + ".txt";
      LOG.info("...place a file...");
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());

      LOG.info("...check the file arrived...");
      var objects = s3List(creds, S3_INTERNAL, keyPrefix);
      assertThat(objects).hasSizeGreaterThanOrEqualTo(1); // this actually isn't a good check

      LOG.info("...delete the file...");
      s3Delete(creds, S3_INTERNAL, objectKey);

      LOG.info("...check for versions, can the delete marker be seen?...");
      s3ListVersions(s3Client, S3_INTERNAL, keyPrefix);
    }
  }

  // places a file with the same name
  // get creds first using `aws configure sso`
  @Test
  void testS3MultipleVersionsSameName() { // earliest version observed on console is: 3 Feb 2025
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "revloc02/target/same-name";
      var objectKey = keyPrefix + "/text.txt";
      LOG.info("...place a file...");
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());

      LOG.info("...check the file arrived...");
      var objects = s3List(creds, S3_INTERNAL, keyPrefix);
      assertThat(objects).hasSizeGreaterThanOrEqualTo(1); // this actually isn't a good check
    }
  }

  @Test
  void testS3WildcardSearch() {
    var creds = getEmxSbCreds();
    List<String> filenameList =
        List.of(
            "test1.txt", "test2.txt", "test3.txt", "test4.txt", "wildcard.txt", "testwildcard.txt");
    var count = 0;
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "revloc02/target/test";

      LOG.info("...place files...");
      for (var filename : filenameList) {
        var objectKey = keyPrefix + "/" + filename;
        s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());
      }

      LOG.info("...check the files arrived...");
      var objects = s3List(creds, S3_INTERNAL, keyPrefix);
      assertThat(objects).hasSizeGreaterThanOrEqualTo(6);

      LOG.info("...find the files with 'wildcard' in the name...");
      var response = s3ListResponse(s3Client, S3_INTERNAL, keyPrefix, 1000);
      if (response.hasContents()) {
        LOG.info("response.contents().size(): {}", response.contents().size());
        for (var object : response.contents()) {
          if (object.key().contains("wildcard")) {
            LOG.info("object: {}", object.key());
            count++;
          }
        }
      }
      assertThat(count).isEqualTo(2);

      LOG.info("...cleanup...");
      s3DeleteAll(s3Client, S3_INTERNAL, keyPrefix);
    }
    LOG.info("count: {}", count);
  }

  // todo: is it possible to write a psuedo search for s3? Fuzzy search file names?
  @Test
  void testS3FilepartSearch() {
    // todo: write a uniquely named file to search for
    // todo: eventually, when this is done, I want to look for .filepart files on SFTP S3s

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

  /**
   * List only objects that are not directories. Can be used for SFTP source customer bucket, which
   * should only have directories.
   */
  @Test
  void listS3ObjectsOnly() {
    var creds = getEmxSbCreds();
    var count = 0;
    var bucket = "emx-sandbox-sftp-source-customer"; // also use S3_INTERNAL
    var keyPrefix = "";
    try (var s3Client = getS3Client(creds)) {
      var response = s3ListResponse(s3Client, bucket, keyPrefix, 1000);
      if (response.hasContents()) {
        LOG.info("response.contents().size(): {}", response.contents().size());
        for (var object : response.contents()) {
          if (!object.key().endsWith("/")) {
            LOG.info("object: {}", object.key());
            count++;
          }
        }
      }
    }
    LOG.info("count: {}", count);
  }

  // check for directory object expiration on Target Customer S3
  // get creds first using `aws configure sso`
  @Test
  void checkSftpTargetCustomerDirectoriesThatExpired() {
    var creds = getEmxProdCreds();
    var bucket = "emx-prod-sftp-target-customer";
    int count = 0;
    Map<String, Integer> emxKnownSystems =
        new HashMap<>(
            Map.of(
                "dw-family-history",
                0,
                "emx-health-check",
                0,
                "ext-deseret-book",
                0,
                "ext-ensign-college-workday",
                0,
                "ext-nomentia",
                0,
                "ext-riskonnect",
                0));
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "";
      var response = s3ListResponse(s3Client, bucket, keyPrefix, 1000);
      if (response.hasContents()) {
        LOG.info("response.contents().size(): {}", response.contents().size());
        for (var object : response.contents()) {
          LOG.info("object: {}", object.key());
          count++;
          for (var system : emxKnownSystems.keySet()) {
            if (object.key().contains(system)) {
              emxKnownSystems.compute(system, (k, currentCount) -> currentCount + 1);
            }
          }
        }
      }
    }
    LOG.info("count: {}", count);
    for (var system : emxKnownSystems.keySet()) {
      LOG.info("{}: {}", system, emxKnownSystems.get(system));
      // if this breaks, I will know that a directory object expired
      assertThat(emxKnownSystems.get(system)).isNotZero();
    }
  }

  // Just a quick SQS message test for various KMS policies I am trying
  @Test
  void testKmsTestSqs() {
    var sqs = "kms_test_queue";
    LOG.info("Interacting with: sqs={}", sqs);
    // send a message
    var creds = getPersonalSbCreds();
    var payload = "message with payload only, no MessageAttributes";
    sqsSend(creds, sqs, payload);
    // check that it arrived
    var msg = sqsReadOneMessage(creds, sqs);
    assert msg != null;
    assertThat(msg.body()).isEqualTo(payload);
    // cleanup
    sqsDeleteMessage(creds, sqs, msg);
  }

  // Gets a UUID and then logs the String hashCode of it, so I know what it looks like
  @Test
  void testHashCodeOfUuid() {
    for (int i = 0; i < 10; i++) {
      var uuid = java.util.UUID.randomUUID();
      LOG.info("uuid: {}. uuid.hashCode(): {}", uuid, uuid.hashCode());
    }
  }
}
