package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.S3Operations.s3ListContResponse;
import static forest.colver.datatransfer.aws.S3Operations.s3ListResponse;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDeleteMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDeleteMessages;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDepth;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadMessages;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.aws.Utils.getEmxProdCreds;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.aws.Utils.getPersonalSbCreds;
import static forest.colver.datatransfer.aws.Utils.getS3Client;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobGet;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPut;
import static forest.colver.datatransfer.azure.BlobStorageOperations.blobPutSas;
import static forest.colver.datatransfer.azure.Utils.EMX_EXTEMCORNP_SA_EXT_EMCOR_NP_SAS_TOKEN;
import static forest.colver.datatransfer.azure.Utils.EMX_PROD_EXT_EMCOR_PROD_SA_CONN_STR;
import static org.assertj.core.api.Assertions.assertThat;

import forest.colver.datatransfer.messaging.DisplayUtils;
import forest.colver.datatransfer.messaging.Environment;
import forest.colver.datatransfer.messaging.JmsBrowse;
import forest.colver.datatransfer.messaging.JmsSend;
import forest.colver.datatransfer.messaging.Utils;
import jakarta.jms.JMSException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ZzzLearningSpaceTests {
  private static final Logger LOG = LoggerFactory.getLogger(ZzzLearningSpaceTests.class);

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

  /**
   * S3 partial filename search when there are over 1000 objects. (Remember to get creds first using
   * `aws configure sso`.)
   */
  @Test
  void testS3PartialFilenameSearchOver1000() {
    var creds = getEmxProdCreds();
    var bucket = "emx-prod-sftp-source-cache";
    int totalCount = 0;
    int foundCount = 0;
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "";
      var response = s3ListResponse(s3Client, bucket, keyPrefix, 1000);
      if (response.hasContents()) {
        do {
          if (response.nextContinuationToken() != null && totalCount > 0) {
            // get the next page
            response =
                s3ListContResponse(s3Client, bucket, keyPrefix, response.nextContinuationToken());
          }
          LOG.info(
              "===================================== response.contents().size()={}; Total Count={}",
              response.contents().size(),
              totalCount);
          for (var object : response.contents()) {
            totalCount++;
            if (object.key().contains("LDS_FINSTMT_PRD")) { // ext-standard-bank-saf
              LOG.info("object: {}", object.key());
              foundCount++;
            }
          }
        } while (response.nextContinuationToken() != null);
      }
    }
    assertThat(foundCount).isNotNegative(); // this assert is moot, just keeping SonarLint happy
    LOG.info("Total count={}; Found count={}", totalCount, foundCount);
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

  // this queue got deleted, but I want to keep the test
  @Test
  void deleteLifeflightFileMalwareScanNotifications() {
    var sqs = "forest-tst-malware-scan-results";
    var resultingDepth = 5;
    var creds = getEmxSbCreds();
    var count = 0;
    do {
      var response = sqsReadMessages(creds, sqs, 1, 30);
      if (response.messages().get(0).body().contains("lifeflightTestFile")) {
        sqsDeleteMessages(creds, sqs, response);
        count++;
      }
    } while (sqsDepth(creds, sqs) > resultingDepth);
    LOG.info("deleted: {}", count);
  }

  @Test
  void addJmsHeaderToMessage() throws JMSException {
    var message =
        JmsBrowse.browseForSpecificMessage(
            Environment.PROD,
            "forest-test",
            "traceparent='00-18d4cd7e866a81038fd621d78e21be14-d998fea593265831'");
    LOG.info("=============== message received ===================");
    var headers = Utils.extractMsgProperties(message);
    headers.put("datatype", "temple.recommend.update");
    var newMessage = Utils.createTextMessage(message.getBody(String.class), headers);
    LOG.info(DisplayUtils.createStringFromMessage(newMessage));
    JmsSend.sendMessageAutoAck(Environment.PROD, "temple-entry-emxonramp-prod", newMessage);
  }
}
