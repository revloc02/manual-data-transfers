package forest.colver.datatransfer;

import static forest.colver.datatransfer.aws.S3Operations.s3Delete;
import static forest.colver.datatransfer.aws.S3Operations.s3Get;
import static forest.colver.datatransfer.aws.S3Operations.s3List;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDeleteMessagesWithPayloadLike;
import static forest.colver.datatransfer.aws.Utils.awsResponseValidation;
import static forest.colver.datatransfer.aws.Utils.getEmxNpCreds;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.aws.Utils.getS3Client;
import static forest.colver.datatransfer.config.Utils.writeFile;
import static forest.colver.datatransfer.messaging.Environment.PROD;
import static forest.colver.datatransfer.messaging.JmsBrowse.browseAndCountSpecificMessages;
import static forest.colver.datatransfer.messaging.JmsBrowse.browseForSpecificMessage;
import static forest.colver.datatransfer.messaging.Utils.getJmsMsgPayload;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

/**
 * This defines methods that perform tasks I commonly use in my work or during watchman. They are
 * specific applications of the general messaging tools.
 */
public class CommonTasks {

  private static final Logger LOG = LoggerFactory.getLogger(CommonTasks.class);

  /**
   * Retrieves a message from the Qpid Replay Caches and writes it as a file to the local Downloads
   * directory.
   *
   * @param selector JMS selector for identifying a unique message. Example:
   * "name='gtmbancoindustrialACH20230331123002258.xml'"
   * @param fullyQualifiedPath The path to write the file. Example:
   * "/Users/revloc02/Downloads/gtmbancoindustrialACH20230331123002258.xml"
   */
  public static void retrieveMessageFromQpidReplayCache(String selector,
      String fullyQualifiedPath) {
    // possible selectors:
    //    emxReplayEnvironmentName = prod
    //    datatype             = finance.payment.eft
    //    sourceSystem         = cubs
    //    emxReplayTimestamp   = 1681237803973
    //    traceparent          = 00-ab1cd1673eca0818d440923099cb9123-6a72088af80545d8-01
    //    name                 = gtmbancoindustrialACH20230411123003061.xml
    //    targetSystem         = ext-banco-industrial

    // edit the selector
    var message = browseForSpecificMessage(PROD, "emx-replay-cache", selector);
    // edit the file name you would like
    var payload = getJmsMsgPayload(message);
    writeFile(fullyQualifiedPath, payload.getBytes());
  }

  /**
   * Clears Lifeflight health checks from the Stage sftp-error queue. Occasionally a Lifeflight
   * health check will fail for some random reason and leave an error in the sftp error queue. These
   * errors are typically anomalies and not valuable since the health check clears the next run.
   */
  public static void cleanupSftpErrorSqsStage() {
    // Sandbox. Obviously refresh sandbox ~/.aws/credentials before running this.
    // sqsDeleteMessagesWithPayloadLike(getEmxSbCreds(), "sftp-error", "lifeflightTestFile");

    // Stage. Obviously refresh stage ~/.aws/credentials before running this.
    sqsDeleteMessagesWithPayloadLike(getEmxNpCreds(), "sftp-error", "lifeflightTestFile");
  }

  /**
   * This will clean up messages from the ops queue that are older than a defined threshold. The ops
   * queue is used so the ops team can easily find and retrieve message payloads for remediation.
   * Messages older than a couple of weeks are almost certainly irrelevant, but retaining three
   * months worth should be plenty of time.
   *
   * The impetus for this was when the ops queue went over 100 messages, and the one that I had just
   * copied to there was not visible in the Voyager interface (which the ops team uses), until I
   * deleted messages down to below 100.
   */
  public static void cleanupOpsQueue() {
    var timestamp = "1638297326591"; // messages older than ~ 30Nov2021

    // 1. You need to find a timestamp to use in the "timestamp" var above, get this from the list of messages in the queue. Use whatever selector is helpful to do this.
    browseForSpecificMessage(PROD, "ops",
        "emxTraceOnrampMessageName='CFISLDS-GTM-INDUS-05-ACH-20210817111659.xml'");

    // 2. Once you have set the timestamp var above, check how many messages are going to be deleted according that timestamp
    browseAndCountSpecificMessages(PROD, "ops", "emxTraceSourceTimestamp<=" + timestamp);

    // 3. If those results look good, actually delete the messages (uncomment and run, then re-comment the code)
//    deleteAllSpecificMessages(PROD, "ops", "emxTraceSourceTimestamp<=" + timestamp);
  }

  /**
   * Delete up to 1000 objects more than 2 weeks old from a s3 directory.
   *
   * @param bucket The S3 bucket to work on.
   * @param objectKey The directory on the S3 to work on. E.g. "emx-health-check1/inbound"
   */
  public static void cleanS3Directory(String bucket, String objectKey) {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      var twoWeeksAgo = Instant.now().minus(14, ChronoUnit.DAYS);
      var deleted = 0;
      var skipped = 0;
      var objects = s3List(s3Client, bucket, objectKey, 1000);
      for (var object : objects) {
        if (object.size() > 0) {
          if (object.lastModified().isBefore(twoWeeksAgo)) {
            LOG.info("key={}; lastModified={}", object.key(), object.lastModified());
            s3Delete(s3Client, bucket, object.key());
            deleted++;
          } else {
            skipped++;
            LOG.info("TOO RECENT: {}", object.key());
            var getTags = GetObjectTaggingRequest.builder().bucket(bucket).key(object.key())
                .build();
            var result = s3Client.getObjectTagging(getTags);
            if (result.hasTagSet()) {
              LOG.info("     tags: {}={}", result.tagSet().get(0).key(),
                  result.tagSet().get(0).value());
            }
          }
        }
      }
      LOG.info("deleted={}; skipped={}", deleted, skipped);
    }
  }

  /**
   * Really just a method to explore S3 objects.
   *
   * @param bucket The S3 bucket to work on. E.g. "cp-aws-gayedtiak3nflbiftucz-s3-logging"
   * @param keyPrefix The directory on the S3 to work on. E.g. "emx-sandbox-sftp/"
   */
  public static void examineS3Objects(String bucket, String keyPrefix) {
    var creds = getEmxSbCreds();
    var maxKeys = 1000;
    var oneYearAgo = Instant.now().minus(365, ChronoUnit.DAYS);

    try (var s3Client = getS3Client(creds)) {
      // get the first set so that we have a continuationToken
      var listObjectsRequest =
          ListObjectsV2Request.builder().bucket(bucket).prefix(keyPrefix).maxKeys(maxKeys).build();
      var listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
      awsResponseValidation(listObjectsResponse);
      var count = 0;
      var lifeflight = 0;
      var divvy = 0;
      var trusted = 0;
      var deleted = 0;
      while (listObjectsResponse.isTruncated()) {
        count = count + listObjectsResponse.contents().size();
        for (var object : listObjectsResponse.contents()) {

//          var response = s3Get(s3Client, bucket, object.key());
//          var contents = new String(response.readAllBytes(),
//              StandardCharsets.UTF_8);
//          if (contents.contains("emx-health-check")) {
//            // delete lifeflight logs to make it easier to peruse other logs
//            lifeflight++;
//            s3Delete(s3Client, bucket, object.key());
//          }
//          if (contents.contains("DivvyCloud")) {
//            // delete lifeflight logs to make it easier to peruse other logs
//            divvy++;
//            s3Delete(s3Client, bucket, object.key());
//          }
//          if (contents.contains("TrustedAdvisor")) {
//            // delete lifeflight logs to make it easier to peruse other logs
//            trusted++;
//            s3Delete(s3Client, bucket, object.key());
//          }

          if (object.lastModified().isBefore(oneYearAgo)) {
            deleted++;
            s3Delete(s3Client, bucket, object.key());
          }
        }
        var now = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault())
            .format(Instant.now());
        LOG.info("Timestamp={} count={} deleted={} key={}", now, count,
            deleted, listObjectsResponse.contents().get(0).key());
        // get the next set
        listObjectsRequest =
            ListObjectsV2Request.builder().bucket(bucket).prefix(keyPrefix)
                .continuationToken(listObjectsResponse.nextContinuationToken()).maxKeys(maxKeys)
                .build();
        listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
        awsResponseValidation(listObjectsResponse);
      }
    }
  }
}
