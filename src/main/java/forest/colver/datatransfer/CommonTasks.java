package forest.colver.datatransfer;

import static forest.colver.datatransfer.aws.SqsOperations.sqsMoveMessagesWithPayloadLike;
import static forest.colver.datatransfer.aws.Utils.getEmxNpCreds;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.config.Utils.writeFile;
import static forest.colver.datatransfer.messaging.Environment.PROD;
import static forest.colver.datatransfer.messaging.JmsBrowse.browseAndCountSpecificMessages;
import static forest.colver.datatransfer.messaging.JmsBrowse.browseForSpecificMessage;
import static forest.colver.datatransfer.messaging.JmsConsume.deleteAllSpecificMessages;
import static forest.colver.datatransfer.messaging.Utils.getJmsMsgPayload;

/**
 * This defines methods that perform tasks I commonly use in my work or during watchman. They are
 * specific applications of the general messaging tools.
 */
public class CommonTasks {

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
   * Clears Lifeflight health checks from the sftp-error queue. Occasionally a Lifeflight health
   * check will fail for some random reason and leave an error in the sftp error queue. These errors
   * are typically anomalies and not valuable since the health check clears the next run.
   */
  public static void cleanupSftpErrorSqsStage() {
    // Sandbox. Obviously get sandbox creds before running this.
//    sqsMoveMessagesWithPayloadLike(getEmxSbCreds(), "sftp-error", "lifeflightTestFile", "blake-emxonramp-test");

    // todo: after I've verified that this does what I think it does and nothing more than that, I need to not move the messages, but rather delete them likely requiring a new method
    // Stage. Obviously get sandbox creds before running this.
    sqsMoveMessagesWithPayloadLike(getEmxNpCreds(), "sftp-error", "lifeflightTestFile",
        "test-queue");
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

    // 1. find a timestamp to use in the "timestamp" var above, get this from the list of messages in the queue. Use whatever selector is helpful to do this.
    browseForSpecificMessage(PROD, "ops",
        "emxTraceOnrampMessageName='CFISLDS-GTM-INDUS-05-ACH-20210817111659.xml'");

    // 2. Once you have set the timestamp var above, check how many messages are going to be deleted according that timestamp
    browseAndCountSpecificMessages(PROD, "ops", "emxTraceSourceTimestamp<=" + timestamp);

    // 3. If those results look good, actually delete the messages (uncomment and run, then re-comment the code)
//    deleteAllSpecificMessages(PROD, "ops", "emxTraceSourceTimestamp<=" + timestamp);
  }
}
