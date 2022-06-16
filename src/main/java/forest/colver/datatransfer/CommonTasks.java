package forest.colver.datatransfer;

import static forest.colver.datatransfer.messaging.Environment.PROD;
import static forest.colver.datatransfer.messaging.JmsBrowse.browseAndCountSpecificMessages;
import static forest.colver.datatransfer.messaging.JmsBrowse.browseForSpecificMessage;
import static forest.colver.datatransfer.messaging.JmsConsume.deleteAllSpecificMessages;

/**
 * This defines methods that perform tasks I commonly use in my work or during watchman. They are
 * specific applications of the general messaging tools.
 */
public class CommonTasks {
  // todo: a way to clean up sftp-error sqs removing only certain messages e.g. all health check errors and not interchange errors

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

    // find a timestamp to use in the "timestamp" var, use whatever selector is helpful
    browseForSpecificMessage(PROD, "ops",
        "emxTraceOnrampMessageName='CFISLDS-GTM-INDUS-05-ACH-20210817111659.xml'");

    // check how many messages are going to be deleted according to defined timestamp
    browseAndCountSpecificMessages(PROD, "ops", "emxTraceSourceTimestamp<=" + timestamp);

    // actually delete the messages (uncomment and run, then re-comment the code)
    deleteAllSpecificMessages(PROD, "ops", "emxTraceSourceTimestamp<=" + timestamp);
  }
}
