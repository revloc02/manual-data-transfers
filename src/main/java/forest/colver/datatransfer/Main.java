package forest.colver.datatransfer;

import static forest.colver.datatransfer.CommonTasks.*;
import static forest.colver.datatransfer.messaging.Environment.PROD;
import static forest.colver.datatransfer.messaging.JmsBrowse.browseAndCountMessages;
import static forest.colver.datatransfer.messaging.JmsBrowse.browseForMessage;
import static forest.colver.datatransfer.messaging.JmsBrowse.copySpecificMessages;
import static forest.colver.datatransfer.messaging.JmsConsume.deleteAllMessagesFromQueue;
import static forest.colver.datatransfer.messaging.JmsConsume.deleteAllSpecificMessagesFromQueue;
import static forest.colver.datatransfer.messaging.JmsConsume.deleteSpecificMessages;

import forest.colver.datatransfer.messaging.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);
  public static final String EMX_ERROR = "emx-error";

  public static void main(String[] args) {
    LOG.info("log");
    cleanupOpsQueue();
//    browseForMessage(PROD, EMX_ERROR, "emxSystem='cars' AND emxErrorMessage LIKE '%The server was unable to process the request due to an internal error.%'");

//    copySpecificMessages(PROD, EMX_ERROR, "emxSystem='cars' AND emxErrorMessage LIKE '%The server was unable to process the request due to an internal error.%'", "skim-forest");

//    deleteAllSpecificMessagesFromQueue(PROD, EMX_ERROR, "emxSystem='cars' AND emxErrorMessage LIKE '%The server was unable to process the request due to an internal error.%'");

//      copySpecificMessages(PROD, "skim-forest", "emxSystem='cars'", "emx-to-cars-prod");

  }
}
