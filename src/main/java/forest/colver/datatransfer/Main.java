package forest.colver.datatransfer;

import static forest.colver.datatransfer.aws.SqsOperations.*;
import static forest.colver.datatransfer.aws.Utils.*;
import static forest.colver.datatransfer.messaging.Environment.*;
import static forest.colver.datatransfer.messaging.JmsConsume.*;
import static forest.colver.datatransfer.messaging.JmsBrowse.*;
import static forest.colver.datatransfer.messaging.JmsSend.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  public static final String EMX_ERROR = "emx-error";
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    LOG.info("start");

//    sqsCopyAll(getEmxSbCreds(), "sftp-error", "blake-emxonramp-dev");
//    sqsMoveMessagesWithPayloadLike(getEmxSbCreds(), "sftp-error", "lifeflightTestFile", "blake-emxonramp-test");
//    sqsMoveMessagesWithPayloadLike(getEmxNpCreds(), "sftp-error", "lifeflightTestFile", "test-queue");


    moveAllSpecificMessages(PROD, "emx-error", "emxSystem='si-services' AND emxSystemEnvironment='prod'", "emx-to-si-services-prod");
    moveAllSpecificMessages(STAGE, "emx-error", "emxSystem='si-services' AND emxSystemEnvironment='test'", "emx-to-si-services-test");
    moveAllSpecificMessages(STAGE, "emx-error", "emxSystem='si-services' AND emxSystemEnvironment='stage'", "emx-to-si-services-stage");

    moveAllSpecificMessages(STAGE, "emx-error",
        "emxSystem='lenel'",
        "lenel-tyler");

    moveAllSpecificMessages(PROD, "emx-error",
        "emxSystem='identity-vault' AND emxErrorMessage LIKE '%HttpTimeoutException: request timed out%'",
        "emx-to-identity-vault-prod");
    moveAllSpecificMessages(PROD, "emx-error",
        "emxSystem='identity-vault' AND emxErrorMessage LIKE '%Internal Server Error code: 503%'",
        "emx-to-identity-vault-prod");
    moveAllSpecificMessages(STAGE, "emx-error",
        "emxSystem='identity-vault' AND emxSystemEnvironment='stage'",
        "emx-to-identity-vault-stage");
  }
}