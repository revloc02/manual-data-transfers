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

    moveAllSpecificMessages(PROD, "emx-error", "emxSystem='si-services' AND emxSystemEnvironment='prod'", "emx-to-si-services-prod");
    moveAllSpecificMessages(STAGE, "emx-error", "emxSystem='si-services' AND emxSystemEnvironment='test'", "emx-to-si-services-test");
    moveAllSpecificMessages(STAGE, "emx-error", "emxSystem='si-services' AND emxSystemEnvironment='stage'", "emx-to-si-services-stage");

    moveAllSpecificMessages(STAGE, "emx-error",
        "emxSystem='lenel'",
        "lenel-tyler");

    moveAllSpecificMessages(PROD, "emx-error",
        "emxSystem='identity-vault' AND emxErrorMessage LIKE '%HttpTimeoutException: request timed out%'",
        "emx-to-identity-vault-prod");
    moveAllSpecificMessages(STAGE, "emx-error",
        "emxSystem='identity-vault' AND emxSystemEnvironment='stage'",
        "emx-to-identity-vault-stage");

//    moveAllSpecificMessages(PROD, "emx-error", "emxSystem='identity-vault'", "identity-vault-error");
//    browseAndCountSpecificMessages(PROD, "emx-error", "emxSystem='crm-correctional-services' AND emxErrorMessage LIKE '%TimeoutException: Request timed out.%'");

//    moveAllSpecificMessages(PROD, "emx-error",
//        "emxSystem='crm-missionary-inquiry' AND emxErrorMessage LIKE '%TimeoutException: Request timed out.%'",
//        "emx-to-crm-missionary-inquiry-prod");

//    moveAllSpecificMessages(STAGE, "emx-error", "emxSystem='ext-salesforce-ccd'", "emx-to-ext-salesforce-ccd-test");
//    moveAllSpecificMessages(STAGE, "emx-error", "emxSystem='crm-missionary-recommend' AND emxSystemEnvironment='dev'", "emx-to-crm-missionary-recommend-dev");
//    moveAllSpecificMessages(STAGE, "emx-error", "emxSystem='crm-missionary-recommend' AND emxSystemEnvironment='test'", "emx-to-crm-missionary-recommend-test");
//    moveAllSpecificMessages(STAGE, "emx-error", "emxSystem='crm-missionary-inquiry' AND emxSystemEnvironment='test'", "emx-to-crm-missionary-inquiry-test");
//    moveAllSpecificMessages(STAGE, "emx-error", "emxSystem='crm-missionary-inquiry' AND emxSystemEnvironment='dev'", "emx-to-crm-missionary-inquiry-dev");
//    moveAllSpecificMessages(STAGE, "emx-error", "emxSystem='crm-correctional-services' AND emxSystemEnvironment='stage'", "emx-to-crm-correctional-services-stage");
//    browseAndCountSpecificMessages(STAGE, "emx-error", "emxSourceSystem='gfs-d365'");
  }
}