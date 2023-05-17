package forest.colver.datatransfer;

import static forest.colver.datatransfer.CommonTasks.cleanS3Directory;
import static forest.colver.datatransfer.CommonTasks.examineS3Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  public static final String EMX_ERROR = "emx-error";
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    LOG.info("main start");

    var bucket = "cp-aws-gayedtiak3nflbiftucz-s3-logging";
    var keyPrefix = "emx-sandbox-sftp/";
    examineS3Objects(bucket, keyPrefix);



//    cleanupSftpErrorSqsStage();

//    moveAllSpecificMessages(PROD, "emx-error", "emxSystem='si-services' AND emxSystemEnvironment='prod'", "emx-to-si-services-prod");
//    moveAllSpecificMessages(STAGE, "emx-error", "emxSystem='si-services' AND emxSystemEnvironment='test'", "emx-to-si-services-test");
//    moveAllSpecificMessages(STAGE, "emx-error", "emxSystem='si-services' AND emxSystemEnvironment='stage'", "emx-to-si-services-stage");

//    retrieveMessageFromQpidReplayCache("name='gtmbancoindustrialACH20230411123003061.xml'", "/Users/revloc02/Downloads/gtmbancoindustrialACH20230411123003061.xml");

//    moveAllSpecificMessages(PROD, "emx-error",
//        "emxSystem='identity-vault' AND emxErrorMessage LIKE '%HttpTimeoutException: request timed out%'",
//        "emx-to-identity-vault-prod");
//    moveAllSpecificMessages(PROD, "emx-error",
//        "emxSystem='identity-vault' AND emxErrorMessage LIKE '%Internal Server Error code: 503%'",
//        "emx-to-identity-vault-prod");
//    moveAllSpecificMessages(STAGE, "emx-error",
//        "emxSystem='identity-vault' AND emxSystemEnvironment='stage'",
//        "emx-to-identity-vault-stage");
  }
}