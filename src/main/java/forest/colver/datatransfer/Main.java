package forest.colver.datatransfer;

import static forest.colver.datatransfer.messaging.Environment.PROD;
import static forest.colver.datatransfer.messaging.Environment.STAGE;
import static forest.colver.datatransfer.messaging.JmsConsume.moveAllSpecificMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  public static final String EMX_ERROR = "emx-error";
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    LOG.info("main start");

    //    var bucket = "cp-aws-gayedtiak3nflbiftucz-s3-logging";
    //    var keyPrefix = "emx-sandbox-sftp/";
    //    examineS3Objects(bucket, keyPrefix);

    //    cleanupSftpErrorSqsStage();

    // si-services
    moveAllSpecificMessages(
        PROD,
        "emx-error",
        "emxSystem='si-services' AND emxSystemEnvironment='prod'",
        "emx-to-si-services-prod");
    moveAllSpecificMessages(
        STAGE,
        "emx-error",
        "emxSystem='si-services' AND emxSystemEnvironment='test'",
        "emx-to-si-services-test");
    moveAllSpecificMessages(
        STAGE,
        "emx-error",
        "emxSystem='si-services' AND emxSystemEnvironment='stage'",
        "watchtower-si-services");
    moveAllSpecificMessages(
        STAGE,
        "watchtower-si-services",
        "emxSystem='si-services' AND emxSystemEnvironment='stage' AND emxErrorRampName='off-ramp'",
        "emx-to-si-services-stage");
    moveAllSpecificMessages(
        STAGE,
        "watchtower-si-services",
        "emxSystem='si-services' AND emxSystemEnvironment='stage' AND emxErrorRampName='off-ramp-wds'",
        "emx-to-si-services-stage-wds");

    // Lenel
    moveAllSpecificMessages(
        PROD,
        "emx-error",
        "emxSystem='lenel' AND emxErrorRampName='hr-data-broker'",
        "watchtower-lenel-hr-data-broker");
    moveAllSpecificMessages(
        PROD, "emx-error", "emxSystem='lenel' AND emxErrorRampName='off-ramp'", "watchtower-lenel");
    moveAllSpecificMessages(
        PROD,
        "watchtower-lenel-hr-data-broker",
        "emxSystem='lenel' AND emxErrorRampName='hr-data-broker'",
        "emx-to-lenel-prod-hr-data-broker");
    moveAllSpecificMessages(
        PROD,
        "watchtower-lenel",
        "emxSystem='lenel' AND emxErrorRampName='off-ramp'",
        "emx-to-lenel-prod");

    //    retrieveMessageFromQpidReplayCache("name='gtmbancoindustrialACH20230411123003061.xml'",
    // "/Users/revloc02/Downloads/gtmbancoindustrialACH20230411123003061.xml");
  }
}
