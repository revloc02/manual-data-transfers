package forest.colver.datatransfer;

import forest.colver.datatransfer.config.ConfigUtils;
import forest.colver.datatransfer.messaging.Environment;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  public static final String EMX_ERROR = "emx-error";
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    LOG.info("main start");

    // inject file into cubs for Equity Bank
    var filename = "PAIN001_43000245600_43000245600_MD_13032026_EDJC_ALL_008.xml";
    var properties =
        Map.of(
            "name", filename, "datatype", "finance.payment.eft", "targetSystem", "ext-equity-bank");
    var body = ConfigUtils.readFile("src/main/resources/" + filename, StandardCharsets.UTF_8);
    CommonTasks.injectQpidMessage(Environment.STAGE, "cubs-emxonramp-stage", body, properties);

    //    cleanS3DirectoryVersioned("emx-sandbox-sftp-internal", "revloc02");

    //    var bucket = "cp-aws-gayedtiak3nflbiftucz-s3-logging";
    //    var keyPrefix = "emx-sandbox-sftp/";
    //    examineS3Objects(bucket, keyPrefix);

    //    cleanupSftpErrorSqsStage();

    //    JmsSend.sendDefaultMessage();

    //    for (int i = 0; i < 10; i++) {
    //      JmsSend.sendDefaultMessage();
    //    }

    //    CommonTasks.downloadListOfMessagesFromQpid(STAGE,
    // "patriarchal-blessing-emxofframp-gsc-dlq");
    //      "patriarchal-blessing-emxofframp-test-dlq");

    // si-services
    //    moveAllSpecificMessages(
    //        STAGE,
    //        "emx-error",
    //        "emxSystem='si-services' AND emxSystemEnvironment='stage'",
    //        "watchtower-si-services-stage");
    //    moveAllSpecificMessages(
    //        STAGE,
    //        "emx-error",
    //        "emxSystem='si-services' AND emxSystemEnvironment='test'",
    //        "watchtower-si-services-test");
    //    moveAllSpecificMessages(
    //        STAGE,
    //        "emx-error",
    //        "emxSystem='si-services' AND emxSystemEnvironment='test' AND
    // emxErrorRampName='off-ramp-wds'",
    //        "emx-to-si-services-test-wds");
    //    moveAllSpecificMessages(
    //        STAGE,
    //        "watchtower-si-services",
    //        "emxSystem='si-services' AND emxSystemEnvironment='stage' AND
    // emxErrorRampName='off-ramp'",
    //        "emx-to-si-services-stage");
    //    moveAllSpecificMessages(
    //        STAGE,
    //        "watchtower-si-services",
    //        "emxSystem='si-services' AND emxSystemEnvironment='stage' AND
    // emxErrorRampName='off-ramp-wds'",
    //        "emx-to-si-services-stage-wds");

    // Lenel
    //    moveAllSpecificMessages(
    //        PROD,
    //        "emx-error",
    //        "emxSystem='lenel' AND emxErrorRampName='hr-data-broker'",
    //        "watchtower-lenel-hr-data-broker");
    //    moveAllSpecificMessages(
    //        PROD, "emx-error", "emxSystem='lenel' AND emxErrorRampName='off-ramp'",
    // "watchtower-lenel");
    //    moveAllSpecificMessages(
    //        PROD,
    //        "watchtower-lenel-hr-data-broker",
    //        "emxSystem='lenel' AND emxErrorRampName='hr-data-broker'",
    //        "emx-to-lenel-prod-hr-data-broker");
    //    moveAllSpecificMessages(
    //        PROD,
    //        "watchtower-lenel",
    //        "emxSystem='lenel' AND emxErrorRampName='off-ramp'",
    //        "emx-to-lenel-prod");

    //    retrieveMessageFromQpidReplayCache("name='gtmbancoindustrialACH20230411123003061.xml'",
    // "/Users/revloc02/Downloads/gtmbancoindustrialACH20230411123003061.xml");
  }
}
