package forest.colver.datatransfer.hybrid;

import static forest.colver.datatransfer.aws.SqsOperations.sqsConsumeOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.aws.Utils.convertSqsMessageAttributesToStrings;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbConsume;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbSend;
import static forest.colver.datatransfer.azure.Utils.createIMessage;

import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class SqsAndAsbQueue {

  private static final Logger LOG = LoggerFactory.getLogger(SqsAndAsbQueue.class);

  public static void moveOneSqsToAsbQueue(AwsCredentialsProvider awsCreds, String sqs,
      ConnectionStringBuilder azureConnStr) {
    var sqsMsg = sqsConsumeOneMessage(awsCreds, sqs);
    if (sqsMsg != null) {
      // send body and properties to ASB queue
      Map<String, Object> properties = new HashMap<>(
          convertSqsMessageAttributesToStrings(sqsMsg.messageAttributes()));
      asbSend(azureConnStr, createIMessage(sqsMsg.body(), properties));
    } else {
      LOG.error("ERROR: SQS message was null.");
    }
  }

  public static void moveOneAsbQueueToSqs(ConnectionStringBuilder azureConnStr,
      AwsCredentialsProvider awsCreds, String sqs) {
    IMessage message = asbConsume(azureConnStr);
    var asbQueProps = message.getProperties();
    var body = new String(message.getMessageBody().getBinaryData().get(0));
    Map<String, String> properties = asbQueProps.entrySet().stream()
        .filter(entry -> entry.getValue() instanceof String).collect(
            Collectors.toMap(Map.Entry::getKey, e -> (String) e.getValue()));
    sqsSend(awsCreds, sqs, body, properties);
  }

  public static void moveAllSqsToAsbQueue(AwsCredentialsProvider awsCreds, String sqs,
      ConnectionStringBuilder azureConnStr) {
    var moreMessages = true;
    var counter = 0;
    while (moreMessages) {
      var sqsMsg = sqsConsumeOneMessage(awsCreds, sqs);
      if (sqsMsg != null) {
        counter++;
        // send body and properties to ASB queue
        Map<String, Object> properties = new HashMap<>(
            convertSqsMessageAttributesToStrings(sqsMsg.messageAttributes()));
        asbSend(azureConnStr, createIMessage(sqsMsg.body(), properties));
        LOG.info(
            "Moved from SQS={} to ASB-Queue={}; counter={}",
            sqs,
            azureConnStr.getEntityPath(),
            counter);
      } else {
        moreMessages = false;
      }
    }
    LOG.info("Moved {} messages from SQS={} to ASB-Queue={}.", counter, sqs,
        azureConnStr.getEntityPath());
  }

  // todo: this needs a unit test
  public static void moveAllAsbQueueToSqs(String sqs,
      ConnectionStringBuilder azureConnStr, AwsCredentialsProvider awsCreds) {
    var moreMessages = true;
    var counter = 0;
    while (moreMessages) {
      var asbQMsg = asbConsume(azureConnStr);
      if (asbQMsg != null) { // not 100% sure if this strategy will work
        counter++;
        // send body and properties to SQS
        var asbQueProps = asbQMsg.getProperties();
        var body = new String(asbQMsg.getMessageBody().getBinaryData().get(0));
        Map<String, String> properties = asbQueProps.entrySet().stream()
            .filter(entry -> entry.getValue() instanceof String).collect(
                Collectors.toMap(Map.Entry::getKey, e -> (String) e.getValue()));
        sqsSend(awsCreds, sqs, body, properties);
        LOG.info(
            "Moved from ASB-Queue={} to SQS={}; counter={}",
            azureConnStr.getEntityPath(),
            sqs,
            counter);
      } else {
        moreMessages = false;
      }
    }
    LOG.info("Moved {} messages from ASB-Queue={} to SQS={}.", counter,
        azureConnStr.getEntityPath(), sqs);
  }

}
