package forest.colver.datatransfer.hybrid;

import static forest.colver.datatransfer.aws.SqsOperations.sqsConsumeOneMessage;
import static forest.colver.datatransfer.aws.Utils.convertSqsMessageAttributesToStrings;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbSend;
import static forest.colver.datatransfer.azure.Utils.createIMessage;

import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import java.util.HashMap;
import java.util.Map;
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
}
