package forest.colver.datatransfer.hybrid;

import static forest.colver.datatransfer.aws.SqsOperations.qUrl;
import static forest.colver.datatransfer.aws.SqsOperations.sqsConsumeOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDepth;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.aws.Utils.convertSqsMessageAttributesToStrings;
import static forest.colver.datatransfer.aws.Utils.getSqsClient;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbConsume;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbRead;
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
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class SqsAndAsbQueue {

  private SqsAndAsbQueue() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

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
    IMessage iMessage = asbConsume(azureConnStr);
    sendIMessageToSqs(iMessage, awsCreds, sqs);
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

  public static void moveAllAsbQueueToSqs(ConnectionStringBuilder azureConnStr, String sqs,
      AwsCredentialsProvider awsCreds) {
    var moreMessages = true;
    var counter = 0;
    while (moreMessages) {
      var iMessage = asbConsume(azureConnStr);
      if (iMessage != null) {
        counter++;
        // send body and properties to SQS
        sendIMessageToSqs(iMessage, awsCreds, sqs);
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

  public static void copyOneSqsToAsbQueue(AwsCredentialsProvider awsCreds, String sqs,
      ConnectionStringBuilder azureConnStr) {
    var sqsMsg = sqsReadOneMessage(awsCreds, sqs);
    if (sqsMsg != null) {
      // send body and properties to ASB queue
      Map<String, Object> properties = new HashMap<>(
          convertSqsMessageAttributesToStrings(sqsMsg.messageAttributes()));
      asbSend(azureConnStr, createIMessage(sqsMsg.body(), properties));
    } else {
      LOG.error("ERROR: SQS message was null.");
    }
  }

  public static void copyOneAsbQueueToSqs(ConnectionStringBuilder azureConnStr,
      AwsCredentialsProvider awsCreds, String sqs) {
    var iMessage = asbRead(azureConnStr);
    if (iMessage != null) {
      sendIMessageToSqs(iMessage, awsCreds, sqs);
    } else {
      LOG.error("ERROR: ASB queue IMessage was null.");
    }
  }

  public static int copyAllSqsToAsbQueue(AwsCredentialsProvider awsCreds, String sqs,
      ConnectionStringBuilder azureConnStr) {
    // check the queue depth, if it is beyond a certain size, abort
    var depth = sqsDepth(awsCreds, sqs);
    var maxDepth = 1000; // This could probably go as high as 40k
    var counter = 0;
    if (depth < maxDepth) {
      // calculate a visibility timeout, probably 1 sec per message in the sqs
      var visibilityTimeout = 10 + (depth); // max is 12 hours or 43,200 seconds
      var moreMessages = true;
      try (var sqsClient = getSqsClient(awsCreds)) {
        do {
          var receiveMessageRequest =
              ReceiveMessageRequest.builder()
                  .waitTimeSeconds(2)
                  .messageAttributeNames("All")
                  .attributeNames(QueueAttributeName.ALL)
                  .queueUrl(qUrl(sqsClient, sqs))
                  .maxNumberOfMessages(10)
                  .visibilityTimeout(visibilityTimeout)
                  .build();
          var response = sqsClient.receiveMessage(receiveMessageRequest);
          if (!response.messages().isEmpty()) {
            for (var message : response.messages()) {
              // copy to ASB queue
              counter++;
              Map<String, Object> properties = new HashMap<>(
                  convertSqsMessageAttributesToStrings(message.messageAttributes()));
              asbSend(azureConnStr, createIMessage(message.body(), properties));
              LOG.info("Copied message #{}", counter);
            }
          } else {
            moreMessages = false;
          }
        } while (moreMessages);
      }
      // display summary: num messages checked, num messages moved
      LOG.info("Copied {} messages", counter);
    } else {
      counter = -1;
      LOG.info("Queue {} is too deep ({}), for an SQS copy all, max depth is currently {}.",
          sqs, depth, maxDepth);
    }
    return counter;

  }

  /**
   * Extracts the payload and properties from an IMessage and sends that data to an SQS.
   *
   * @param iMessage An Azure IMessage.
   * @param awsCreds Credentials for AWS.
   * @param sqs The SQS queue.
   */
  private static void sendIMessageToSqs(IMessage iMessage, AwsCredentialsProvider awsCreds,
      String sqs) {
    var asbQueProps = iMessage.getProperties();
    var body = new String(iMessage.getMessageBody().getBinaryData().get(0));
    Map<String, String> properties = asbQueProps.entrySet().stream()
        .filter(entry -> entry.getValue() instanceof String).collect(
            Collectors.toMap(Map.Entry::getKey, e -> (String) e.getValue()));
    sqsSend(awsCreds, sqs, body, properties);
  }
}
