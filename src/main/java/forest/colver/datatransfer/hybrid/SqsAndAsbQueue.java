package forest.colver.datatransfer.hybrid;

import static forest.colver.datatransfer.aws.AwsUtils.convertSqsMessageAttributesToStrings;
import static forest.colver.datatransfer.aws.AwsUtils.getSqsClient;
import static forest.colver.datatransfer.aws.SqsOperations.qUrl;
import static forest.colver.datatransfer.aws.SqsOperations.sqsConsumeOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDepth;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.azure.AzureUtils.createServiceBusMessage;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbConsume;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbRead;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbSend;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class SqsAndAsbQueue {

  private static final Logger LOG = LoggerFactory.getLogger(SqsAndAsbQueue.class);
  private static final int COPY_ALL_MAX_DEPTH = 1000; // This could probably go as high as 40k
  private static final int COPY_ALL_WAIT_TIME_SECONDS = 2;
  private static final int COPY_ALL_MAX_MESSAGES_PER_BATCH = 10;
  private static final int COPY_ALL_VISIBILITY_TIMEOUT_BASE = 10;

  private SqsAndAsbQueue() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

  public static void moveOneSqsToAsbQueue(
      AwsCredentialsProvider awsCreds,
      String sqs,
      String asbConnectionString,
      String asbQueueName) {
    sqsConsumeOneMessage(awsCreds, sqs)
        .ifPresentOrElse(
            sqsMsg -> {
              Map<String, Object> properties =
                  new HashMap<>(convertSqsMessageAttributesToStrings(sqsMsg.messageAttributes()));
              asbSend(
                  asbConnectionString,
                  asbQueueName,
                  createServiceBusMessage(sqsMsg.body(), properties));
            },
            () -> LOG.error("No SQS message available."));
  }

  public static void moveOneAsbQueueToSqs(
      String asbConnectionString,
      String asbQueueName,
      AwsCredentialsProvider awsCreds,
      String sqs) {
    asbConsume(asbConnectionString, asbQueueName)
        .ifPresentOrElse(
            message -> sendReceivedMessageToSqs(message, awsCreds, sqs),
            () -> LOG.error("No ASB message available."));
  }

  public static void moveAllSqsToAsbQueue(
      AwsCredentialsProvider awsCreds,
      String sqs,
      String asbConnectionString,
      String asbQueueName) {
    var counter = 0;
    var sqsMsg = sqsConsumeOneMessage(awsCreds, sqs);
    while (sqsMsg.isPresent()) {
      counter++;
      var msg = sqsMsg.get();
      Map<String, Object> properties =
          new HashMap<>(convertSqsMessageAttributesToStrings(msg.messageAttributes()));
      asbSend(asbConnectionString, asbQueueName, createServiceBusMessage(msg.body(), properties));
      LOG.info("Moved from SQS={} to ASB-Queue={}; counter={}", sqs, asbQueueName, counter);
      sqsMsg = sqsConsumeOneMessage(awsCreds, sqs);
    }
    LOG.info("Moved {} messages from SQS={} to ASB-Queue={}.", counter, sqs, asbQueueName);
  }

  public static void moveAllAsbQueueToSqs(
      String asbConnectionString,
      String asbQueueName,
      String sqs,
      AwsCredentialsProvider awsCreds) {
    var counter = 0;
    var message = asbConsume(asbConnectionString, asbQueueName);
    while (message.isPresent()) {
      counter++;
      sendReceivedMessageToSqs(message.get(), awsCreds, sqs);
      LOG.info("Moved from ASB-Queue={} to SQS={}; counter={}", asbQueueName, sqs, counter);
      message = asbConsume(asbConnectionString, asbQueueName);
    }
    LOG.info("Moved {} messages from ASB-Queue={} to SQS={}.", counter, asbQueueName, sqs);
  }

  public static void copyOneSqsToAsbQueue(
      AwsCredentialsProvider awsCreds,
      String sqs,
      String asbConnectionString,
      String asbQueueName) {
    sqsReadOneMessage(awsCreds, sqs)
        .ifPresentOrElse(
            msg -> {
              Map<String, Object> properties =
                  new HashMap<>(convertSqsMessageAttributesToStrings(msg.messageAttributes()));
              asbSend(
                  asbConnectionString,
                  asbQueueName,
                  createServiceBusMessage(msg.body(), properties));
            },
            () -> LOG.error("No SQS message available."));
  }

  public static void copyOneAsbQueueToSqs(
      String asbConnectionString,
      String asbQueueName,
      AwsCredentialsProvider awsCreds,
      String sqs) {
    asbRead(asbConnectionString, asbQueueName)
        .ifPresentOrElse(
            message -> sendReceivedMessageToSqs(message, awsCreds, sqs),
            () -> LOG.error("No ASB message available."));
  }

  public static int copyAllSqsToAsbQueue(
      AwsCredentialsProvider awsCreds,
      String sqs,
      String asbConnectionString,
      String asbQueueName) {
    // check the queue depth, if it is beyond a certain size, abort
    var depth = sqsDepth(awsCreds, sqs);
    var counter = 0;
    if (depth < COPY_ALL_MAX_DEPTH) {
      // calculate a visibility timeout, probably 1 sec per message in the sqs
      var visibilityTimeout =
          COPY_ALL_VISIBILITY_TIMEOUT_BASE + depth; // max is 12 hours or 43,200 seconds
      var moreMessages = true;
      try (var sqsClient = getSqsClient(awsCreds)) {
        do {
          var receiveMessageRequest =
              ReceiveMessageRequest.builder()
                  .waitTimeSeconds(COPY_ALL_WAIT_TIME_SECONDS)
                  .messageAttributeNames("All")
                  .attributeNames(QueueAttributeName.ALL)
                  .queueUrl(qUrl(sqsClient, sqs))
                  .maxNumberOfMessages(COPY_ALL_MAX_MESSAGES_PER_BATCH)
                  .visibilityTimeout(visibilityTimeout)
                  .build();
          var response = sqsClient.receiveMessage(receiveMessageRequest);
          if (!response.messages().isEmpty()) {
            for (var message : response.messages()) {
              counter++;
              Map<String, Object> properties =
                  new HashMap<>(convertSqsMessageAttributesToStrings(message.messageAttributes()));
              asbSend(
                  asbConnectionString,
                  asbQueueName,
                  createServiceBusMessage(message.body(), properties));
              LOG.info("Copied message #{}", counter);
            }
          } else {
            moreMessages = false;
          }
        } while (moreMessages);
      }
      LOG.info("Copied {} messages", counter);
    } else {
      counter = -1;
      LOG.info(
          "Queue {} is too deep ({}), for an SQS copy all, max depth is currently {}.",
          sqs,
          depth,
          COPY_ALL_MAX_DEPTH);
    }
    return counter;
  }

  /**
   * Extracts the payload and properties from a ServiceBusReceivedMessage and sends that data to an
   * SQS.
   */
  private static void sendReceivedMessageToSqs(
      ServiceBusReceivedMessage message, AwsCredentialsProvider awsCreds, String sqs) {
    var body = message.getBody().toString();
    Map<String, String> properties =
        message.getApplicationProperties().entrySet().stream()
            .filter(entry -> entry.getValue() instanceof String)
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (String) e.getValue()));
    sqsSend(awsCreds, sqs, body, properties);
  }
}
