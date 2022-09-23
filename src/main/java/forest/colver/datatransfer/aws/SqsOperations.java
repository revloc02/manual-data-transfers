package forest.colver.datatransfer.aws;

import static forest.colver.datatransfer.aws.Utils.awsResponseValidation;
import static forest.colver.datatransfer.aws.Utils.createMessageAttributes;
import static forest.colver.datatransfer.aws.Utils.getSqsClient;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Contains several common SQS operations for sending and receiving data from them.
 */
public class SqsOperations {

  private static final Logger LOG = LoggerFactory.getLogger(SqsOperations.class);

  /**
   * Overloaded method that sends in an empty Map of messages properties.
   */
  public static void sqsSend(AwsCredentialsProvider awsCp, String queueName, String payload) {
    Map<String, String> messageProps = Map.of();
    sqsSend(awsCp, queueName, payload, messageProps);
  }

  /**
   * Sends a {@link software.amazon.awssdk.services.sqs.model.Message Message} to an SQS.
   *
   * @param awsCp Credentials.
   * @param queueName SQS name.
   * @param message A {@link software.amazon.awssdk.services.sqs.model.Message Message}.
   */
  public static void sqsSend(AwsCredentialsProvider awsCp, String queueName, Message message) {
    try (var sqsClient = getSqsClient(awsCp)) {
      var sendMessageRequest =
          SendMessageRequest.builder()
              .messageBody(message.body())
              .messageAttributes(createMessageAttributes(message.attributesAsStrings()))
              .queueUrl(qUrl(sqsClient, queueName))
              .build();
      var response = sqsClient.sendMessage(sendMessageRequest);
      awsResponseValidation(response);
      LOG.info("SQSSEND: messageId={} was put on the SQS: {}.", message.messageId(), queueName);
    }
  }

  /**
   * Send a message using a map of message properties to the desired SQS queue.
   */
  public static void sqsSend(
      AwsCredentialsProvider awsCp,
      String queueName,
      String payload,
      Map<String, String> messageProps) {
    try (var sqsClient = getSqsClient(awsCp)) {
      var sendMessageRequest =
          SendMessageRequest.builder()
              .messageBody(payload)
              .messageAttributes(createMessageAttributes(messageProps))
              .queueUrl(qUrl(sqsClient, queueName))
              .build();
      var response = sqsClient.sendMessage(sendMessageRequest);
      awsResponseValidation(response);
      LOG.info("SQSSEND: The payload '{}' was put on the SQS: {}.", payload, queueName);
    }
  }

  /**
   * This retrieves one message from the SQS queue, then deletes that message off of the SQS.
   *
   * @return A Message.
   */
  public static Message sqsConsumeOneMessage(AwsCredentialsProvider awsCP, String queueName) {
    var response = sqsReadOneMessage(awsCP, queueName);
    if (response.hasMessages()) {
      sqsDelete(awsCP, response, queueName);
      LOG.info("======== SQSCONSUME: Consumed a message from SQS: {}.=======", queueName);
      return response.messages().get(0);
    } else {
      LOG.info("======== SQSCONSUME: No messages to consume from SQS: {}.=======", queueName);
      return null;
    }
  }

  /**
   * Reads one message from the SQS, and then displays the data and properties of it.
   */
  public static ReceiveMessageResponse sqsReadOneMessage(
      AwsCredentialsProvider awsCP, String queueName) {
    try (var sqsClient = getSqsClient(awsCP)) {
      var receiveMessageRequest =
          ReceiveMessageRequest.builder()
              .waitTimeSeconds(2)
              .messageAttributeNames("All")
              .attributeNames(QueueAttributeName.ALL)
              .queueUrl(qUrl(sqsClient, queueName))
              .maxNumberOfMessages(1)
              .visibilityTimeout(3) // default 30 sec
              .build();
      var response = sqsClient.receiveMessage(receiveMessageRequest);
      awsResponseValidation(response);
      LOG.info("SQSREAD: {} has messages: {}", queueName, response.hasMessages());
      displayMessageAttributes(response);
      return response;
    }
  }

  /**
   * Clears an SQS.
   */
  public static void sqsPurge(AwsCredentialsProvider awsCP, String queueName) {
    try (var sqsClient = getSqsClient(awsCP)) {
      var purgeQueueRequest =
          PurgeQueueRequest.builder().queueUrl(qUrl(sqsClient, queueName)).build();
      var response = sqsClient.purgeQueue(purgeQueueRequest);
      awsResponseValidation(response);
      LOG.info("SQSPURGE: The SQS {} has beeen purged.", queueName);
    }
  }

  /**
   * Displays the data and message properties of SQS messages.
   */
  public static void displayMessageAttributes(ReceiveMessageResponse response) {
    for (Message message : response.messages()) {
      LOG.info("Message payload: {}", message.body());
      for (Map.Entry<String, String> entry : message.attributesAsStrings().entrySet()) {
        LOG.info("Attribute: {}={}", entry.getKey(), entry.getValue());
      }
      for (Map.Entry<String, MessageAttributeValue> entry :
          message.messageAttributes().entrySet()) {
        LOG.info("MessageAttribute: {}={}", entry.getKey(), entry.getValue());
      }
      LOG.info("\n");
    }
  }

  /**
   * Gets the attributes from the SQS.
   */
  public static GetQueueAttributesResponse sqsGetQueueAttributes(
      AwsCredentialsProvider awsCP, String queueName) {
    try (var sqsClient = getSqsClient(awsCP)) {
      var getQueueAttributesRequest =
          GetQueueAttributesRequest.builder()
              .queueUrl(qUrl(sqsClient, queueName))
              .attributeNamesWithStrings("All")
              .build();
      var response = sqsClient.getQueueAttributes(getQueueAttributesRequest);
      awsResponseValidation(response);
      if (response.hasAttributes()) {
        for (Map.Entry<String, String> entry : response.attributesAsStrings().entrySet()) {
          LOG.info("Queue Attribute {} = {}", entry.getKey(), entry.getValue());
        }
      } else {
        LOG.info("SQS queue attributes is null.");
      }
      return response;
    }
  }

  public static int sqsDepth(AwsCredentialsProvider awsCP, String queueName) {
    var response = sqsGetQueueAttributes(awsCP, queueName);
    var numMsgs = 0;
    if (response.hasAttributes()) {
      numMsgs = Integer.parseInt(
          response.attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
    } else {
      LOG.error("ERROR: SQS queue attributes is null.");
    }
    return numMsgs;
  }

  /**
   * Goes and gets the queueUrl so that queue can be accessed for operations.
   */
  private static String qUrl(SqsClient sqsClient, String queueName) {
    String queueUrl =
        sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl();
//    LOG.info("Queue URL={}", queueUrl);
    return queueUrl;
  }

  /**
   * Gets a list of messages from a given SQS.
   */
  private static List<Message> getMessages(SqsClient sqsClient, String queueUrl) {
    ReceiveMessageRequest.Builder receiveMessageRequestBuilder =
        ReceiveMessageRequest.builder()
            .waitTimeSeconds(1)
            .messageAttributeNames("All")
            .attributeNames(QueueAttributeName.ALL)
            .queueUrl(queueUrl)
            .maxNumberOfMessages(10);
    ReceiveMessageResponse receiveMessageResponse =
        sqsClient.receiveMessage(receiveMessageRequestBuilder.build());
    List<Message> messages = receiveMessageResponse.messages();
    LOG.info("count={}", messages.size());
    if (!messages.isEmpty()) {
      LOG.info("Body[0]={}", messages.get(0).body());
    }
    return messages;
  }

  /**
   * Deletes a list of messages from the given SQS.
   *
   * @param response ReceiveMessageResponse which contains the list of messages to be deleted.
   */
  public static void sqsDelete(
      AwsCredentialsProvider awsCP, ReceiveMessageResponse response, String queueName) {
    try (var sqsClient = getSqsClient(awsCP)) {
      for (Message message : response.messages()) {
        var deleteMessageRequest =
            DeleteMessageRequest.builder()
                .queueUrl(qUrl(sqsClient, queueName))
                .receiptHandle(message.receiptHandle())
                .build();
        var deleteResponse = sqsClient.deleteMessage(deleteMessageRequest);
        awsResponseValidation(deleteResponse);
        LOG.info("DELETE: message {}.", message);
      }
    }
  }

  /**
   * Copy a message from one SQS queue to another.
   */
  public static void sqsCopy(AwsCredentialsProvider awsCP, String fromSqs, String toSqs) {
    var response = sqsReadOneMessage(awsCP, fromSqs);
    for (Message message : response.messages()) {
      sqsSend(awsCP, toSqs, message.body());
    }
  }

  /**
   * Move a message from one SQS queue to another.
   */
  public static void sqsMove(AwsCredentialsProvider awsCP, String fromSqs, String toSqs) {
    var message = sqsConsumeOneMessage(awsCP, fromSqs);
    sqsSend(awsCP, toSqs, message.body(), message.attributesAsStrings());
  }

  public static void sqsCopyAll(AwsCredentialsProvider awsCP, String fromQueue, String toQueue) {
    // todo: remember during a copy all the visibility timeout needs to be managed appropriately so the already copied message doesn't become available and copied again
    // todo: in connection with the above comment, probably do moveAll first and see how long it takes to move 1 M messages do visibility timeout is easier to manage

  }

  /**
   * Moves all messages from one SQS to another. This method is slow and verbose.
   *
   * @param awsCP creds
   * @param fromSqs source SQS
   * @param toSqs target SQS
   */
  public static void sqsMoveAllVerbose(AwsCredentialsProvider awsCP, String fromSqs, String toSqs) {
    var counter = 0;
    var moreMessages = true;
    Message message;
    while (moreMessages) {
      message = sqsConsumeOneMessage(awsCP, fromSqs);
      if (message != null) {
        counter++;
        sqsSend(awsCP, toSqs, message.body(), message.attributesAsStrings());
        LOG.info("Moved from SQS={} to SQS={}, counter={}", fromSqs, toSqs, counter);
      } else {
        moreMessages = false;
      }
    }
    LOG.info("Moved {} messages.", counter);
  }

  /**
   * Move all messages from one SQS to another, with less log verbosity. I was also attempting to
   * make this method faster, but it is only about twice as fast as {@link
   * #sqsMoveAllVerbose(AwsCredentialsProvider, String, String) sqsMoveAllVerbose}.
   */
  public static void sqsMoveAll(AwsCredentialsProvider awsCP, String fromSqs, String toSqs) {
    var counter = 0;
    var moreMessages = true;
    try (var sqsClient = getSqsClient(awsCP)) {
      do {
        // receive
        var receiveMessageRequest =
            ReceiveMessageRequest.builder()
                .waitTimeSeconds(2)
                .messageAttributeNames("All")
                .attributeNames(QueueAttributeName.ALL)
                .queueUrl(qUrl(sqsClient, fromSqs))
                .maxNumberOfMessages(10)
                .visibilityTimeout(3) // default 30 sec
                .build();
        var response = sqsClient.receiveMessage(receiveMessageRequest);
        // send
        if (response.hasMessages()) {
          for (var message : response.messages()) {
            counter++;
            var sendMessageRequest =
                SendMessageRequest.builder()
                    .messageBody(message.body())
                    .messageAttributes(createMessageAttributes(message.attributesAsStrings()))
                    .queueUrl(qUrl(sqsClient, toSqs))
                    .build();
            sqsClient.sendMessage(sendMessageRequest);
          }
        } else {
          moreMessages = false;
        }
        // delete
        for (Message message : response.messages()) {
          var deleteMessageRequest =
              DeleteMessageRequest.builder()
                  .queueUrl(qUrl(sqsClient, fromSqs))
                  .receiptHandle(message.receiptHandle())
                  .build();
          sqsClient.deleteMessage(deleteMessageRequest);
        }
      } while (moreMessages);
    }
    LOG.info("Moved {} messages.", counter);
  }

  /**
   * SQS Selector. Find messages on an SQS with certain attributes. How to do this right?
   */
  public static void sqsSelector() {
    // todo: indeed, how to do this correctly?
    // for deep queues this is impossible. But for queue with, less than 100 say, perhaps the
    // visibility timeout could be used to rummage through all of the messages to find the ones you
    // want. Maybe either move them to another queue or collect their message.IDs--can you access
    // a message from an SQS by its message.ID?
  }
}
