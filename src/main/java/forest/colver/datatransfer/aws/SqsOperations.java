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

  // todo: how to sqsSend with a Message object as the arguement?

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
      LOG.info("SQSSEND: The payload '{}' was put on the SQS: {}.\n", payload, queueName);
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
      LOG.info("======== SQSCONSUME: Consumed a message from SQS: {}.=======\n", queueName);
      return response.messages().get(0);
    } else {
      LOG.info("======== SQSCONSUME: No messages to consume from SQS: {}.=======\n", queueName);
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
      LOG.info("SQSPURGE: The SQS {} has beeen purged.\n", queueName);
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
        LOG.info("Attributes is null.");
      }
      return response;
    }
  }

  /**
   * Goes and gets the queueUrl so that queue can be accessed for operations.
   */
  private static String qUrl(SqsClient sqsClient, String queueName) {
    String queueUrl =
        sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl();
    LOG.info("Queue URL={}", queueUrl);
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
   * Deletes messages from the given SQS.
   *
   * @param response ReceiveMessageResponse which contains the list of messages to be deleted.
   */
  public static void sqsDelete(
      AwsCredentialsProvider awsCP, ReceiveMessageResponse response, String queueName) {
    try (var sqsClient = getSqsClient(awsCP)) {
      for (Message message : response.messages()) {
        var deleteMessageReqest =
            DeleteMessageRequest.builder()
                .queueUrl(qUrl(sqsClient, queueName))
                .receiptHandle(message.receiptHandle())
                .build();
        var deleteResponse = sqsClient.deleteMessage(deleteMessageReqest);
        awsResponseValidation(deleteResponse);
        LOG.info("DELETE: message {}.", message);
      }
    }
  }

  /**
   * Copy a message from one SQS queue to another.
   */
  public static void sqsCopy(AwsCredentialsProvider awsCP, String fromQueue, String toQueue) {
    var response = sqsReadOneMessage(awsCP, fromQueue);
    for (Message message : response.messages()) {
      sqsSend(awsCP, toQueue, message.body());
    }
  }

  /**
   * Move a message from one SQS queue to another.
   */
  public static void sqsMove(AwsCredentialsProvider awsCP, String fromQueue, String toQueue) {
    var message = sqsConsumeOneMessage(awsCP, fromQueue);
    sqsSend(awsCP, toQueue, message.body(), message.attributesAsStrings());
  }

  // todo: need a sqsCopyAll and sqsMoveAll
}
