package forest.colver.datatransfer.aws;

import static forest.colver.datatransfer.aws.Utils.awsResponseValidation;
import static forest.colver.datatransfer.aws.Utils.createSqsMessageAttributes;
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
              .messageAttributes(message.messageAttributes())
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
              .messageAttributes(createSqsMessageAttributes(messageProps))
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

  /**
   * Copy all messages from one SQS to another. 1) Check the queue depth, if it is deeper than 1000
   * messages, abort. 2) Calculate a visibility timeout, one second per message currently on the
   * SQS. 3) Retrieve each message from the SQS setting the visibility timeout. 4) Copy the message
   * to the other SQS.
   *
   * @param awsCP Credentials.
   * @param fromSqs Source SQS.
   * @param toSqs Destination SQS.
   */
  public static int sqsCopyAll(AwsCredentialsProvider awsCP, String fromSqs, String toSqs) {
    // check the queue depth, if it is beyond a certain size, abort
    var depth = sqsDepth(awsCP, fromSqs);
    var maxDepth = 1000; // This could probably go as high as 40k
    var counter = 0;
    if (depth < maxDepth) {
      // calculate a visibility timeout, probably 1 sec per message in the sqs
      var visibilityTimeout = 10 + (depth);
      var moreMessages = true;
      try (var sqsClient = getSqsClient(awsCP)) {
        do {
          // receive 10 messages, setting the visibility timeout
          var receiveMessageRequest =
              ReceiveMessageRequest.builder()
                  .waitTimeSeconds(2)
                  .messageAttributeNames("All")
                  .attributeNames(QueueAttributeName.ALL)
                  .queueUrl(qUrl(sqsClient, fromSqs))
                  .maxNumberOfMessages(10)
                  .visibilityTimeout(visibilityTimeout) // default 30 sec
                  .build();
          var response = sqsClient.receiveMessage(receiveMessageRequest);
          if (response.hasMessages()) {
            for (var message : response.messages()) {
              // copy to other queue
              counter++;
              var sendMessageRequest =
                  SendMessageRequest.builder()
                      .messageBody(message.body())
                      .messageAttributes(message.messageAttributes())
                      .queueUrl(qUrl(sqsClient, toSqs))
                      .build();
              sqsClient.sendMessage(sendMessageRequest);
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
          fromSqs,
          depth, maxDepth);
    }
    return counter;
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
            counter = moveMessage(fromSqs, toSqs, counter, sqsClient, message);
          }
        } else {
          moreMessages = false;
        }
      } while (moreMessages);
    }
    LOG.info("Moved {} messages.", counter);
  }

  /**
   * Pseudo SQS Selector. Find messages on an SQS with a certain attribute and move it to another
   * SQS. 1) Retrieve a message, using an appropriate visibility timeout. 2) Identify if the message
   * has the specific attribute that meets the criteria you are interested in. 3a) If it does, move
   * the message and delete it from the SQS. 3b) If it does not, ignore it, and it will become
   * available again after the visibility timeout is over. If the queue is too deep this strategy
   * will not work as the entire queue must be iterated through before the visibility timeout is
   * over, making messages already checked available again. SQS Visibility Timeout: Default= 30
   * seconds, Max= 43,200 seconds (12 hours).
   */
  public static int sqsMoveMessagesWithSelectedAttribute(AwsCredentialsProvider awsCP,
      String fromSqs,
      String selectKey, String selectValue, String toSqs) {
    // check queue depth, if it is too deep just stop
    var depth = sqsDepth(awsCP, fromSqs);
    var maxDepth = 100; // This could probably go as high as 40k, or 50k if the vt calculation is changed
    var counter = 0;
    if (depth < maxDepth) {
      // from queue depth calculate visibility timeout
      var visibilityTimeout = 10 + (depth);
      var moreMessages = true;
      try (var sqsClient = getSqsClient(awsCP)) {
        do {
          // receive 10 messages
          var receiveMessageRequest =
              ReceiveMessageRequest.builder()
                  .waitTimeSeconds(2)
                  .messageAttributeNames("All")
                  .attributeNames(QueueAttributeName.ALL)
                  .queueUrl(qUrl(sqsClient, fromSqs))
                  .maxNumberOfMessages(10)
                  .visibilityTimeout(visibilityTimeout) // default 30 sec
                  .build();
          var response = sqsClient.receiveMessage(receiveMessageRequest);
          if (response.hasMessages()) {
            for (var message : response.messages()) {
              // check each one for selector stuff
              if (message.hasAttributes()) {
                if (message.messageAttributes().get(selectKey) != null) {
                  if (message.messageAttributes().get(selectKey).stringValue()
                      .equals(selectValue)) {
                    counter = moveMessage(fromSqs, toSqs, counter, sqsClient, message);
                  } else {
                    LOG.info("This message doesn't have any matching attributes, bypassing it.");
                  }
                } else {
                  LOG.info("Message does not have desired attribute key, bypassing it.");
                }
              } else {
                LOG.info("Message does not have any attributes, bypassing it.");
              }
            }
          } else {
            moreMessages = false;
          }
        } while (moreMessages);
      }
      // display summary: num messages checked, num messages moved
      LOG.info("Moved {} messages matching Key={} and Value={}", counter, selectKey, selectValue);
    } else {
      counter = -1;
      LOG.info(
          "Queue {} is too deep ({}), for selective message moving, max depth is currently {}.",
          fromSqs,
          depth, maxDepth);
    }
    return counter;
  }

  public static int sqsMoveMessagesWithPayloadLike(AwsCredentialsProvider awsCP, String fromSqs,
      String payloadLike, String toSqs) {
    // check queue depth, if it is too deep just stop
    var depth = sqsDepth(awsCP, fromSqs);
    var maxDepth = 500; // This could probably go as high as 40k
    var counter = 0;
    if (depth < maxDepth) {
      // from queue depth calculate visibility timeout
      var visibilityTimeout = 10 + (depth);
      var moreMessages = true;
      try (var sqsClient = getSqsClient(awsCP)) {
        do {
          // receive 10 messages
          var receiveMessageRequest =
              ReceiveMessageRequest.builder()
                  .waitTimeSeconds(2)
                  .messageAttributeNames("All")
                  .attributeNames(QueueAttributeName.ALL)
                  .queueUrl(qUrl(sqsClient, fromSqs))
                  .maxNumberOfMessages(10)
                  .visibilityTimeout(visibilityTimeout) // default 30 sec
                  .build();
          var response = sqsClient.receiveMessage(receiveMessageRequest);
          if (response.hasMessages()) {
            for (var message : response.messages()) {
              // check each one for selector stuff
              if (message.body().contains(payloadLike)) {
                counter = moveMessage(fromSqs, toSqs, counter, sqsClient, message);
              } else {
                LOG.info("Message does not have contents containing criteria, bypassing it.");
              }
            }
          } else {
            moreMessages = false;
          }
        } while (moreMessages);
      }
      // display summary: num messages checked, num messages moved
      LOG.info("Moved {} messages with payload containing: {}", counter, payloadLike);
    } else {
      counter = -1;
      LOG.info(
          "Queue {} is too deep ({}), for selective message moving, max depth is currently {}.",
          fromSqs,
          depth, maxDepth);
    }
    return counter;
  }

  // todo: this needs a Javadoc
  // todo: I hate that this has a counter arg, it makes it too specialized
  private static int moveMessage(String fromSqs, String toSqs, int counter, SqsClient sqsClient,
      Message message) {
    // if it matches move it and then delete it using the receiptHandle()
    counter++;
    var sendMessageRequest =
        SendMessageRequest.builder()
            .messageBody(message.body())
            .messageAttributes(message.messageAttributes())
            // todo: fix this and everything that touches it. previous line is correct, following 2 commented lines are wrong
//            .messageAttributes(
//                createMessageAttributes(message.attributesAsStrings()))
            // todo: furthermore, look for other usages of createMessageAttributes(message.attributesAsStrings()) should also be changed
            .queueUrl(qUrl(sqsClient, toSqs))
            .build();
    sqsClient.sendMessage(sendMessageRequest);
    var deleteMessageRequest =
        DeleteMessageRequest.builder()
            .queueUrl(qUrl(sqsClient, fromSqs))
            .receiptHandle(message.receiptHandle())
            .build();
    sqsClient.deleteMessage(deleteMessageRequest);
    LOG.info("Moved message #{}", counter);
    return counter;
  }
}
