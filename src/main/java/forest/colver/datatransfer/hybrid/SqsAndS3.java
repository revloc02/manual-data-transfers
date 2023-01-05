package forest.colver.datatransfer.hybrid;

import static forest.colver.datatransfer.aws.S3Operations.s3Put;
import static forest.colver.datatransfer.aws.SqsOperations.sqsConsumeOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.Utils.convertSqsMessageAttributesToStrings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class SqsAndS3 {

  private static final Logger LOG = LoggerFactory.getLogger(SqsAndS3.class);

  /**
   * Retrieve next message from an SQS and deliver it to an S3.
   */
  public static void moveOneSqsToS3(
      AwsCredentialsProvider awsCreds, String sqs, String bucket, String objectKey) {
    var sqsMsg = sqsConsumeOneMessage(awsCreds, sqs);
    if (sqsMsg != null) {
      // send body and properties to s3
      s3Put(awsCreds, bucket, objectKey, sqsMsg.body(),
          convertSqsMessageAttributesToStrings(sqsMsg.messageAttributes()));
    } else {
      LOG.error("ERROR: SQS message was null.");
    }
  }

  // todo: this needs a unit test
  /**
   * Copy (read) next message from an SQS and save it to an S3.
   */
  public static void copyOneSqsToS3(
      AwsCredentialsProvider awsCreds, String sqs, String bucket, String objectKey) {
    var response = sqsReadOneMessage(awsCreds, sqs);
    var sqsMsg = response.messages().get(0);
    if (sqsMsg != null) {
      // send body and properties to s3
      s3Put(awsCreds, bucket, objectKey, sqsMsg.body(),
          convertSqsMessageAttributesToStrings(sqsMsg.messageAttributes()));
    } else {
      LOG.error("ERROR: SQS message was null.");
    }
  }

  // todo: need some more methods that transfers data from S3 to SQS and vice versa
}
