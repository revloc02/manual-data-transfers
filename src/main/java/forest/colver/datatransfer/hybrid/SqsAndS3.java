package forest.colver.datatransfer.hybrid;

import static forest.colver.datatransfer.aws.S3Operations.s3Delete;
import static forest.colver.datatransfer.aws.S3Operations.s3Get;
import static forest.colver.datatransfer.aws.S3Operations.s3Head;
import static forest.colver.datatransfer.aws.S3Operations.s3Put;
import static forest.colver.datatransfer.aws.SqsOperations.sqsConsumeOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDepth;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsSend;
import static forest.colver.datatransfer.aws.Utils.convertSqsMessageAttributesToStrings;
import static forest.colver.datatransfer.aws.Utils.getS3Client;

import java.io.IOException;
import java.time.Duration;
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

  /**
   * Retrieve an object from S3, check that the size is not too big for SQS, and then place it on an
   * SQS.
   */
  public static void moveS3ObjectToSqs(AwsCredentialsProvider awsCreds, String bucket,
      String objectKey, String sqs) throws IOException {
    try (var s3Client = getS3Client(awsCreds)) {
      // find out how big the object is
      var size = s3Head(s3Client, bucket, objectKey).contentLength();
      LOG.info("Object size: {}", size);
      // SQS The maximum is 262,144 bytes (256 KiB)
      if (size < 262_144) {
        try (var obj = s3Get(s3Client, bucket, objectKey)) {
          sqsSend(awsCreds, sqs, new String(obj.readAllBytes()));
        }
        s3Delete(s3Client, bucket, objectKey);
      } else {
        LOG.error("The S3 object size is greater than 256K, which is too big for SQS, and therefore cannot be moved.");
      }
    }
  }

  // todo: this needs a unit test
  /**
   * Copy an object from S3, check that the size is not too big for SQS, and then place it on an
   * SQS.
   */
  public static void copyS3ObjectToSqs(AwsCredentialsProvider awsCreds, String bucket,
      String objectKey, String sqs) throws IOException {
    try (var s3Client = getS3Client(awsCreds)) {
      // find out how big the object is
      var size = s3Head(s3Client, bucket, objectKey).contentLength();
      LOG.info("Object size: {}", size);
      // SQS The maximum is 262,144 bytes (256 KiB)
      if (size < 262_144) {
        try (var obj = s3Get(s3Client, bucket, objectKey)) {
          sqsSend(awsCreds, sqs, new String(obj.readAllBytes()));
        }
      } else {
        LOG.error("The S3 object size is greater than 256K, which is too big for SQS, and therefore cannot be copied.");
      }
    }
  }
}
