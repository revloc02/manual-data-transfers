package forest.colver.datatransfer.hybrid;

import static forest.colver.datatransfer.aws.S3Operations.s3Put;
import static forest.colver.datatransfer.aws.SqsOperations.sqsConsumeOneMessage;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class SqsAndS3 {

  private static final Logger LOG = LoggerFactory.getLogger(SqsAndS3.class);

  // todo: need some methods that transfers data from S3 to SQS and vice versa
  public static void moveOneSqsToS3(
      AwsCredentialsProvider awsCreds, String sqs, String bucket, String objectKey) {
    var sqsMsg = sqsConsumeOneMessage(awsCreds, sqs);
    if (sqsMsg != null) {
      // send body and properties to s3
      s3Put(awsCreds, bucket, objectKey, sqsMsg.body(), sqsMsg.attributesAsStrings());
    } else {
      LOG.error("ERROR: SQS message was null.");
    }
  }
}
