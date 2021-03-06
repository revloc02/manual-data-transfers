package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.LambdaOps.lambdaInvoke;
import static forest.colver.datatransfer.aws.S3Operations.s3Delete;
import static forest.colver.datatransfer.aws.S3Operations.s3Put;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDelete;
import static forest.colver.datatransfer.aws.SqsOperations.sqsRead;
import static forest.colver.datatransfer.aws.SqsOperations.sqsPurge;
import static forest.colver.datatransfer.aws.Utils.S3_SOURCE_CACHE;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS1;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.config.Utils.readFile;
import static forest.colver.datatransfer.config.Utils.pause;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration Tests for AWS Lambda
 */
public class AwsLambdaIntTests {

  private static final Logger LOG = LoggerFactory.getLogger(AwsLambdaIntTests.class);
  private static final String SQS1 = EMX_SANDBOX_TEST_SQS1;

  /**
   * Currently creds to run this are obtained with: aws-azure-login --mode=gui --profile
   * enterprise-sb
   */
  @Test
  public void testInvokeLambda() {
    var creds = getEmxSbCreds();

    // Place object in the s3 cache.
    var objectKey = "ext-aiko1/outbound/dev/flox/dd/1test.txt";
    var contents = readFile("src/test/resources/1test.txt", StandardCharsets.UTF_8);
    s3Put(creds, S3_SOURCE_CACHE, objectKey, contents);

    // The initial placement triggered the bridge lambda, so verify that...
    var messageResp = sqsRead(creds, SQS1);
    assertThat(messageResp.hasMessages()).isTrue();
    assertThat(messageResp.messages().get(0).body()).contains(
        "\"key\": \"ext-aiko1/outbound/dev/flox/dd/1test.txt\"");

    // ...and then clean it up.
    sqsPurge(creds, SQS1);

    // Now invoke the BridgeLambda on the object...
    var function = "sftpBridgeLambda";
    var payload = readFile("src/test/resources/invokeLambdaReq.json", StandardCharsets.UTF_8);
    var response = lambdaInvoke(creds, function, payload);
    assertThat(response.statusCode()).isEqualTo(200);

    pause(1);

    // ...and ensure the SQS got the new message.
    messageResp = sqsRead(creds, SQS1);
    assertThat(messageResp.hasMessages()).isTrue();
    assertThat(messageResp.messages().get(0).body()).contains(
        "\"key\": \"ext-aiko1/outbound/dev/flox/dd/1test.txt\"");

    // Cleanup the queue and the s3.
    sqsDelete(creds, messageResp, SQS1);
    s3Delete(creds, S3_SOURCE_CACHE, objectKey);
  }

}
