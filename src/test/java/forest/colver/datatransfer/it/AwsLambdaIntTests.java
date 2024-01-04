package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.LambdaOps.lambdaInvoke;
import static forest.colver.datatransfer.aws.S3Operations.s3Delete;
import static forest.colver.datatransfer.aws.S3Operations.s3Put;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDeleteMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDepth;
import static forest.colver.datatransfer.aws.SqsOperations.sqsPurge;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.Utils.EMX_SANDBOX_TEST_SQS1;
import static forest.colver.datatransfer.aws.Utils.S3_SOURCE_CACHE;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.config.Utils.readFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration Tests for AWS Lambda
 */
class AwsLambdaIntTests {

  private static final Logger LOG = LoggerFactory.getLogger(AwsLambdaIntTests.class);
  private static final String SQS1 = EMX_SANDBOX_TEST_SQS1;
  private static final String FUNCTION = "sftp-sandbox-source-bridge";

  /**
   * Currently creds to run this are obtained with: aws-azure-login --mode=gui --profile
   * enterprise-sb. This test of the Invoke Lambda method is running against the Bridge Lambda, but
   * some setup is required. Essentially the setup invokes the Bridge Lambda, but then that gets
   * cleaned up so the test can manually invoke the Bridge Lambda (again).
   */
  @Test
  void testInvokeLambda() {
    LOG.info("Interacting with: sqs={}", SQS1);
    var creds = getEmxSbCreds();

    // Place object in the s3 cache.
    var objectKey = "ext-aiko1/outbound/dev/flox/dd/1test.txt";
    var contents = readFile("src/test/resources/1test.txt", StandardCharsets.UTF_8);
    s3Put(creds, S3_SOURCE_CACHE, objectKey, contents);

    // wait for the message to get to the sqs
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isOne());

    // The initial placement triggered the bridge lambda, so verify that...
    var messageResp = sqsReadOneMessage(creds, SQS1);
    assert messageResp != null;
    assertThat(messageResp.body()).contains(
        "\"key\": \"ext-aiko1/outbound/dev/flox/dd/1test.txt\"");

    // ...and then clean it up, so we can test invoking the Lambda directly using that object.
    sqsPurge(creds, SQS1);

    // Did all of the above just to set up a situation to invoke a lambda
    // Now invoke the BridgeLambda on the object...
    var payload = readFile("src/test/resources/invokeLambdaReq.json", StandardCharsets.UTF_8);
    var response = lambdaInvoke(creds, FUNCTION, payload);
    assertThat(response.statusCode()).isEqualTo(200);

    // ...wait for the message to get to the sqs...
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(() -> assertThat(sqsDepth(creds, SQS1)).isOne());

    // ...and ensure the SQS got the new message.
    messageResp = sqsReadOneMessage(creds, SQS1);
    assertThat(messageResp.body()).contains(
        "\"key\": \"ext-aiko1/outbound/dev/flox/dd/1test.txt\"");

    // Cleanup the queue and the s3.
    sqsDeleteMessage(creds, SQS1, messageResp);
    s3Delete(creds, S3_SOURCE_CACHE, objectKey);
  }

}
