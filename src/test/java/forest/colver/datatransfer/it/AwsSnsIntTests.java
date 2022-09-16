package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.SnsOperations.getSnsTopicAttributes;
import static forest.colver.datatransfer.aws.SnsOperations.publishTopic;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDelete;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDepth;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.Utils.PERSONAL_SANDBOX_SQS_SUB_SNS;
import static forest.colver.datatransfer.aws.Utils.PERSONAL_SANDBOX_TEST_SNS_TOPIC_ARN;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.aws.Utils.getPrsnlSbCreds;
import static forest.colver.datatransfer.config.Utils.pause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration Tests for AWS SNS
 */
public class AwsSnsIntTests {

  private static final Logger LOG = LoggerFactory.getLogger(AwsSnsIntTests.class);
  private static final String SNS_ARN = PERSONAL_SANDBOX_TEST_SNS_TOPIC_ARN;
  private static final String SQS = PERSONAL_SANDBOX_SQS_SUB_SNS;

  /**
   * This tests a very specific use case: grants access from Enterprise Sandbox account to a topic
   * that exists in my personal sandbox account. The topic was created with the EIS SNS module and
   * used variable policy_other_statements to grant this specific attributes access.
   */
  @Test
  public void testGetSnsTopicAttributes() {
    // refresh personal sandbox creds
    // run TF in terraform/aws/main.tf to create personal sandbox SNS topic
    // copy the created SNS ARN to .aws/credentials.properties
    getSnsTopicAttributes(getPrsnlSbCreds(), SNS_ARN);
  }

  @Test
  public void testSnsPublishTopic() {
    LOG.info("SNS ARN: {}", SNS_ARN);
    var creds = getPrsnlSbCreds();
    var message = "testing SNS publish topic via SQS subscription";
    publishTopic(getPrsnlSbCreds(), SNS_ARN, message);

    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .until(() -> sqsDepth(creds, SQS) >= 1);

    // check that it arrived
    var response = sqsReadOneMessage(creds, SQS);
    LOG.info("response.messages().get(0).body():{}", response.messages().get(0).body());
    var jo = new JSONObject(response.messages().get(0).body());
    assertThat(jo.getString("Type")).isEqualTo("Notification");
    assertThat(jo.getString("TopicArn")).isEqualTo(SNS_ARN);
    assertThat(jo.getString("Message")).isEqualTo(message);
    // cleanup
    sqsDelete(creds, response, SQS);
  }
}
