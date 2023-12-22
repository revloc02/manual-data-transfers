package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.SnsOperations.getSnsTopicAttributes;
import static forest.colver.datatransfer.aws.SnsOperations.publishTopic;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDeleteMessage;
import static forest.colver.datatransfer.aws.SqsOperations.sqsDepth;
import static forest.colver.datatransfer.aws.SqsOperations.sqsReadOneMessage;
import static forest.colver.datatransfer.aws.Utils.PERSONAL_SANDBOX_SQS_SUB_SNS;
import static forest.colver.datatransfer.aws.Utils.PERSONAL_SANDBOX_TEST_SNS_TOPIC_ARN;
import static forest.colver.datatransfer.aws.Utils.getPersonalSbCreds;
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
class AwsSnsIntTests {

  private static final Logger LOG = LoggerFactory.getLogger(AwsSnsIntTests.class);
  // Watch out! This changes frequently, update the ~/.aws/config file to your latest sandbox account
  private static final String SNS_ARN_TEMP = PERSONAL_SANDBOX_TEST_SNS_TOPIC_ARN;
  private static final String SQS = PERSONAL_SANDBOX_SQS_SUB_SNS;

  /**
   * This tests a very specific use case: grants access from Enterprise Sandbox account to a topic
   * that exists in my personal sandbox account. The topic was created with the EIS SNS module and
   * used variable policy_other_statements to grant this specific attributes access.
   */
  @Test
  void testGetSnsTopicAttributes() {
    // refresh personal sandbox creds
    // run TF in terraform/aws/main.tf to create personal sandbox SNS topic
    // copy the created SNS ARN to .aws/credentials.properties

    // need to refresh Enterprise Sandbox creds to run and test access from that external account
    var attributes = getSnsTopicAttributes(getPersonalSbCreds(), SNS_ARN_TEMP);
    assertThat(attributes).isNotEmpty();
  }

  /**
   * Tests publishTopic. Requires Terraform to be run in advance in a personal AWS sandbox.
   */
  @Test
  void testSnsPublishTopic() {
    LOG.info("SNS ARN: {}", SNS_ARN_TEMP);
    var creds = getPersonalSbCreds(); // runs in a personal sandbox
    // publish message
    var message = "testing SNS publish topic via SQS subscription";
    publishTopic(getPersonalSbCreds(), SNS_ARN_TEMP, message);

    // await its arrival in the queue
    await()
        .pollInterval(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(60))
        .until(() -> sqsDepth(creds, SQS) >= 1);

    // check the data
    var msg = sqsReadOneMessage(creds, SQS);
    LOG.info("msg.body():{}", msg.body());
    var jo = new JSONObject(msg.body());
    assertThat(jo.getString("Type")).isEqualTo("Notification");
    assertThat(jo.getString("TopicArn")).isEqualTo(SNS_ARN_TEMP);
    assertThat(jo.getString("Message")).isEqualTo(message);

    // cleanup
    sqsDeleteMessage(creds, SQS, msg);
  }
}
