package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.SnsOperations.getSnsTopicAttributes;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;

import org.junit.jupiter.api.Test;

/**
 * Integration Tests for AWS SNS
 */
public class AwsSnsIntTests {

  /**
   * This tests a very specific use case, access from Enterprise Sandbox account to a topic created
   * on my personal sandbox account. The topic was created with the EIS SNS module and used
   * variable policy_other_statements to grant this specific attributes access.
   */
  @Test
  public void testGetSnsTopicAttributes() {
    // need to refresh Enterprise Sandbox creds to run
    var creds = getEmxSbCreds();
    // personal sandbox SNS topic created from terraform/aws/main.tf
    // refresh personal sandbox creds and then run that Terraform and copy ARN to here
    var topicArn = "arn:aws:sns:us-east-1:130968167839:topic_a";
    getSnsTopicAttributes(creds, topicArn);
  }
}
