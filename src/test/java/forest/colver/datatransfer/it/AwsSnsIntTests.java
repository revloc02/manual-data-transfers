package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.SnsOperations.getSnsTopicAttributes;
import static forest.colver.datatransfer.aws.Utils.getSbCreds;

import org.junit.jupiter.api.Test;

/**
 * Integration Tests for AWS SNS
 */
public class AwsSnsIntTests {

  /**
   * This tests a very specific use case, access from Enterprise Sandbox account to a topic created
   * on my personal sandbox account. The topic was created with the EIS SNS module and used
   * variables defined there to grant this specific access.
   */
  @Test
  public void testGetSnsTopicAttributes() {
    // place some messages
    var creds = getSbCreds();
    var topicArn = "arn:aws:sns:us-east-1:308660710587:topic_a"; // personal sandbox topic
    getSnsTopicAttributes(creds, topicArn);
  }
}
