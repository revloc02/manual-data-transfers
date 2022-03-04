// simple topic for testGetSnsTopicAttributes int test
module "basic_topic" {
  source            = "app.terraform.io/ICS/sns-topic/aws"
  version           = "1.5.0"
  name              = "topic_a"
  kms_master_key_id = "alias/aws/sns"

  policy_other_statements = [
    jsonencode({
      Effect : "Allow"
      Principal : {
        AWS : "arn:aws:iam::646129096172:root"
      }
      Action : "SNS:GetTopicAttributes"
      Resource : "*"
      }
    )
  ]
}