// simple topic for testGetSnsTopicAttributes int test
// When you set this up afresh on a new sandbox account, update the ~/.aws/config file with the new sandbox account ID
data "aws_caller_identity" "current" {}
module "basic_topic" {
  source            = "app.terraform.io/ICS/sns-topic/aws"
  version           = "~>1.0"
  name              = "topic_a"
  kms_master_key_id = "alias/aws/sns"

  policy_other_statements = [
    jsonencode({
      Effect : "Allow"
      Principal : {
        AWS : data.aws_caller_identity.current.arn
      }
      Action : "SNS:GetTopicAttributes"
      Resource : "*"
      }
    )
  ]
}

// SQS subscribed to the SNS
module "my_queue" {
  source                        = "app.terraform.io/ICS/sqs/aws"
  version                       = "~>1.0"
  name                          = "sub_demo_adv_queue"
  kms_master_key_id             = module.my_key.arn
  policy_allow_source_arns_send = [module.basic_topic.arn]
}
module "my_key" {
  source                = "app.terraform.io/ICS/kms/aws"
  version               = "~>1.0"
  alias                 = "sub_demo_adv_key"
  description           = "sub_demo_adv_key"
  policy_allow_services = ["sns.amazonaws.com"]
}
module "simple_sub" {
  source    = "app.terraform.io/ICS/sns-subscription/aws"
  version   = "~>1.0"
  topic_arn = module.basic_topic.arn
  protocol  = "sqs"
  endpoint  = module.my_queue.arn
}
