# To get cred goto myapplications.microsoft.com > AWS - Identity Center - Enterprise >
# (find the sandbox account) > Access keys > Option 1: Set AWS environment variables >
# (copy and paste the `export` commands to your cli)

data "aws_caller_identity" "current" {}
module "basic_topic" {
  source            = "app.terraform.io/ICS/sns-topic/aws"
  version           = "~>2.0"
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
  source                  = "app.terraform.io/ICS/sqs/aws"
  version                 = "~>2.0"
  name                    = "sub_demo_adv_queue"
  # kms_master_key_create = true
  sqs_managed_sse_enabled = true
  sns_topic_subscriptions = [
    {
      topic_arn            = module.basic_topic.arn
      raw_message_delivery = true
      filter_policy        = null
    }
  ]
}

module "kms_key" {
  source  = "app.terraform.io/ICS/kms/aws"
  version = "~>2.0"
  alias   = "test-key"
}
module "queue_2" {
  source                  = "app.terraform.io/ICS/sqs/aws"
  version                 = "~>2.0"
  name                    = "test-queue-2"
  sqs_managed_sse_enabled = true
}
