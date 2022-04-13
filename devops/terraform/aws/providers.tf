terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">=2.0,<4.0"
    }
  }
  required_version = ">= 0.14.11,< 1.1"
}
provider "aws" {
  region                  = "us-east-1"
  shared_credentials_file = "~/.aws/credentials"
  profile                 = "personal-sandbox"
}
