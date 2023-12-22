terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">=2.0,<5.0"
    }
  }
  required_version = ">= 1.0,< 2.0"
}
provider "aws" {
  region                  = "us-east-1"
  shared_credentials_file = "~/.aws/credentials"
  profile                 = "personal-sandbox"
}
