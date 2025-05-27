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
  region = "us-east-1"
  # To get cred goto myapplications.microsoft.com > AWS - Identity Center - Enterprise >
  # (find the sandbox account) > Access keys > Option 1: Set AWS environment variables >
  # (copy and paste the `export` commands to your cli)
  # shared_credentials_file = "~/.aws/credentials"
  # profile                 = "personal-sandbox"
}
