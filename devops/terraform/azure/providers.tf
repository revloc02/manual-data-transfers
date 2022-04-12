// Azure individual sandbox
provider "azurerm" {
  subscription_id            = "f4340665-3df8-41fb-9d50-eff048439979"
  skip_provider_registration = true
  features {}
}

//provider "azurerm" {
//  alias                      = "log_subscription"
//  subscription_id            = "b0d46910-956d-4f5e-afe5-c79784ac5af5"
//  skip_provider_registration = true
//  features {}
//}

//terraform {
//  backend "azurerm" {
//    #Please leave this empty
//  }
//}