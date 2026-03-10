terraform {
  backend "azurerm" {
    resource_group_name  = "ai-trial-tfstate-rg"
    storage_account_name = "aitrialterraform"
    container_name       = "tfstate"
    key                  = "ai-trial.terraform.tfstate"
  }
}
