terraform {
  required_version = ">= 1.5.0"

  required_providers {
    # Azure provider — active when cloud_provider = "azure"
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    # Scaleway provider — active when cloud_provider = "scaleway"
    scaleway = {
      source  = "scaleway/scaleway"
      version = "~> 2.40"
    }
    # Cloud-agnostic providers (always active)
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.12"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.25"
    }
  }
}
