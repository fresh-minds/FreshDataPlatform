#!/usr/bin/env bash
set -euo pipefail

# One-time bootstrap: create Azure Storage Account for Terraform remote state.
# Run this before the first `terraform init`.

TFSTATE_RG="${TFSTATE_RG:-ai-trial-tfstate-rg}"
TFSTATE_SA="${TFSTATE_SA:-aitrialterraform}"
TFSTATE_CONTAINER="${TFSTATE_CONTAINER:-tfstate}"
LOCATION="${LOCATION:-westeurope}"

log() {
  echo "[tf-bootstrap] $*"
}

log "Creating resource group '$TFSTATE_RG' in '$LOCATION'..."
az group create \
  --name "$TFSTATE_RG" \
  --location "$LOCATION" \
  -o none

log "Creating storage account '$TFSTATE_SA'..."
az storage account create \
  --name "$TFSTATE_SA" \
  --resource-group "$TFSTATE_RG" \
  --location "$LOCATION" \
  --sku Standard_LRS \
  --min-tls-version TLS1_2 \
  --allow-blob-public-access false \
  -o none

log "Creating blob container '$TFSTATE_CONTAINER'..."
az storage container create \
  --name "$TFSTATE_CONTAINER" \
  --account-name "$TFSTATE_SA" \
  -o none

cat <<EOT

Terraform state backend is ready.

  Resource Group:  $TFSTATE_RG
  Storage Account: $TFSTATE_SA
  Container:       $TFSTATE_CONTAINER

Configure in terraform/backend.tf:

  terraform {
    backend "azurerm" {
      resource_group_name  = "$TFSTATE_RG"
      storage_account_name = "$TFSTATE_SA"
      container_name       = "$TFSTATE_CONTAINER"
      key                  = "ai-trial.terraform.tfstate"
    }
  }

EOT
