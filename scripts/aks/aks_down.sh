#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

AKS_RESOURCE_GROUP="${AKS_RESOURCE_GROUP:-ai-trial-rg}"
AKS_CLUSTER_NAME="${AKS_CLUSTER_NAME:-ai-trial-aks}"
NAMESPACE="${NAMESPACE:-odp-dev}"

KUBECONFIG_PATH="${KUBECONFIG_PATH:-${KUBECONFIG:-$HOME/.kube/config}}"

# Terraform-managed infrastructure destruction
TF_DIR="${TF_DIR:-$ROOT_DIR/terraform}"
TF_DESTROY="${TF_DESTROY:-false}"
TF_ENVIRONMENT="${TF_ENVIRONMENT:-dev}"

log() {
  echo "[aks-down] $*"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

kubectl_ctx() {
  kubectl --context "$AKS_CLUSTER_NAME" "$@"
}

require_cmd az
require_cmd kubectl

export KUBECONFIG="$KUBECONFIG_PATH"

AKS_CLUSTER_EXISTS=true
if ! az aks show --resource-group "$AKS_RESOURCE_GROUP" --name "$AKS_CLUSTER_NAME" >/dev/null 2>&1; then
  AKS_CLUSTER_EXISTS=false
  log "AKS cluster '$AKS_CLUSTER_NAME' not found in resource group '$AKS_RESOURCE_GROUP'; skipping in-cluster cleanup steps."
fi

if [[ "$AKS_CLUSTER_EXISTS" == "true" ]]; then
  log "Fetching kubectl credentials for '$AKS_CLUSTER_NAME'..."
  az aks get-credentials \
    --resource-group "$AKS_RESOURCE_GROUP" \
    --name "$AKS_CLUSTER_NAME" \
    --overwrite-existing \
    -o none

  kubectl config use-context "$AKS_CLUSTER_NAME" >/dev/null || true

  log "Deleting application namespace '$NAMESPACE' (workloads, services, ingress, secrets)..."
  kubectl_ctx delete namespace "$NAMESPACE" --ignore-not-found

  log "Deleting ClusterIssuer 'letsencrypt-prod' (so it doesn't get stuck after cert-manager removal)..."
  kubectl_ctx delete clusterissuer letsencrypt-prod --ignore-not-found || true
fi

# ---------------------------------------------------------------------------
# Infrastructure destruction via Terraform
# ---------------------------------------------------------------------------
if [[ "$TF_DESTROY" == "true" ]]; then
  if [[ -d "$TF_DIR" ]] && command -v terraform >/dev/null 2>&1; then
    log "Destroying Terraform-managed infrastructure (AKS cluster, ACR, DNS, Key Vault, etc.)..."
    cd "$TF_DIR"
    terraform init -input=false
    terraform destroy -auto-approve -var-file="environments/${TF_ENVIRONMENT}.tfvars"
  else
    log "ERROR: TF_DESTROY=true but Terraform directory '$TF_DIR' not found or terraform not installed." >&2
    exit 1
  fi
fi

cat <<EOT

AKS teardown completed.

Deleted from cluster (if present):
  - namespace/$NAMESPACE
  - clusterissuer/letsencrypt-prod

To redeploy everything in one go:
  make k8s-aks-up

To destroy all Terraform-managed infrastructure:
  TF_DESTROY=true make k8s-aks-down

  Or directly:
  cd terraform && terraform destroy -var-file=environments/dev.tfvars

EOT
