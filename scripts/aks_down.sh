#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

AKS_LOCATION="${AKS_LOCATION:-westeurope}"
AKS_RESOURCE_GROUP="${AKS_RESOURCE_GROUP:-ai-trial-rg}"
AKS_CLUSTER_NAME="${AKS_CLUSTER_NAME:-ai-trial-aks}"
NAMESPACE="${NAMESPACE:-odp-dev}"

FRONTEND_DOMAIN="${FRONTEND_DOMAIN:-eu-sovereigndataplatform.com}"
DNS_RESOURCE_GROUP="${DNS_RESOURCE_GROUP:-$AKS_RESOURCE_GROUP}"
INGRESS_PIP_NAME="${INGRESS_PIP_NAME:-ai-trial-ingress-pip}"
INGRESS_NGINX_VERSION="${INGRESS_NGINX_VERSION:-controller-v1.14.3}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.19.3}"

# Destructive flags (all default to false)
DELETE_AKS_CLUSTER="${DELETE_AKS_CLUSTER:-false}"
DELETE_ACR="${DELETE_ACR:-false}"
DELETE_INGRESS_PIP="${DELETE_INGRESS_PIP:-false}"
DELETE_DNS_RECORDS="${DELETE_DNS_RECORDS:-false}"
DELETE_RESOURCE_GROUP="${DELETE_RESOURCE_GROUP:-false}"

KUBECONFIG_PATH="${KUBECONFIG_PATH:-${KUBECONFIG:-$HOME/.kube/config}}"

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

if ! az aks show --resource-group "$AKS_RESOURCE_GROUP" --name "$AKS_CLUSTER_NAME" >/dev/null 2>&1; then
  log "AKS cluster '$AKS_CLUSTER_NAME' not found in resource group '$AKS_RESOURCE_GROUP'. Nothing to do."
  exit 0
fi

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

log "Removing ingress-nginx ($INGRESS_NGINX_VERSION)..."
kubectl_ctx delete -f "https://raw.githubusercontent.com/kubernetes/ingress-nginx/${INGRESS_NGINX_VERSION}/deploy/static/provider/cloud/deploy.yaml" --ignore-not-found || true
kubectl_ctx delete namespace ingress-nginx --ignore-not-found || true

log "Removing cert-manager ($CERT_MANAGER_VERSION)..."
kubectl_ctx delete -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml" --ignore-not-found || true
kubectl_ctx delete namespace cert-manager --ignore-not-found || true

if [[ "$DELETE_DNS_RECORDS" == "true" ]]; then
  log "Deleting DNS records in zone '$FRONTEND_DOMAIN' (resource group '$DNS_RESOURCE_GROUP')..."
  az network dns record-set a delete --resource-group "$DNS_RESOURCE_GROUP" --zone-name "$FRONTEND_DOMAIN" --name "@" --yes 2>/dev/null || true
  az network dns record-set cname delete --resource-group "$DNS_RESOURCE_GROUP" --zone-name "$FRONTEND_DOMAIN" --name "www" --yes 2>/dev/null || true
fi

if [[ "$DELETE_INGRESS_PIP" == "true" ]]; then
  NODE_RESOURCE_GROUP="$(az aks show --resource-group "$AKS_RESOURCE_GROUP" --name "$AKS_CLUSTER_NAME" --query nodeResourceGroup -o tsv)"
  log "Deleting ingress Public IP '$INGRESS_PIP_NAME' from node resource group '$NODE_RESOURCE_GROUP'..."
  az network public-ip delete --resource-group "$NODE_RESOURCE_GROUP" --name "$INGRESS_PIP_NAME" 2>/dev/null || true
fi

if [[ "$DELETE_AKS_CLUSTER" == "true" ]]; then
  log "Deleting AKS cluster '$AKS_CLUSTER_NAME' (this can take several minutes)..."
  az aks delete --resource-group "$AKS_RESOURCE_GROUP" --name "$AKS_CLUSTER_NAME" --yes -o none
fi

if [[ "$DELETE_ACR" == "true" ]]; then
  SUBSCRIPTION_ID="$(az account show --query id -o tsv)"
  SUB_HASH="$(echo "$SUBSCRIPTION_ID" | tr -d '-' | cut -c1-8)"
  ACR_NAME="${ACR_NAME:-aitrial${SUB_HASH}}"
  log "Deleting ACR '$ACR_NAME'..."
  az acr delete --resource-group "$AKS_RESOURCE_GROUP" --name "$ACR_NAME" --yes -o none
fi

if [[ "$DELETE_RESOURCE_GROUP" == "true" ]]; then
  log "Deleting resource group '$AKS_RESOURCE_GROUP' (WARNING: this also deletes DNS zones and any domain resources in the RG)..."
  az group delete --name "$AKS_RESOURCE_GROUP" --yes --no-wait
fi

cat <<EOT

AKS teardown completed.

Deleted from cluster (if present):
  - namespace/$NAMESPACE
  - clusterissuer/letsencrypt-prod
  - namespace/ingress-nginx
  - namespace/cert-manager

To redeploy everything in one go:
  make k8s-aks-up

Optional destructive flags (set to true when running this script):
  DELETE_AKS_CLUSTER=true        # deletes AKS cluster
  DELETE_ACR=true                # deletes ACR in $AKS_RESOURCE_GROUP
  DELETE_INGRESS_PIP=true        # deletes the ingress public IP in the node RG
  DELETE_DNS_RECORDS=true        # removes A/@ and CNAME/www from Azure DNS zone
  DELETE_RESOURCE_GROUP=true     # deletes $AKS_RESOURCE_GROUP (DANGEROUS)

EOT
