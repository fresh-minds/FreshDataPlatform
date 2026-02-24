#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

AKS_RESOURCE_GROUP="${AKS_RESOURCE_GROUP:-ai-trial-rg}"
AKS_CLUSTER_NAME="${AKS_CLUSTER_NAME:-ai-trial-aks}"
NAMESPACE="${NAMESPACE:-odp-dev}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-${KUBECONFIG:-$HOME/.kube/config}}"

AIRFLOW_IMAGE_REPO="${AIRFLOW_IMAGE_REPO:-ai-trial/airflow}"
AIRFLOW_IMAGE_TAG="${AIRFLOW_IMAGE_TAG:-dev-$(date +%Y%m%d%H%M%S)}"
FRONTEND_IMAGE_REPO="${FRONTEND_IMAGE_REPO:-ai-trial/frontend}"
FRONTEND_IMAGE_TAG="${FRONTEND_IMAGE_TAG:-frontend-$(date +%Y%m%d%H%M%S)}"
PORTAL_API_IMAGE_REPO="${PORTAL_API_IMAGE_REPO:-ai-trial/portal-api}"
PORTAL_API_IMAGE_TAG="${PORTAL_API_IMAGE_TAG:-portal-api-$(date +%Y%m%d%H%M%S)}"
JUPYTER_IMAGE_REPO="${JUPYTER_IMAGE_REPO:-ai-trial/jupyter}"
JUPYTER_IMAGE_TAG="${JUPYTER_IMAGE_TAG:-jupyter-$(date +%Y%m%d%H%M%S)}"
MINIO_SSO_BRIDGE_IMAGE_REPO="${MINIO_SSO_BRIDGE_IMAGE_REPO:-ai-trial/minio-sso-bridge}"
MINIO_SSO_BRIDGE_IMAGE_TAG="${MINIO_SSO_BRIDGE_IMAGE_TAG:-minio-sso-bridge-$(date +%Y%m%d%H%M%S)}"

AKS_IMAGES="${AKS_IMAGES:-airflow,frontend,portal-api,jupyter,minio-sso-bridge}"
AKS_IMAGE_UPDATE_ROLLOUT_TIMEOUT="${AKS_IMAGE_UPDATE_ROLLOUT_TIMEOUT:-600s}"
AKS_WAIT_RETRIES="${AKS_WAIT_RETRIES:-6}"
AKS_WAIT_RETRY_DELAY_SECONDS="${AKS_WAIT_RETRY_DELAY_SECONDS:-10}"

AKS_HELPERS_LIB="$ROOT_DIR/scripts/aks/aks_up_lib.sh"

log() {
  echo "[aks-update-images] $*"
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

if [[ ! -f "$AKS_HELPERS_LIB" ]]; then
  echo "Missing AKS helper library: $AKS_HELPERS_LIB" >&2
  exit 1
fi

# shellcheck source=scripts/aks/aks_up_lib.sh
source "$AKS_HELPERS_LIB"

require_cmd az
require_cmd kubectl
require_cmd docker

SUBSCRIPTION_ID="$(az account show --query id -o tsv)"
SUB_HASH="$(echo "$SUBSCRIPTION_ID" | tr -d '-' | cut -c1-8)"
ACR_NAME="${ACR_NAME:-aitrial${SUB_HASH}}"

AIRFLOW_IMAGE="${ACR_NAME}.azurecr.io/${AIRFLOW_IMAGE_REPO}:${AIRFLOW_IMAGE_TAG}"
FRONTEND_IMAGE="${ACR_NAME}.azurecr.io/${FRONTEND_IMAGE_REPO}:${FRONTEND_IMAGE_TAG}"
PORTAL_API_IMAGE="${ACR_NAME}.azurecr.io/${PORTAL_API_IMAGE_REPO}:${PORTAL_API_IMAGE_TAG}"
JUPYTER_IMAGE="${ACR_NAME}.azurecr.io/${JUPYTER_IMAGE_REPO}:${JUPYTER_IMAGE_TAG}"
MINIO_SSO_BRIDGE_IMAGE="${ACR_NAME}.azurecr.io/${MINIO_SSO_BRIDGE_IMAGE_REPO}:${MINIO_SSO_BRIDGE_IMAGE_TAG}"

export KUBECONFIG="$KUBECONFIG_PATH"

log "Using Azure subscription: $(az account show --query name -o tsv)"
log "Fetching AKS credentials for '$AKS_CLUSTER_NAME'..."
az aks get-credentials \
  --resource-group "$AKS_RESOURCE_GROUP" \
  --name "$AKS_CLUSTER_NAME" \
  --overwrite-existing \
  -o none

kubectl config use-context "$AKS_CLUSTER_NAME" >/dev/null

log "Logging in to ACR '$ACR_NAME'..."
az acr login --name "$ACR_NAME" -o none

contains_image() {
  local needle="$1"
  [[ ",${AKS_IMAGES}," == *",${needle},"* ]]
}

build_image_for_service() {
  local service="$1"

  case "$service" in
    airflow)
      build_and_push_image "$AIRFLOW_IMAGE" "$ROOT_DIR/airflow/Dockerfile" "$ROOT_DIR" "Airflow"
      ;;
    frontend)
      build_and_push_image "$FRONTEND_IMAGE" "$ROOT_DIR/frontend/Dockerfile.k8s" "$ROOT_DIR/frontend" "Frontend"
      ;;
    portal-api)
      build_and_push_image "$PORTAL_API_IMAGE" "$ROOT_DIR/ops/portal-api/Dockerfile" "$ROOT_DIR" "Portal API"
      ;;
    jupyter)
      build_and_push_image "$JUPYTER_IMAGE" "$ROOT_DIR/notebooks/Dockerfile" "$ROOT_DIR/notebooks" "Jupyter"
      ;;
    minio-sso-bridge)
      build_and_push_image "$MINIO_SSO_BRIDGE_IMAGE" "$ROOT_DIR/ops/minio-sso-bridge/Dockerfile" "$ROOT_DIR" "MinIO SSO bridge"
      ;;
    *)
      echo "Unsupported service in AKS_IMAGES: '$service'" >&2
      return 1
      ;;
  esac
}

update_deployment_for_service() {
  local service="$1"

  case "$service" in
    airflow)
      set_deployment_image_and_wait airflow-webserver airflow-webserver "$AIRFLOW_IMAGE"
      set_deployment_image_and_wait airflow-scheduler airflow-scheduler "$AIRFLOW_IMAGE"
      ;;
    frontend)
      set_deployment_image_and_wait portal portal "$FRONTEND_IMAGE"
      ;;
    portal-api)
      set_deployment_image_and_wait portal-api portal-api "$PORTAL_API_IMAGE"
      ;;
    jupyter)
      set_deployment_image_and_wait jupyter jupyter "$JUPYTER_IMAGE"
      ;;
    minio-sso-bridge)
      set_deployment_image_and_wait minio-sso-bridge minio-sso-bridge "$MINIO_SSO_BRIDGE_IMAGE"
      ;;
    *)
      echo "Unsupported service in AKS_IMAGES: '$service'" >&2
      return 1
      ;;
  esac
}

run_for_selected_services() {
  local callback="$1"
  local -a selected_services
  local service

  IFS=',' read -r -a selected_services <<< "$AKS_IMAGES"
  for service in "${selected_services[@]}"; do
    service="$(echo "$service" | xargs)"
    [[ -z "$service" ]] && continue
    "$callback" "$service"
  done
}

set_deployment_image_and_wait() {
  local deployment="$1"
  local container="$2"
  local image="$3"
  local resolved_container="$container"
  local containers
  local container_count

  if ! kubectl_ctx -n "$NAMESPACE" get deployment "$deployment" >/dev/null 2>&1; then
    log "Skipping deployment '$deployment' (not found in namespace '$NAMESPACE')."
    return 0
  fi

  containers="$(kubectl_ctx -n "$NAMESPACE" get deployment "$deployment" -o jsonpath='{.spec.template.spec.containers[*].name}')"
  if [[ " $containers " != *" $container "* ]]; then
    container_count="$(wc -w <<<"$containers" | tr -d ' ')"
    if [[ "$container_count" == "1" ]]; then
      resolved_container="$containers"
      log "Container '$container' not found in deployment/$deployment; using only container '$resolved_container'."
    else
      echo "Could not resolve container '$container' for deployment/$deployment. Available containers: $containers" >&2
      return 1
    fi
  fi

  log "Updating deployment/$deployment container '$resolved_container' -> '$image'"
  kubectl_ctx -n "$NAMESPACE" set image "deployment/${deployment}" "${resolved_container}=${image}"
  wait_for_deployment "$deployment" "$AKS_IMAGE_UPDATE_ROLLOUT_TIMEOUT"
}

log "Building and pushing selected images: ${AKS_IMAGES}"
run_for_selected_services build_image_for_service

log "Updating AKS deployments with latest image tags..."
run_for_selected_services update_deployment_for_service

cat <<EOT

AKS image-only update complete.

Namespace:       $NAMESPACE
Cluster:         $AKS_CLUSTER_NAME
Updated images:  $AKS_IMAGES
Airflow image:   $AIRFLOW_IMAGE
Frontend image:  $FRONTEND_IMAGE
Portal API image:$PORTAL_API_IMAGE
Jupyter image:   $JUPYTER_IMAGE
Bridge image:    $MINIO_SSO_BRIDGE_IMAGE

EOT
