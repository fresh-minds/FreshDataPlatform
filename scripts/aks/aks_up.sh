#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

AKS_LOCATION="${AKS_LOCATION:-westeurope}"
AKS_RESOURCE_GROUP="${AKS_RESOURCE_GROUP:-ai-trial-rg}"
AKS_CLUSTER_NAME="${AKS_CLUSTER_NAME:-ai-trial-aks}"
AKS_NODE_COUNT="${AKS_NODE_COUNT:-1}"
AKS_NODE_VM_SIZE="${AKS_NODE_VM_SIZE:-Standard_B2s}"
NAMESPACE="${NAMESPACE:-odp-dev}"
AIRFLOW_IMAGE_REPO="${AIRFLOW_IMAGE_REPO:-ai-trial/airflow}"
AIRFLOW_IMAGE_TAG="${AIRFLOW_IMAGE_TAG:-dev-$(date +%Y%m%d%H%M%S)}"
FRONTEND_IMAGE_REPO="${FRONTEND_IMAGE_REPO:-ai-trial/frontend}"
FRONTEND_IMAGE_TAG="${FRONTEND_IMAGE_TAG:-frontend-$(date +%Y%m%d%H%M%S)}"
FRONTEND_DOMAIN="${FRONTEND_DOMAIN:-eu-sovereigndataplatform.com}"
DNS_RESOURCE_GROUP="${DNS_RESOURCE_GROUP:-$AKS_RESOURCE_GROUP}"
LETSENCRYPT_EMAIL="${LETSENCRYPT_EMAIL:-karel.goense@freshminds.nl}"
INGRESS_PIP_NAME="${INGRESS_PIP_NAME:-ai-trial-ingress-pip}"
INGRESS_NGINX_VERSION="${INGRESS_NGINX_VERSION:-controller-v1.14.3}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.19.3}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300s}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-${KUBECONFIG:-$HOME/.kube/config}}"
AKS_FORCE_ATTACH_ACR="${AKS_FORCE_ATTACH_ACR:-false}"

log() {
  echo "[aks-up] $*"
}

kubectl_ctx() {
  kubectl --context "$AKS_CLUSTER_NAME" "$@"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

wait_for_job_complete() {
  local job_name="$1"
  local timeout="${2:-300s}"

  if kubectl_ctx -n "$NAMESPACE" wait --for=condition=complete "job/${job_name}" --timeout="$timeout"; then
    return 0
  fi

  echo "[aks-up] Job '${job_name}' did not complete within ${timeout}. Dumping diagnostics..." >&2
  kubectl_ctx -n "$NAMESPACE" describe "job/${job_name}" >&2 || true

  local pods
  pods="$(kubectl_ctx -n "$NAMESPACE" get pods -l "job-name=${job_name}" -o name 2>/dev/null || true)"
  if [[ -z "${pods}" ]]; then
    echo "[aks-up] No pods found for job '${job_name}'." >&2
    return 1
  fi

  while IFS= read -r pod; do
    [[ -z "$pod" ]] && continue
    kubectl_ctx -n "$NAMESPACE" describe "$pod" >&2 || true
    kubectl_ctx -n "$NAMESPACE" logs "$pod" --all-containers=true --tail=200 >&2 || true
    kubectl_ctx -n "$NAMESPACE" logs "$pod" --all-containers=true --previous --tail=200 >&2 || true
  done <<< "$pods"

  return 1
}

render_and_apply() {
  local manifest="$1"
  sed \
    -e "s|__NAMESPACE__|${NAMESPACE}|g" \
    -e "s|__AIRFLOW_IMAGE__|${AIRFLOW_IMAGE}|g" \
    -e "s|__FRONTEND_IMAGE__|${FRONTEND_IMAGE}|g" \
    -e "s|__FRONTEND_DOMAIN__|${FRONTEND_DOMAIN}|g" \
    -e "s|__LETSENCRYPT_EMAIL__|${LETSENCRYPT_EMAIL}|g" \
    "$manifest" | kubectl_ctx apply -f -
}

require_cmd az
require_cmd kubectl
require_cmd docker
require_cmd curl
require_cmd openssl

AKS_SKIP_OPENAPI_VALIDATE="${AKS_SKIP_OPENAPI_VALIDATE:-false}"

kubectl_ctx_apply() {
  if [[ "$AKS_SKIP_OPENAPI_VALIDATE" == "true" ]]; then
    kubectl_ctx apply --validate=false -f "$@"
  else
    kubectl_ctx apply -f "$@"
  fi
}

check_kube_api() {
  if kubectl_ctx get --raw='/readyz' >/dev/null 2>&1; then
    return 0
  fi

  echo "[aks-up] ERROR: Kubernetes API endpoint is unreachable from this machine." >&2
  echo "[aks-up] If this is a private AKS cluster, connect to the VNet (VPN/bastion) or ensure private DNS resolves the API server." >&2
  echo "[aks-up] You can set AKS_SKIP_OPENAPI_VALIDATE=true to skip client-side OpenAPI validation once connectivity is fixed." >&2
  return 1
}

if [[ ! -f "$ROOT_DIR/.env" ]]; then
  echo "Missing $ROOT_DIR/.env. Create it first (for example: cp .env.template .env)." >&2
  exit 1
fi

SUBSCRIPTION_ID="$(az account show --query id -o tsv)"
SUB_HASH="$(echo "$SUBSCRIPTION_ID" | tr -d '-' | cut -c1-8)"
ACR_NAME="${ACR_NAME:-aitrial${SUB_HASH}}"
AIRFLOW_IMAGE="${ACR_NAME}.azurecr.io/${AIRFLOW_IMAGE_REPO}:${AIRFLOW_IMAGE_TAG}"
FRONTEND_IMAGE="${ACR_NAME}.azurecr.io/${FRONTEND_IMAGE_REPO}:${FRONTEND_IMAGE_TAG}"
export KUBECONFIG="$KUBECONFIG_PATH"

log "Using Azure subscription: $(az account show --query name -o tsv)"
log "Creating/updating resource group '$AKS_RESOURCE_GROUP' in '$AKS_LOCATION'..."
az group create --name "$AKS_RESOURCE_GROUP" --location "$AKS_LOCATION" -o none

if az acr show --name "$ACR_NAME" >/dev/null 2>&1; then
  log "ACR '$ACR_NAME' already exists."
else
  log "Creating ACR '$ACR_NAME'..."
  az acr create --resource-group "$AKS_RESOURCE_GROUP" --name "$ACR_NAME" --sku Basic -o none
fi

if az aks show --resource-group "$AKS_RESOURCE_GROUP" --name "$AKS_CLUSTER_NAME" >/dev/null 2>&1; then
  log "AKS cluster '$AKS_CLUSTER_NAME' already exists."
else
  log "Creating AKS cluster '$AKS_CLUSTER_NAME' (this can take several minutes)..."
  az aks create \
    --resource-group "$AKS_RESOURCE_GROUP" \
    --name "$AKS_CLUSTER_NAME" \
    --location "$AKS_LOCATION" \
    --node-count "$AKS_NODE_COUNT" \
    --node-vm-size "$AKS_NODE_VM_SIZE" \
    --tier free \
    --enable-managed-identity \
    --generate-ssh-keys \
    --attach-acr "$ACR_NAME" \
    -o none
fi

if [[ "$AKS_FORCE_ATTACH_ACR" == "true" ]]; then
  log "Ensuring AKS cluster can pull from ACR '$ACR_NAME'..."
  az aks update \
    --resource-group "$AKS_RESOURCE_GROUP" \
    --name "$AKS_CLUSTER_NAME" \
    --attach-acr "$ACR_NAME" \
    -o none
fi

mkdir -p "$(dirname "$KUBECONFIG_PATH")"
if [[ ! -f "$KUBECONFIG_PATH" ]]; then
  cat > "$KUBECONFIG_PATH" <<'EOC'
apiVersion: v1
kind: Config
clusters: []
contexts: []
current-context: ""
users: []
EOC
  chmod 600 "$KUBECONFIG_PATH"
elif ! grep -q '^clusters:' "$KUBECONFIG_PATH"; then
  backup_path="${KUBECONFIG_PATH}.bak.$(date +%Y%m%d%H%M%S)"
  cp "$KUBECONFIG_PATH" "$backup_path"
  log "Backed up invalid kubeconfig to '$backup_path'."
  cat > "$KUBECONFIG_PATH" <<'EOC'
apiVersion: v1
kind: Config
clusters: []
contexts: []
current-context: ""
users: []
EOC
  chmod 600 "$KUBECONFIG_PATH"
fi

log "Fetching kubectl credentials for '$AKS_CLUSTER_NAME'..."
az aks get-credentials \
  --resource-group "$AKS_RESOURCE_GROUP" \
  --name "$AKS_CLUSTER_NAME" \
  --overwrite-existing \
  -o none

kubectl config use-context "$AKS_CLUSTER_NAME" >/dev/null

NODE_RESOURCE_GROUP="$(az aks show --resource-group "$AKS_RESOURCE_GROUP" --name "$AKS_CLUSTER_NAME" --query nodeResourceGroup -o tsv)"

log "Ensuring static Public IP '$INGRESS_PIP_NAME' exists in node resource group '$NODE_RESOURCE_GROUP'..."
if az network public-ip show --resource-group "$NODE_RESOURCE_GROUP" --name "$INGRESS_PIP_NAME" >/dev/null 2>&1; then
  log "Public IP '$INGRESS_PIP_NAME' already exists."
else
  az network public-ip create \
    --resource-group "$NODE_RESOURCE_GROUP" \
    --name "$INGRESS_PIP_NAME" \
    --location "$AKS_LOCATION" \
    --sku Standard \
    --allocation-method Static \
    -o none
fi

INGRESS_PIP_IP="$(az network public-ip show --resource-group "$NODE_RESOURCE_GROUP" --name "$INGRESS_PIP_NAME" --query ipAddress -o tsv)"
log "Ingress Public IP: $INGRESS_PIP_IP"

log "Installing/upgrading ingress-nginx ($INGRESS_NGINX_VERSION)..."
check_kube_api
kubectl_ctx_apply "https://raw.githubusercontent.com/kubernetes/ingress-nginx/${INGRESS_NGINX_VERSION}/deploy/static/provider/cloud/deploy.yaml"

log "Configuring ingress-nginx service to use Public IP '$INGRESS_PIP_NAME'..."
for _ in {1..60}; do
  if kubectl_ctx -n ingress-nginx get svc ingress-nginx-controller >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

kubectl_ctx -n ingress-nginx patch svc ingress-nginx-controller --type merge -p "$(cat <<EOF
{
  "metadata": {
    "annotations": {
      "service.beta.kubernetes.io/azure-load-balancer-resource-group": "${NODE_RESOURCE_GROUP}",
      "service.beta.kubernetes.io/azure-pip-name": "${INGRESS_PIP_NAME}"
    }
  }
}
EOF
)"

log "Waiting for ingress-nginx controller to be ready..."
kubectl_ctx -n ingress-nginx rollout status deployment/ingress-nginx-controller --timeout="$WAIT_TIMEOUT"

log "Waiting for ingress-nginx external IP to match $INGRESS_PIP_IP..."
for _ in {1..120}; do
  svc_ip="$(kubectl_ctx -n ingress-nginx get svc ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)"
  if [[ -n "$svc_ip" && "$svc_ip" == "$INGRESS_PIP_IP" ]]; then
    break
  fi
  sleep 5
done

svc_ip="$(kubectl_ctx -n ingress-nginx get svc ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)"
if [[ -z "$svc_ip" ]]; then
  echo "[aks-up] ERROR: ingress-nginx service has no external IP after waiting." >&2
  exit 1
fi
if [[ "$svc_ip" != "$INGRESS_PIP_IP" ]]; then
  echo "[aks-up] ERROR: ingress-nginx external IP '$svc_ip' does not match expected '$INGRESS_PIP_IP'." >&2
  exit 1
fi

log "Ensuring Azure DNS zone '$FRONTEND_DOMAIN' exists in resource group '$DNS_RESOURCE_GROUP'..."
if ! az network dns zone show --resource-group "$DNS_RESOURCE_GROUP" --name "$FRONTEND_DOMAIN" >/dev/null 2>&1; then
  az network dns zone create --resource-group "$DNS_RESOURCE_GROUP" --name "$FRONTEND_DOMAIN" -o none
fi
zone_ns="$(az network dns zone show --resource-group "$DNS_RESOURCE_GROUP" --name "$FRONTEND_DOMAIN" --query nameServers -o tsv | tr '\n' ' ')"
log "DNS name servers: $zone_ns"

log "Upserting DNS records for '$FRONTEND_DOMAIN' -> $INGRESS_PIP_IP ..."
az network dns record-set a create --resource-group "$DNS_RESOURCE_GROUP" --zone-name "$FRONTEND_DOMAIN" --name "@" --ttl 300 -o none || true
existing_a_ips="$(az network dns record-set a show --resource-group "$DNS_RESOURCE_GROUP" --zone-name "$FRONTEND_DOMAIN" --name "@" --query "ARecords[].ipv4Address" -o tsv 2>/dev/null || true)"
if [[ -n "$existing_a_ips" ]]; then
  while IFS= read -r old_ip; do
    [[ -z "$old_ip" ]] && continue
    if [[ "$old_ip" != "$INGRESS_PIP_IP" ]]; then
      az network dns record-set a remove-record --resource-group "$DNS_RESOURCE_GROUP" --zone-name "$FRONTEND_DOMAIN" --record-set-name "@" --ipv4-address "$old_ip" -o none || true
    fi
  done <<< "$existing_a_ips"
fi
az network dns record-set a add-record --resource-group "$DNS_RESOURCE_GROUP" --zone-name "$FRONTEND_DOMAIN" --record-set-name "@" --ipv4-address "$INGRESS_PIP_IP" -o none || true

for cname in www airflow minio minio-api keycloak datahub superset grafana jupyter prometheus; do
  az network dns record-set cname create --resource-group "$DNS_RESOURCE_GROUP" --zone-name "$FRONTEND_DOMAIN" --name "$cname" --ttl 300 -o none || true
  az network dns record-set cname set-record --resource-group "$DNS_RESOURCE_GROUP" --zone-name "$FRONTEND_DOMAIN" --record-set-name "$cname" --cname "$FRONTEND_DOMAIN" -o none
done

log "Installing/upgrading cert-manager ($CERT_MANAGER_VERSION)..."
kubectl_ctx apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"

log "Waiting for cert-manager to be ready..."
kubectl_ctx -n cert-manager rollout status deployment/cert-manager --timeout="$WAIT_TIMEOUT"
kubectl_ctx -n cert-manager rollout status deployment/cert-manager-cainjector --timeout="$WAIT_TIMEOUT"
kubectl_ctx -n cert-manager rollout status deployment/cert-manager-webhook --timeout="$WAIT_TIMEOUT"

log "Logging in to ACR '$ACR_NAME'..."
az acr login --name "$ACR_NAME" -o none

log "Building and pushing Airflow image '$AIRFLOW_IMAGE' (linux/amd64)..."
docker buildx build \
  --platform linux/amd64 \
  --tag "$AIRFLOW_IMAGE" \
  --file "$ROOT_DIR/airflow/Dockerfile" \
  "$ROOT_DIR" \
  --push

log "Building and pushing Frontend image '$FRONTEND_IMAGE' (linux/amd64)..."
docker buildx build \
  --platform linux/amd64 \
  --tag "$FRONTEND_IMAGE" \
  --file "$ROOT_DIR/frontend/Dockerfile.k8s" \
  "$ROOT_DIR/frontend" \
  --push

log "Applying namespace..."
render_and_apply "$ROOT_DIR/k8s/aks/namespace.yaml"

log "Creating/updating Kubernetes secret from .env..."
kubectl_ctx -n "$NAMESPACE" create secret generic odp-env \
  --from-env-file="$ROOT_DIR/.env" \
  --dry-run=client -o yaml | kubectl_ctx apply -f -

log "Creating/updating Airflow webserver config ConfigMap..."
kubectl_ctx -n "$NAMESPACE" create configmap airflow-webserver-config \
  --from-file=webserver_config.py="$ROOT_DIR/airflow/webserver_config.py" \
  --dry-run=client -o yaml | kubectl_ctx apply -f -

log "Applying core services (postgres, warehouse, minio)..."
render_and_apply "$ROOT_DIR/k8s/aks/postgres-airflow.yaml"
render_and_apply "$ROOT_DIR/k8s/aks/warehouse.yaml"
render_and_apply "$ROOT_DIR/k8s/aks/minio.yaml"

log "Waiting for core deployments..."
kubectl_ctx -n "$NAMESPACE" rollout status deployment/postgres --timeout="$WAIT_TIMEOUT"
kubectl_ctx -n "$NAMESPACE" rollout status deployment/warehouse --timeout="$WAIT_TIMEOUT"
kubectl_ctx -n "$NAMESPACE" rollout status deployment/minio --timeout="$WAIT_TIMEOUT"

log "Running MinIO bucket init job..."
kubectl_ctx -n "$NAMESPACE" delete job minio-create-buckets --ignore-not-found
render_and_apply "$ROOT_DIR/k8s/aks/minio-create-buckets-job.yaml"
wait_for_job_complete "minio-create-buckets" "$WAIT_TIMEOUT"

log "Running Airflow init job..."
kubectl_ctx -n "$NAMESPACE" delete job airflow-init --ignore-not-found
render_and_apply "$ROOT_DIR/k8s/aks/airflow-init-job.yaml"
wait_for_job_complete "airflow-init" "$WAIT_TIMEOUT"

log "Applying Airflow webserver + scheduler..."
render_and_apply "$ROOT_DIR/k8s/aks/airflow-webserver.yaml"
render_and_apply "$ROOT_DIR/k8s/aks/airflow-scheduler.yaml"

log "Waiting for Airflow deployments..."
kubectl_ctx -n "$NAMESPACE" rollout status deployment/airflow-webserver --timeout="$WAIT_TIMEOUT"
kubectl_ctx -n "$NAMESPACE" rollout status deployment/airflow-scheduler --timeout="$WAIT_TIMEOUT"

log "Applying cert issuer + frontend + ingress..."
render_and_apply "$ROOT_DIR/k8s/aks/cert-issuer-letsencrypt-prod.yaml"
render_and_apply "$ROOT_DIR/k8s/aks/frontend.yaml"
kubectl_ctx -n "$NAMESPACE" rollout status deployment/frontend --timeout="$WAIT_TIMEOUT"
render_and_apply "$ROOT_DIR/k8s/aks/frontend-ingress.yaml"
render_and_apply "$ROOT_DIR/k8s/aks/minio-sso-login-ingress.yaml"

log "Waiting for TLS certificate to be Ready..."
kubectl_ctx -n "$NAMESPACE" wait --for=condition=Ready certificate/frontend-tls --timeout=600s

log "Smoke test (bypass DNS with --resolve)..."
curl -sS -o /dev/null -D - --resolve "${FRONTEND_DOMAIN}:80:${INGRESS_PIP_IP}" "http://${FRONTEND_DOMAIN}" | head -n 1
curl -sS -o /dev/null -D - --resolve "${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://${FRONTEND_DOMAIN}" | head -n 1
curl -sS -o /dev/null -D - --resolve "airflow.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://airflow.${FRONTEND_DOMAIN}/health" | head -n 1
curl -sS -o /dev/null -D - --resolve "minio.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://minio.${FRONTEND_DOMAIN}/" | head -n 1
curl -sS -o /dev/null -D - --resolve "minio-api.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://minio-api.${FRONTEND_DOMAIN}/minio/health/live" | head -n 1
curl -sS -o /dev/null -D - --resolve "keycloak.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://keycloak.${FRONTEND_DOMAIN}/" | head -n 1
echo | openssl s_client -servername "${FRONTEND_DOMAIN}" -connect "${INGRESS_PIP_IP}:443" 2>/dev/null | openssl x509 -noout -subject -issuer | sed -n '1,2p'

cat <<EOT

AKS deployment is up.

Cluster:       $AKS_CLUSTER_NAME
ResourceGroup: $AKS_RESOURCE_GROUP
Namespace:     $NAMESPACE
Airflow image: $AIRFLOW_IMAGE
Frontend URL:  https://$FRONTEND_DOMAIN
Airflow URL:   https://airflow.$FRONTEND_DOMAIN
MinIO URL:     https://minio.$FRONTEND_DOMAIN
MinIO API URL: https://minio-api.$FRONTEND_DOMAIN
Keycloak URL:  https://keycloak.$FRONTEND_DOMAIN

Access services with port-forward from your machine:
  kubectl -n $NAMESPACE port-forward svc/airflow-webserver 8080:8080
  kubectl -n $NAMESPACE port-forward svc/minio 9000:9000 9001:9001
  kubectl -n $NAMESPACE port-forward svc/warehouse 5433:5432

Cleanup when done (to avoid costs):
  az group delete --name $AKS_RESOURCE_GROUP --yes --no-wait

EOT
