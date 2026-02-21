#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-ai-trial-dev}"
NAMESPACE="${NAMESPACE:-odp-dev}"
KIND_MOUNT_PATH="${KIND_MOUNT_PATH:-/workspace/ai_trial}"
KIND_CONTEXT="kind-${CLUSTER_NAME}"
KOMPOSE_OVERRIDE_FILE="${KOMPOSE_OVERRIDE_FILE:-$ROOT_DIR/docker-compose.k8s.yml}"
KOMPOSE_LIB="$ROOT_DIR/scripts/k8s/k8s_kompose_lib.sh"

PORTAL_IMAGE="${PORTAL_IMAGE:-ai-trial/portal:dev}"
JUPYTER_IMAGE="${JUPYTER_IMAGE:-ai-trial/jupyter:dev}"
MINIO_SSO_BRIDGE_IMAGE="${MINIO_SSO_BRIDGE_IMAGE:-ai-trial/minio-sso-bridge:dev}"

export NAMESPACE

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

if [[ ! -f "$KOMPOSE_LIB" ]]; then
  echo "Missing kompose shared library: $KOMPOSE_LIB" >&2
  exit 1
fi

# shellcheck source=scripts/k8s/k8s_kompose_lib.sh
source "$KOMPOSE_LIB"

log() {
  echo "[k8s-dev-up-full] $*"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

wait_for_job_complete() {
  local job_name="$1"
  local timeout="${2:-600s}"

  if kubectl -n "$NAMESPACE" wait --for=condition=complete "job/${job_name}" --timeout="$timeout"; then
    return 0
  fi

  echo "[k8s-dev-up-full] Job '${job_name}' did not complete within ${timeout}. Dumping diagnostics..." >&2
  kubectl -n "$NAMESPACE" describe "job/${job_name}" >&2 || true

  local pods
  pods="$(kubectl -n "$NAMESPACE" get pods -l "job-name=${job_name}" -o name 2>/dev/null || true)"
  if [[ -z "${pods}" ]]; then
    echo "[k8s-dev-up-full] No pods found for job '${job_name}'." >&2
    return 1
  fi

  while IFS= read -r pod; do
    [[ -z "$pod" ]] && continue
    kubectl -n "$NAMESPACE" describe "$pod" >&2 || true
    kubectl -n "$NAMESPACE" logs "$pod" --all-containers=true --tail=200 >&2 || true
    kubectl -n "$NAMESPACE" logs "$pod" --all-containers=true --previous --tail=200 >&2 || true
  done <<< "$pods"

  return 1
}

wait_for_deployment() {
  local deployment="$1"
  local timeout="${2:-600s}"

  if kubectl -n "$NAMESPACE" get deployment "$deployment" >/dev/null 2>&1; then
    kubectl -n "$NAMESPACE" rollout status "deployment/${deployment}" --timeout="$timeout"
  fi
}

apply_namespaced_manifest() {
  local manifest_path="$1"
  yq eval '.metadata.namespace = strenv(NAMESPACE)' "$manifest_path" | kubectl apply -f -
}

require_cmd docker
require_cmd kubectl
require_cmd kind
require_cmd kompose
require_cmd yq

if [[ ! -f "$ROOT_DIR/.env" ]]; then
  echo "Missing $ROOT_DIR/.env. Create it first (for example: cp .env.template .env)." >&2
  exit 1
fi

if [[ ! -f "$KOMPOSE_OVERRIDE_FILE" ]]; then
  echo "Missing override file: $KOMPOSE_OVERRIDE_FILE" >&2
  exit 1
fi

if kind get clusters | grep -qx "$CLUSTER_NAME"; then
  log "Switching kubectl context to ${KIND_CONTEXT}..."
  kubectl config use-context "$KIND_CONTEXT" >/dev/null
fi

log "Bootstrapping core k8s stack (Airflow, Postgres, Warehouse, Keycloak, MinIO)..."
CLUSTER_NAME="$CLUSTER_NAME" NAMESPACE="$NAMESPACE" KIND_MOUNT_PATH="$KIND_MOUNT_PATH" "$ROOT_DIR/scripts/k8s/k8s_dev_up.sh"

log "Ensuring kubectl context is ${KIND_CONTEXT}..."
kubectl config use-context "$KIND_CONTEXT" >/dev/null

NODE_ARCH="$(kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}' 2>/dev/null || echo unknown)"
SKIP_MSTEAMS=false
if [[ "$NODE_ARCH" == "arm64" ]]; then
  SKIP_MSTEAMS=true
  log "Detected arm64 node architecture; prometheus-msteams image is amd64-only and will be skipped."
fi

log "Building additional images (portal, jupyter, minio-sso-bridge)..."
docker build -t "$PORTAL_IMAGE" -f "$ROOT_DIR/frontend/Dockerfile" "$ROOT_DIR/frontend"
docker build -t "$JUPYTER_IMAGE" -f "$ROOT_DIR/notebooks/Dockerfile" "$ROOT_DIR/notebooks"
docker build -t "$MINIO_SSO_BRIDGE_IMAGE" -f "$ROOT_DIR/ops/minio-sso-bridge/Dockerfile" "$ROOT_DIR"

log "Loading additional images into kind cluster..."
kind load docker-image "$PORTAL_IMAGE" --name "$CLUSTER_NAME"
kind load docker-image "$JUPYTER_IMAGE" --name "$CLUSTER_NAME"
kind load docker-image "$MINIO_SSO_BRIDGE_IMAGE" --name "$CLUSTER_NAME"

log "Generating Kubernetes manifests from docker-compose..."
KOMPOSE_OUT_DIR="$TMP_DIR"
KOMPOSE_OVERRIDE="$KOMPOSE_OVERRIDE_FILE"
KOMPOSE_LOG_SOURCE="k8s-dev-up-full"
SKIP_MSTEAMS="$SKIP_MSTEAMS"
export ROOT_DIR KOMPOSE_OUT_DIR KOMPOSE_OVERRIDE KIND_MOUNT_PATH SKIP_MSTEAMS NAMESPACE KOMPOSE_LOG_SOURCE
kompose_generate

log "Removing manifests covered by core stack..."
kompose_remove_phase_a

log "Rewriting host paths for kind node mount..."
kompose_postprocess_local

log "Normalizing generated service ports for in-cluster access..."
kompose_normalise_services

log "Fixing deployment probes and service-link env collisions..."
kompose_fix_deployments

GMS_MANIFEST="$TMP_DIR/datahub-gms-deployment.yaml"
FRONTEND_MANIFEST="$TMP_DIR/datahub-frontend-deployment.yaml"
kompose_hold_datahub

log "Applying extended stack manifests..."
kubectl -n "$NAMESPACE" apply -f "$TMP_DIR"

if [[ "$SKIP_MSTEAMS" == "true" ]]; then
  kubectl -n "$NAMESPACE" delete deployment prometheus-msteams service prometheus-msteams --ignore-not-found
fi

log "Waiting for DataHub dependencies..."
wait_for_deployment datahub-mysql 600s
wait_for_deployment datahub-elasticsearch 600s
wait_for_deployment datahub-zookeeper 600s
wait_for_deployment datahub-kafka 600s
wait_for_deployment datahub-schema-registry 600s

log "Running DataHub setup jobs..."
for job in datahub-mysql-setup datahub-elasticsearch-setup datahub-kafka-setup datahub-upgrade; do
  kubectl -n "$NAMESPACE" delete job "$job" --ignore-not-found
  apply_namespaced_manifest "$ROOT_DIR/k8s/dev/${job}-job.yaml"
  wait_for_job_complete "$job" 600s
done

HAS_GMS_HOLD=false
HAS_FRONTEND_HOLD=false
[[ -f "$TMP_DIR/.datahub-gms-deployment.hold" ]] && HAS_GMS_HOLD=true
[[ -f "$TMP_DIR/.datahub-frontend-deployment.hold" ]] && HAS_FRONTEND_HOLD=true

if [[ "$HAS_GMS_HOLD" == "true" || "$HAS_FRONTEND_HOLD" == "true" ]]; then
  kompose_restore_datahub
fi

if [[ "$HAS_GMS_HOLD" == "true" ]]; then
  kubectl -n "$NAMESPACE" apply -f "$GMS_MANIFEST"
fi
if [[ "$HAS_FRONTEND_HOLD" == "true" ]]; then
  kubectl -n "$NAMESPACE" apply -f "$FRONTEND_MANIFEST"
fi

log "Waiting for remaining extended deployments..."
for deployment in \
  minio-sso-bridge \
  superset-db \
  superset \
  datahub-gms \
  datahub-frontend \
  portal \
  jupyter \
  alertmanager \
  prometheus \
  loki \
  tempo \
  otel-collector \
  grafana \
  statsd-exporter \
  postgres-exporter-airflow \
  postgres-exporter-warehouse \
  promtail
 do
  wait_for_deployment "$deployment" 600s
 done

if [[ "$SKIP_MSTEAMS" == "false" ]]; then
  wait_for_deployment prometheus-msteams 600s
fi

cat <<EOT

Full docker-compose parity stack is now deployed in Kubernetes namespace '$NAMESPACE'.

Useful port-forwards:
  kubectl -n $NAMESPACE port-forward svc/portal 3000:3000
  kubectl -n $NAMESPACE port-forward svc/airflow-webserver 8080:8080
  kubectl -n $NAMESPACE port-forward svc/keycloak 8090:8090
  kubectl -n $NAMESPACE port-forward svc/minio 9000:9000 9001:9001
  kubectl -n $NAMESPACE port-forward svc/superset 8088:8088
  kubectl -n $NAMESPACE port-forward svc/datahub-frontend 9002:9002
  kubectl -n $NAMESPACE port-forward svc/jupyter 8888:8888
  kubectl -n $NAMESPACE port-forward svc/prometheus 9090:9090
  kubectl -n $NAMESPACE port-forward svc/grafana 3001:3000

EOT

kubectl -n "$NAMESPACE" get pods
