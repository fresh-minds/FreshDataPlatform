#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-ai-trial-dev}"
NAMESPACE="${NAMESPACE:-odp-dev}"
AIRFLOW_IMAGE="${AIRFLOW_IMAGE:-ai-trial/airflow:dev}"
KIND_MOUNT_PATH="${KIND_MOUNT_PATH:-/workspace/ai_trial}"

log() {
  echo "[k8s-dev-up] $*"
}

wait_for_job_complete() {
  local job_name="$1"
  local timeout="${2:-300s}"

  if kubectl -n "$NAMESPACE" wait --for=condition=complete "job/${job_name}" --timeout="$timeout"; then
    return 0
  fi

  echo "[k8s-dev-up] Job '${job_name}' did not complete within ${timeout}. Dumping diagnostics..." >&2
  kubectl -n "$NAMESPACE" describe "job/${job_name}" >&2 || true

  local pods
  pods="$(kubectl -n "$NAMESPACE" get pods -l "job-name=${job_name}" -o name 2>/dev/null || true)"
  if [[ -z "${pods}" ]]; then
    echo "[k8s-dev-up] No pods found for job '${job_name}'." >&2
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

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd docker
require_cmd kubectl
require_cmd kind

if [[ ! -f "$ROOT_DIR/.env" ]]; then
  echo "Missing $ROOT_DIR/.env. Create it first (for example: cp .env.template .env)." >&2
  exit 1
fi

if ! kind get clusters | grep -qx "$CLUSTER_NAME"; then
  log "Creating kind cluster '$CLUSTER_NAME' with repo mount '$ROOT_DIR' -> '$KIND_MOUNT_PATH'..."
  KIND_CONFIG="$(mktemp)"
  cat > "$KIND_CONFIG" <<EOC
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraMounts:
      - hostPath: "$ROOT_DIR"
        containerPath: "$KIND_MOUNT_PATH"
EOC
  kind create cluster --name "$CLUSTER_NAME" --config "$KIND_CONFIG"
  rm -f "$KIND_CONFIG"
else
  log "kind cluster '$CLUSTER_NAME' already exists."
fi

KIND_NODE="$(kind get nodes --name "$CLUSTER_NAME" | head -n 1)"
if [[ -z "${KIND_NODE:-}" ]]; then
  echo "Failed to resolve kind node for cluster '$CLUSTER_NAME'." >&2
  exit 1
fi

if ! docker exec "$KIND_NODE" test -f "$KIND_MOUNT_PATH/Makefile"; then
  cat <<EOT >&2
The cluster '$CLUSTER_NAME' does not expose this repo at '$KIND_MOUNT_PATH'.
Run 'make k8s-dev-down' and then 'make k8s-dev-up' to recreate the cluster with the repo mount.
EOT
  exit 1
fi

log "Building Airflow image ($AIRFLOW_IMAGE)..."
docker build -t "$AIRFLOW_IMAGE" -f "$ROOT_DIR/airflow/Dockerfile" "$ROOT_DIR"

log "Loading Airflow image into kind cluster..."
kind load docker-image "$AIRFLOW_IMAGE" --name "$CLUSTER_NAME"

log "Applying namespace..."
kubectl apply -f "$ROOT_DIR/k8s/dev/namespace.yaml"

log "Creating/updating Kubernetes Secret from .env..."
kubectl -n "$NAMESPACE" create secret generic odp-env \
  --from-env-file="$ROOT_DIR/.env" \
  --dry-run=client -o yaml | kubectl apply -f -

log "Applying core services (postgres, warehouse, minio)..."
kubectl apply -f "$ROOT_DIR/k8s/dev/postgres-airflow.yaml"
kubectl apply -f "$ROOT_DIR/k8s/dev/warehouse.yaml"
kubectl apply -f "$ROOT_DIR/k8s/dev/minio.yaml"

log "Waiting for core deployments..."
kubectl -n "$NAMESPACE" rollout status deployment/postgres --timeout=300s
kubectl -n "$NAMESPACE" rollout status deployment/warehouse --timeout=300s
kubectl -n "$NAMESPACE" rollout status deployment/minio --timeout=300s

log "Running MinIO bucket init job..."
kubectl -n "$NAMESPACE" delete job minio-create-buckets --ignore-not-found
kubectl apply -f "$ROOT_DIR/k8s/dev/minio-create-buckets-job.yaml"
wait_for_job_complete "minio-create-buckets" "300s"

log "Running Airflow init job..."
kubectl -n "$NAMESPACE" delete job airflow-init --ignore-not-found
kubectl apply -f "$ROOT_DIR/k8s/dev/airflow-init-job.yaml"
wait_for_job_complete "airflow-init" "300s"

log "Applying Airflow webserver + scheduler..."
kubectl apply -f "$ROOT_DIR/k8s/dev/airflow-webserver.yaml"
kubectl apply -f "$ROOT_DIR/k8s/dev/airflow-scheduler.yaml"

log "Waiting for Airflow deployments..."
kubectl -n "$NAMESPACE" rollout status deployment/airflow-webserver --timeout=300s
kubectl -n "$NAMESPACE" rollout status deployment/airflow-scheduler --timeout=300s

cat <<EOT

Dev-like Kubernetes stack is up.

Access services with port-forward:
  kubectl -n $NAMESPACE port-forward svc/airflow-webserver 8080:8080
  kubectl -n $NAMESPACE port-forward svc/minio 9000:9000 9001:9001
  kubectl -n $NAMESPACE port-forward svc/warehouse 5433:5432

Mounted repo path inside Airflow pods: $KIND_MOUNT_PATH

EOT
