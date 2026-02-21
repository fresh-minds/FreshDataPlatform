#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-ai-trial-dev}"
NAMESPACE="${NAMESPACE:-odp-dev}"

log() {
  echo "[k8s-dev-down] $*"
}

if command -v kubectl >/dev/null 2>&1; then
  if kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
    log "Deleting namespace '$NAMESPACE'..."
    kubectl delete namespace "$NAMESPACE" --ignore-not-found
  fi
fi

if command -v kind >/dev/null 2>&1; then
  if kind get clusters | grep -qx "$CLUSTER_NAME"; then
    log "Deleting kind cluster '$CLUSTER_NAME'..."
    kind delete cluster --name "$CLUSTER_NAME"
  else
    log "kind cluster '$CLUSTER_NAME' does not exist."
  fi
else
  echo "kind not found; skipped cluster deletion." >&2
fi
