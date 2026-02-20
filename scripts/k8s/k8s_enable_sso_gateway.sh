#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NAMESPACE="${NAMESPACE:-odp-dev}"
INGRESS_NAMESPACE="${INGRESS_NAMESPACE:-ingress-nginx}"
HOST_BASE="${HOST_BASE:-localtest.me}"
INGRESS_LOCAL_PORT="${INGRESS_LOCAL_PORT:-8085}"
TEMPLATE_PATH="${TEMPLATE_PATH:-$ROOT_DIR/k8s/dev/sso-gateway.yaml}"
KEYCLOAK_INTERNAL_URL="${KEYCLOAK_INTERNAL_URL:-http://keycloak:8090}"
KEYCLOAK_BROWSER_URL="${KEYCLOAK_BROWSER_URL:-http://keycloak.$HOST_BASE:$INGRESS_LOCAL_PORT}"
OIDC_ISSUER_URL="${OIDC_ISSUER_URL:-$KEYCLOAK_BROWSER_URL/realms/odp}"

log() {
  echo "[k8s-sso-gateway] $*"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

read_env_secret_key() {
  local key="$1"
  kubectl -n "$NAMESPACE" get secret odp-env -o "jsonpath={.data.$key}" 2>/dev/null | base64 --decode 2>/dev/null || true
}

read_gateway_secret_key() {
  local key="$1"
  kubectl -n "$NAMESPACE" get secret oauth2-proxy-secret -o "jsonpath={.data.$key}" 2>/dev/null | base64 --decode 2>/dev/null || true
}

generate_cookie_secret() {
  python3 - <<'PY'
import base64
import os
print(base64.urlsafe_b64encode(os.urandom(32)).decode())
PY
}

require_cmd kubectl
require_cmd helm
require_cmd python3

if [[ ! -f "$TEMPLATE_PATH" ]]; then
  echo "Missing template: $TEMPLATE_PATH" >&2
  exit 1
fi

if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "Namespace '$NAMESPACE' not found. Deploy the k8s stack first." >&2
  exit 1
fi

KEYCLOAK_GATEWAY_CLIENT_ID="${KEYCLOAK_GATEWAY_CLIENT_ID:-$(read_gateway_secret_key OAUTH2_PROXY_CLIENT_ID)}"
if [[ -z "$KEYCLOAK_GATEWAY_CLIENT_ID" ]]; then
  KEYCLOAK_GATEWAY_CLIENT_ID="$(read_env_secret_key KEYCLOAK_GATEWAY_CLIENT_ID)"
fi

KEYCLOAK_GATEWAY_CLIENT_SECRET="${KEYCLOAK_GATEWAY_CLIENT_SECRET:-$(read_gateway_secret_key OAUTH2_PROXY_CLIENT_SECRET)}"
if [[ -z "$KEYCLOAK_GATEWAY_CLIENT_SECRET" ]]; then
  KEYCLOAK_GATEWAY_CLIENT_SECRET="$(read_env_secret_key KEYCLOAK_GATEWAY_CLIENT_SECRET)"
fi

OAUTH2_PROXY_COOKIE_SECRET="${OAUTH2_PROXY_COOKIE_SECRET:-$(read_gateway_secret_key OAUTH2_PROXY_COOKIE_SECRET)}"
if [[ -z "$OAUTH2_PROXY_COOKIE_SECRET" ]]; then
  OAUTH2_PROXY_COOKIE_SECRET="$(read_env_secret_key OAUTH2_PROXY_COOKIE_SECRET)"
fi

if [[ -z "$KEYCLOAK_GATEWAY_CLIENT_ID" ]]; then
  KEYCLOAK_GATEWAY_CLIENT_ID="platform-gateway"
fi

if [[ -z "$KEYCLOAK_GATEWAY_CLIENT_SECRET" ]]; then
  KEYCLOAK_GATEWAY_CLIENT_SECRET="change_me_keycloak_gateway_secret"
  log "WARN: KEYCLOAK_GATEWAY_CLIENT_SECRET missing; using fallback secret."
fi

if [[ -z "$OAUTH2_PROXY_COOKIE_SECRET" ]]; then
  OAUTH2_PROXY_COOKIE_SECRET="$(generate_cookie_secret)"
  log "Generated a random oauth2-proxy cookie secret for this cluster."
fi

log "Ensuring ingress-nginx is installed..."
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx >/dev/null 2>&1 || true
helm repo update >/dev/null
helm upgrade --install ingress-nginx ingress-nginx/ingress-nginx \
  --namespace "$INGRESS_NAMESPACE" \
  --create-namespace \
  --set controller.ingressClassResource.name=nginx \
  --set controller.ingressClassResource.default=true \
  --set controller.service.type=ClusterIP \
  --wait >/dev/null

log "Refreshing Keycloak realm import with gateway client..."
kubectl apply -f "$ROOT_DIR/k8s/dev/keycloak.yaml" >/dev/null
kubectl -n "$NAMESPACE" rollout status deployment/keycloak --timeout=300s >/dev/null

log "Creating oauth2-proxy secret..."
kubectl -n "$NAMESPACE" create secret generic oauth2-proxy-secret \
  --from-literal=OAUTH2_PROXY_CLIENT_ID="$KEYCLOAK_GATEWAY_CLIENT_ID" \
  --from-literal=OAUTH2_PROXY_CLIENT_SECRET="$KEYCLOAK_GATEWAY_CLIENT_SECRET" \
  --from-literal=OAUTH2_PROXY_COOKIE_SECRET="$OAUTH2_PROXY_COOKIE_SECRET" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

log "Applying SSO gateway manifests..."
export TEMPLATE_PATH NAMESPACE HOST_BASE INGRESS_LOCAL_PORT OIDC_ISSUER_URL KEYCLOAK_INTERNAL_URL KEYCLOAK_BROWSER_URL
python3 - <<'PY' | kubectl apply -f - >/dev/null
import os
from pathlib import Path

template = Path(os.environ["TEMPLATE_PATH"]).read_text()
replacements = {
    "__NAMESPACE__": os.environ["NAMESPACE"],
    "__HOST_BASE__": os.environ["HOST_BASE"],
    "__INGRESS_PORT__": os.environ["INGRESS_LOCAL_PORT"],
    "__OIDC_ISSUER_URL__": os.environ["OIDC_ISSUER_URL"],
    "__KEYCLOAK_INTERNAL_URL__": os.environ["KEYCLOAK_INTERNAL_URL"],
    "__KEYCLOAK_BROWSER_URL__": os.environ["KEYCLOAK_BROWSER_URL"],
}
for old, new in replacements.items():
    template = template.replace(old, new)
print(template)
PY

kubectl -n "$NAMESPACE" rollout status deployment/oauth2-proxy --timeout=300s >/dev/null
kubectl -n "$INGRESS_NAMESPACE" rollout status deployment/ingress-nginx-controller --timeout=300s >/dev/null

cat <<EOF

SSO gateway is ready.

1) Keep this running in a terminal:
   kubectl -n $INGRESS_NAMESPACE port-forward svc/ingress-nginx-controller $INGRESS_LOCAL_PORT:80

2) Then open these URLs (single sign-on via Keycloak):
   http://auth.$HOST_BASE:$INGRESS_LOCAL_PORT/oauth2/sign_in
   http://airflow.$HOST_BASE:$INGRESS_LOCAL_PORT
   http://superset.$HOST_BASE:$INGRESS_LOCAL_PORT
   http://datahub.$HOST_BASE:$INGRESS_LOCAL_PORT
   http://minio.$HOST_BASE:$INGRESS_LOCAL_PORT
   http://portal.$HOST_BASE:$INGRESS_LOCAL_PORT
   http://grafana.$HOST_BASE:$INGRESS_LOCAL_PORT
   http://prometheus.$HOST_BASE:$INGRESS_LOCAL_PORT
   http://jupyter.$HOST_BASE:$INGRESS_LOCAL_PORT

Keycloak remains directly reachable at:
  http://keycloak.$HOST_BASE:$INGRESS_LOCAL_PORT

EOF
