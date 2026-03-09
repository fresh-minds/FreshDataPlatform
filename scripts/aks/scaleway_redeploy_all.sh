#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TF_DIR="${TF_DIR:-$ROOT_DIR/terraform/scaleway}"
TF_VARS_FILE="${TF_VARS_FILE:-$ROOT_DIR/terraform/environments/scaleway-dev.tfvars}"
TF_PROJECT_ID="${TF_PROJECT_ID:-${SCW_DEFAULT_PROJECT_ID:-}}"
DRY_RUN="false"
AUTO_APPROVE="false"
SKIP_TERRAFORM_APPLY="false"
SKIP_DEPLOY="false"
SKIP_SMOKE="false"
SKIP_REGISTRY_PREFLIGHT="false"
MINIMAL_DEPLOY="false"

tf_state_has() {
  local address="$1"
  terraform -chdir="$TF_DIR" state list 2>/dev/null | grep -Fxq "$address"
}

get_tf_cluster_name() {
  local value

  value="$(awk -F= '/^\s*cluster_name\s*=/{gsub(/["[:space:]]/, "", $2); print $2; exit}' "$TF_VARS_FILE" 2>/dev/null || true)"
  if [[ -n "$value" ]]; then
    printf '%s' "$value"
    return 0
  fi

  value="$(terraform -chdir="$TF_DIR" output -raw cluster_name 2>/dev/null || true)"
  if [[ -n "$value" ]]; then
    printf '%s' "$value"
    return 0
  fi

  printf 'ai-trial'
}

get_tf_region() {
  local value

  value="$(awk -F= '/^\s*scw_region\s*=/{gsub(/["[:space:]]/, "", $2); print $2; exit}' "$TF_VARS_FILE" 2>/dev/null || true)"
  if [[ -n "$value" ]]; then
    printf '%s' "$value"
    return 0
  fi

  value="$(terraform -chdir="$TF_DIR" output -raw scw_region 2>/dev/null || true)"
  if [[ -n "$value" ]]; then
    printf '%s' "$value"
    return 0
  fi

  printf 'nl-ams'
}

get_tf_cluster_id() {
  local value

  value="$(terraform -chdir="$TF_DIR" output -raw scw_cluster_id 2>/dev/null || true)"
  if [[ -n "$value" ]]; then
    printf '%s' "${value#*/}"
    return 0
  fi

  printf ''
}

import_scaleway_k8s_pool_if_exists() {
  local region cluster_id pool_name pool_id
  local pool_addr="module.scaleway_kubernetes.scaleway_k8s_pool.main"
  local pools_json

  require_cmd curl
  require_cmd jq

  if [[ -z "${SCW_SECRET_KEY:-}" ]]; then
    log "Cannot auto-reconcile pool state: SCW_SECRET_KEY is not set."
    return 1
  fi

  region="$(get_tf_region)"
  cluster_id="$(get_tf_cluster_id)"
  pool_name="default"

  if [[ -z "$cluster_id" ]]; then
    log "Cannot auto-reconcile pool state: cluster ID is not available from Terraform outputs."
    return 1
  fi

  pools_json="$(curl -fsS -H "X-Auth-Token: ${SCW_SECRET_KEY}" "https://api.scaleway.com/k8s/v1/regions/${region}/clusters/${cluster_id}/pools")"
  pool_id="$(echo "$pools_json" | jq -r --arg n "$pool_name" '((.pools // .items // [])[] | select(.name == $n) | .id) // empty' | head -n1)"

  if [[ -z "$pool_id" ]]; then
    log "No existing pool named '${pool_name}' found in cluster '${cluster_id}'."
    return 1
  fi

  if ! tf_state_has "$pool_addr"; then
    log "Importing existing Kubernetes pool into Terraform state: ${region}/${pool_id}"
    terraform -chdir="$TF_DIR" import -input=false \
      -var-file="$TF_VARS_FILE" \
      -var "scw_project_id=$TF_PROJECT_ID" \
      "$pool_addr" "${region}/${pool_id}" >/dev/null
  fi

  return 0
}

import_scaleway_secrets_iam_if_exists() {
  local cluster_name app_name policy_name
  local app_id policy_id api_key_id
  local org_id project_json
  local app_addr="module.scaleway_secrets[0].scaleway_iam_application.kapsule_secrets_reader"
  local policy_addr="module.scaleway_secrets[0].scaleway_iam_policy.kapsule_secrets_reader"
  local key_addr="module.scaleway_secrets[0].scaleway_iam_api_key.kapsule_secrets_reader"
  local app_json policy_json key_json

  require_cmd curl
  require_cmd jq

  if [[ -z "${SCW_SECRET_KEY:-}" ]]; then
    log "Cannot auto-reconcile IAM state: SCW_SECRET_KEY is not set."
    return 1
  fi

  if [[ -z "$TF_PROJECT_ID" ]]; then
    log "Cannot auto-reconcile IAM state: missing Scaleway project id."
    return 1
  fi

  cluster_name="$(get_tf_cluster_name)"
  app_name="${cluster_name}-secrets-reader"
  policy_name="${cluster_name}-secrets-reader-policy"

  project_json="$(curl -fsS -H "X-Auth-Token: ${SCW_SECRET_KEY}" "https://api.scaleway.com/account/v3/projects/${TF_PROJECT_ID}")"
  org_id="$(echo "$project_json" | jq -r '.organization_id // empty')"
  if [[ -z "$org_id" ]]; then
    log "Cannot auto-reconcile IAM state: failed to resolve organization_id from project '${TF_PROJECT_ID}'."
    return 1
  fi

  log "Attempting Scaleway IAM state reconciliation for '${app_name}'"

  app_json="$(curl -fsS -H "X-Auth-Token: ${SCW_SECRET_KEY}" "https://api.scaleway.com/iam/v1alpha1/applications?organization_id=${org_id}")"
  policy_json="$(curl -fsS -H "X-Auth-Token: ${SCW_SECRET_KEY}" "https://api.scaleway.com/iam/v1alpha1/policies?organization_id=${org_id}")"
  key_json="$(curl -fsS -H "X-Auth-Token: ${SCW_SECRET_KEY}" "https://api.scaleway.com/iam/v1alpha1/api-keys?organization_id=${org_id}")"

  app_id="$(echo "$app_json" | jq -r --arg n "$app_name" '((.applications // .items // [])[] | select(.name == $n) | .id) // empty' | head -n1)"
  policy_id="$(echo "$policy_json" | jq -r --arg n "$policy_name" '((.policies // .items // [])[] | select(.name == $n) | .id) // empty' | head -n1)"

  if [[ -n "$app_id" ]]; then
    if ! tf_state_has "$app_addr"; then
      log "Importing existing IAM application into Terraform state: $app_id"
      terraform -chdir="$TF_DIR" import -input=false \
        -var-file="$TF_VARS_FILE" \
        -var "scw_project_id=$TF_PROJECT_ID" \
        "$app_addr" "$app_id" >/dev/null
    fi
  else
    log "No existing IAM application found with name '$app_name'"
  fi

  if [[ -n "$policy_id" ]]; then
    if ! tf_state_has "$policy_addr"; then
      log "Importing existing IAM policy into Terraform state: $policy_id"
      terraform -chdir="$TF_DIR" import -input=false \
        -var-file="$TF_VARS_FILE" \
        -var "scw_project_id=$TF_PROJECT_ID" \
        "$policy_addr" "$policy_id" >/dev/null
    fi
  else
    log "No existing IAM policy found with name '$policy_name'"
  fi

  if [[ -n "$app_id" ]]; then
    api_key_id="$(echo "$key_json" | jq -r --arg app "$app_id" --arg d "API key for Kapsule nodes to read ODP secrets" '((."api-keys" // .api_keys // .api_keys // .items // [])[] | select(.application_id == $app and .description == $d) | .access_key) // empty' | head -n1)"
    if [[ -n "$api_key_id" ]]; then
      if ! tf_state_has "$key_addr"; then
        log "Importing existing IAM API key into Terraform state: $api_key_id"
        terraform -chdir="$TF_DIR" import -input=false \
          -var-file="$TF_VARS_FILE" \
          -var "scw_project_id=$TF_PROJECT_ID" \
          "$key_addr" "$api_key_id" >/dev/null
      fi
    else
      log "No existing IAM API key found for app '$app_name' (Terraform may create one)"
    fi
  fi

  return 0
}

normalize_scaleway_repo() {
  local repo="$1"
  # Scaleway registry namespaces do not support nested paths the same way ACR does.
  # Use the leaf segment as repository name (ai-trial/airflow -> airflow).
  printf '%s' "${repo##*/}"
}

get_registry_login_server() {
  local value

  value="${ACR_LOGIN_SERVER:-}"
  if [[ -n "$value" ]]; then
    printf '%s' "$value"
    return 0
  fi

  value="$(terraform -chdir="$TF_DIR" output -raw registry_login_server 2>/dev/null || true)"
  printf '%s' "$value"
}

verify_scaleway_registry_push_rights() {
  local registry="$1"
  local probe_repo="registry-permission-check"
  local probe_tag image_ref
  local push_log push_exit

  if [[ -z "$registry" ]]; then
    log "Skipping registry preflight: registry login server is not available."
    return 0
  fi

  if [[ -z "${SCW_SECRET_KEY:-}" ]]; then
    log "Skipping registry preflight: SCW_SECRET_KEY is not set in environment."
    return 0
  fi

  probe_tag="check-$(date +%s)"
  image_ref="${registry}/${probe_repo}:${probe_tag}"
  push_log="$(mktemp)"

  log "Preflight: verifying push rights on '${registry}'"
  echo "${SCW_SECRET_KEY}" | docker login "$registry" -u nologin --password-stdin >/dev/null
  docker pull busybox:1.36 >/dev/null
  docker tag busybox:1.36 "$image_ref"

  set +e
  docker push "$image_ref" >"$push_log" 2>&1
  push_exit=$?
  set -e

  docker rmi "$image_ref" >/dev/null 2>&1 || true

  if [[ $push_exit -ne 0 ]]; then
    cat "$push_log" >&2
    rm -f "$push_log"
    echo "Registry preflight failed: no push rights for '$registry'." >&2
    echo "Grant Container Registry push permissions to the principal behind SCW_ACCESS_KEY/SCW_SECRET_KEY and retry." >&2
    exit 1
  fi

  rm -f "$push_log"
  log "Registry preflight succeeded (push rights confirmed)."
}

log() {
  echo "[scw-redeploy] $*"
}

usage() {
  cat <<EOF
Redeploy full Scaleway platform in one flow (Terraform + workloads + smoke checks).

Usage:
  scripts/aks/scaleway_redeploy_all.sh [options]

Options:
  --dry-run                     Run Terraform plan only, then exit.
  --yes                         Skip confirmation prompt.
  --minimal                     Deploy minimal stack (no DataHub, no heavy observability, no jupyter).
  --skip-terraform-apply        Skip Terraform apply and reuse existing infra.
  --skip-deploy                 Skip workload deployment (aks_up.sh).
  --skip-smoke                  Skip post-deploy smoke checks.
  --skip-registry-preflight     Skip Scaleway registry push-permission preflight.
  --tf-vars-file <path>         Path to Scaleway tfvars file.
  --tf-dir <path>               Path to Scaleway Terraform root module.
  --project-id <uuid>           Scaleway project id (defaults to SCW_DEFAULT_PROJECT_ID).
  -h, --help                    Show this help.

Environment:
  SCW_ACCESS_KEY, SCW_SECRET_KEY, SCW_DEFAULT_PROJECT_ID
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN="true"
      shift
      ;;
    --yes)
      AUTO_APPROVE="true"
      shift
      ;;
    --skip-terraform-apply)
      SKIP_TERRAFORM_APPLY="true"
      shift
      ;;
    --minimal)
      MINIMAL_DEPLOY="true"
      shift
      ;;
    --skip-deploy)
      SKIP_DEPLOY="true"
      shift
      ;;
    --skip-smoke)
      SKIP_SMOKE="true"
      shift
      ;;
    --skip-registry-preflight)
      SKIP_REGISTRY_PREFLIGHT="true"
      shift
      ;;
    --tf-vars-file)
      TF_VARS_FILE="$2"
      shift 2
      ;;
    --tf-dir)
      TF_DIR="$2"
      shift 2
      ;;
    --project-id)
      TF_PROJECT_ID="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ "$TF_DIR" != /* ]]; then
  TF_DIR="$ROOT_DIR/$TF_DIR"
fi

if [[ "$TF_VARS_FILE" != /* ]]; then
  TF_VARS_FILE="$ROOT_DIR/$TF_VARS_FILE"
fi

require_cmd terraform
require_cmd jq

if [[ ! -d "$TF_DIR" ]]; then
  echo "Terraform directory not found: $TF_DIR" >&2
  exit 1
fi

if [[ ! -f "$TF_VARS_FILE" ]]; then
  echo "tfvars file not found: $TF_VARS_FILE" >&2
  exit 1
fi

if [[ "$SKIP_TERRAFORM_APPLY" != "true" && -z "$TF_PROJECT_ID" ]]; then
  echo "Missing project id. Set SCW_DEFAULT_PROJECT_ID or pass --project-id." >&2
  exit 1
fi

if [[ "$DRY_RUN" != "true" && "$AUTO_APPROVE" != "true" ]]; then
  echo
  deploy_scope="full"
  [[ "$MINIMAL_DEPLOY" == "true" ]] && deploy_scope="minimal"
  echo "WARNING: This will redeploy the ${deploy_scope} Scaleway stack using:"
  echo "  TF_DIR=$TF_DIR"
  echo "  TF_VARS_FILE=$TF_VARS_FILE"
  if [[ -n "$TF_PROJECT_ID" ]]; then
    echo "  scw_project_id=$TF_PROJECT_ID"
  fi
  echo
  read -r -p "Type 'redeploy' to continue: " CONFIRM
  if [[ "$CONFIRM" != "redeploy" ]]; then
    log "Aborted."
    exit 1
  fi
fi

if [[ "$SKIP_TERRAFORM_APPLY" != "true" ]]; then
  log "Initializing Terraform in $TF_DIR"
  terraform -chdir="$TF_DIR" init -input=false >/dev/null

  if [[ "$DRY_RUN" == "true" ]]; then
    log "Running Terraform plan (dry-run)"
    terraform -chdir="$TF_DIR" plan \
      -input=false \
      -var-file="$TF_VARS_FILE" \
      -var "scw_project_id=$TF_PROJECT_ID"
    log "Dry-run complete."
    exit 0
  fi

  log "Applying Terraform infrastructure"
  APPLY_LOG="$(mktemp)"
  set +e
  terraform -chdir="$TF_DIR" apply \
    -input=false \
    -auto-approve \
    -var-file="$TF_VARS_FILE" \
    -var "scw_project_id=$TF_PROJECT_ID" 2>&1 | tee "$APPLY_LOG"
  apply_exit=$?
  set -e

  if [[ $apply_exit -ne 0 ]]; then
    if grep -q "resource application: resource already exists" "$APPLY_LOG"; then
      log "Detected existing Scaleway IAM resources outside Terraform state; attempting one-time import+retry."
      import_scaleway_secrets_iam_if_exists

      terraform -chdir="$TF_DIR" apply \
        -input=false \
        -auto-approve \
        -var-file="$TF_VARS_FILE" \
        -var "scw_project_id=$TF_PROJECT_ID"
    elif grep -q "pool name must be unique across the cluster" "$APPLY_LOG"; then
      log "Detected existing Kubernetes pool outside Terraform state; attempting one-time import+retry."
      import_scaleway_k8s_pool_if_exists

      terraform -chdir="$TF_DIR" apply \
        -input=false \
        -auto-approve \
        -var-file="$TF_VARS_FILE" \
        -var "scw_project_id=$TF_PROJECT_ID"
    else
      rm -f "$APPLY_LOG"
      exit $apply_exit
    fi
  fi

  rm -f "$APPLY_LOG"
else
  if [[ "$DRY_RUN" == "true" ]]; then
    log "--dry-run with --skip-terraform-apply performs no actions."
    exit 0
  fi
  log "Skipping Terraform apply (--skip-terraform-apply)"
fi

if [[ "$SKIP_DEPLOY" == "true" ]]; then
  log "Skipping workload deployment (--skip-deploy)"
  log "Scaleway redeploy flow completed (infrastructure stage only)."
  exit 0
fi

require_cmd docker
require_cmd kubectl
require_cmd curl
require_cmd openssl
require_cmd kompose
require_cmd yq

if [[ "$SKIP_REGISTRY_PREFLIGHT" != "true" ]]; then
  verify_scaleway_registry_push_rights "$(get_registry_login_server)"
else
  log "Skipping registry preflight (--skip-registry-preflight)"
fi

KUBE_CONFIG_COMMAND_FOR_DEPLOY="${KUBE_CONFIG_COMMAND:-}"
if ! command -v scw >/dev/null 2>&1; then
  if [[ -z "$KUBE_CONFIG_COMMAND_FOR_DEPLOY" || "$KUBE_CONFIG_COMMAND_FOR_DEPLOY" == *"scw "* ]]; then
    log "scw CLI not found; reusing existing kubeconfig context and skipping 'KUBE_CONFIG_COMMAND'."
    KUBE_CONFIG_COMMAND_FOR_DEPLOY=":"
  fi
fi

AIRFLOW_IMAGE_REPO_FOR_DEPLOY="$(normalize_scaleway_repo "${AIRFLOW_IMAGE_REPO:-ai-trial/airflow}")"
FRONTEND_IMAGE_REPO_FOR_DEPLOY="$(normalize_scaleway_repo "${FRONTEND_IMAGE_REPO:-ai-trial/frontend}")"
PORTAL_API_IMAGE_REPO_FOR_DEPLOY="$(normalize_scaleway_repo "${PORTAL_API_IMAGE_REPO:-ai-trial/portal-api}")"
JUPYTER_IMAGE_REPO_FOR_DEPLOY="$(normalize_scaleway_repo "${JUPYTER_IMAGE_REPO:-ai-trial/jupyter}")"
MINIO_SSO_BRIDGE_IMAGE_REPO_FOR_DEPLOY="$(normalize_scaleway_repo "${MINIO_SSO_BRIDGE_IMAGE_REPO:-ai-trial/minio-sso-bridge}")"

log "Deploying workloads via scripts/aks/aks_up.sh in Scaleway ${MINIMAL_DEPLOY:+minimal }mode"
TF_DIR="$TF_DIR" \
CLOUD_PROVIDER="scaleway" \
MINIMAL_DEPLOY="$MINIMAL_DEPLOY" \
KUBE_CONFIG_COMMAND="$KUBE_CONFIG_COMMAND_FOR_DEPLOY" \
AKS_DISABLE_BUILDX_ATTESTATIONS="true" \
AKS_DOCKER_NO_CACHE="true" \
AKS_USE_CLASSIC_DOCKER_PUSH="true" \
AIRFLOW_IMAGE_REPO="$AIRFLOW_IMAGE_REPO_FOR_DEPLOY" \
FRONTEND_IMAGE_REPO="$FRONTEND_IMAGE_REPO_FOR_DEPLOY" \
PORTAL_API_IMAGE_REPO="$PORTAL_API_IMAGE_REPO_FOR_DEPLOY" \
JUPYTER_IMAGE_REPO="$JUPYTER_IMAGE_REPO_FOR_DEPLOY" \
MINIO_SSO_BRIDGE_IMAGE_REPO="$MINIO_SSO_BRIDGE_IMAGE_REPO_FOR_DEPLOY" \
"$ROOT_DIR/scripts/aks/aks_up.sh"

if [[ "$SKIP_SMOKE" == "true" ]]; then
  log "Skipping smoke checks (--skip-smoke)"
  log "Scaleway redeploy completed."
  exit 0
fi

if [[ -x "$ROOT_DIR/scripts/testing/verify_aks_smoke.sh" ]]; then
  log "Running post-deploy smoke checks"
  "$ROOT_DIR/scripts/testing/verify_aks_smoke.sh"
else
  log "Smoke script not found or not executable: scripts/testing/verify_aks_smoke.sh"
  log "Skipping smoke checks."
fi

log "Scaleway redeploy completed successfully."
