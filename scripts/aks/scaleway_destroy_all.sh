#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TF_DIR="${TF_DIR:-$ROOT_DIR/terraform/scaleway}"
TF_VARS_FILE="${TF_VARS_FILE:-$ROOT_DIR/terraform/environments/scaleway-dev.tfvars}"
TF_PROJECT_ID="${TF_PROJECT_ID:-${SCW_DEFAULT_PROJECT_ID:-}}"
SCW_REGION="${SCW_REGION:-nl-ams}"
SCW_ZONE="${SCW_ZONE:-nl-ams-1}"
AUTO_APPROVE="false"
DRY_RUN="false"
PURGE_LEFTOVERS="false"

log() {
  echo "[scw-destroy] $*"
}

usage() {
  cat <<EOF
Destroy Scaleway resources managed by terraform/scaleway.

Usage:
  scripts/aks/scaleway_destroy_all.sh [--dry-run] [--yes] [--purge-leftovers] [--tf-vars-file <path>] [--tf-dir <path>] [--project-id <uuid>]

Options:
  --dry-run              Show terraform destroy plan only (no deletion).
  --yes                  Skip confirmation prompt.
  --purge-leftovers      Also remove leftover Scaleway Registry namespaces and LB IPs in the target project.
  --tf-vars-file <path>  Path to scaleway tfvars file.
  --tf-dir <path>        Path to Scaleway Terraform root module.
  --project-id <uuid>    Scaleway project id (defaults to SCW_DEFAULT_PROJECT_ID).
  -h, --help             Show this help.

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

remove_helm_state_if_present() {
  local helm_resources
  helm_resources="$(terraform -chdir="$TF_DIR" state list 2>/dev/null | grep '^module\.scaleway_helm_releases\.helm_release\.' || true)"

  if [[ -z "$helm_resources" ]]; then
    log "No Helm releases found in Terraform state for cleanup fallback"
    return 0
  fi

  while IFS= read -r resource; do
    [[ -z "$resource" ]] && continue
    log "Removing unreachable Helm release from Terraform state: $resource"
    terraform -chdir="$TF_DIR" state rm "$resource" >/dev/null
  done <<< "$helm_resources"
}

purge_leftovers() {
  local purge_mode="$1"

  require_cmd curl
  require_cmd jq

  if [[ -z "${SCW_SECRET_KEY:-}" ]]; then
    echo "SCW_SECRET_KEY is required when --purge-leftovers is set." >&2
    exit 1
  fi

  local registry_url="https://api.scaleway.com/registry/v1/regions/${SCW_REGION}/namespaces?project_id=${TF_PROJECT_ID}"
  local lb_ip_url="https://api.scaleway.com/lb/v1/zones/${SCW_ZONE}/ips?project_id=${TF_PROJECT_ID}"

  local namespaces_json
  local lb_ips_json
  namespaces_json="$(curl -fsS -H "X-Auth-Token: ${SCW_SECRET_KEY}" "$registry_url")"
  lb_ips_json="$(curl -fsS -H "X-Auth-Token: ${SCW_SECRET_KEY}" "$lb_ip_url")"

  local namespace_count
  local lb_ip_count
  namespace_count="$(echo "$namespaces_json" | jq -r '.total_count // (.namespaces | length) // 0')"
  lb_ip_count="$(echo "$lb_ips_json" | jq -r '.total_count // (.ips | length) // 0')"

  if [[ "$purge_mode" == "dry-run" ]]; then
    log "Purge preview: registry_namespaces=$namespace_count, lb_ips=$lb_ip_count"
    if [[ "$namespace_count" != "0" ]]; then
      echo "$namespaces_json" | jq -r '.namespaces[] | "  - registry namespace: \(.id) (\(.name))"'
    fi
    if [[ "$lb_ip_count" != "0" ]]; then
      echo "$lb_ips_json" | jq -r '.ips[] | "  - lb ip: \(.id) (\(.ip_address))"'
    fi
    return 0
  fi

  log "Purging leftover Scaleway resources (registry_namespaces=$namespace_count, lb_ips=$lb_ip_count)"

  if [[ "$namespace_count" != "0" ]]; then
    while IFS= read -r namespace_id; do
      [[ -z "$namespace_id" ]] && continue
      log "Deleting leftover registry namespace: $namespace_id"
      curl -fsS -X DELETE -H "X-Auth-Token: ${SCW_SECRET_KEY}" "https://api.scaleway.com/registry/v1/regions/${SCW_REGION}/namespaces/${namespace_id}" >/dev/null
    done < <(echo "$namespaces_json" | jq -r '.namespaces[] | .id')
  fi

  if [[ "$lb_ip_count" != "0" ]]; then
    while IFS= read -r lb_ip_id; do
      [[ -z "$lb_ip_id" ]] && continue
      log "Deleting leftover lb ip: $lb_ip_id"
      curl -fsS -X DELETE -H "X-Auth-Token: ${SCW_SECRET_KEY}" "https://api.scaleway.com/lb/v1/zones/${SCW_ZONE}/ips/${lb_ip_id}" >/dev/null
    done < <(echo "$lb_ips_json" | jq -r '.ips[] | .id')
  fi

  sleep 2

  namespace_count="$(curl -fsS -H "X-Auth-Token: ${SCW_SECRET_KEY}" "$registry_url" | jq -r '.total_count // (.namespaces | length) // 0')"
  lb_ip_count="$(curl -fsS -H "X-Auth-Token: ${SCW_SECRET_KEY}" "$lb_ip_url" | jq -r '.total_count // (.ips | length) // 0')"

  if [[ "$namespace_count" != "0" || "$lb_ip_count" != "0" ]]; then
    echo "Leftover purge incomplete: registry_namespaces=$namespace_count, lb_ips=$lb_ip_count" >&2
    exit 1
  fi

  log "Leftover purge completed (registry_namespaces=0, lb_ips=0)"
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
    --purge-leftovers)
      PURGE_LEFTOVERS="true"
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

if [[ ! -d "$TF_DIR" ]]; then
  echo "Terraform directory not found: $TF_DIR" >&2
  exit 1
fi

if [[ ! -f "$TF_VARS_FILE" ]]; then
  echo "tfvars file not found: $TF_VARS_FILE" >&2
  exit 1
fi

if [[ -z "$TF_PROJECT_ID" ]]; then
  echo "Missing project id. Set SCW_DEFAULT_PROJECT_ID or pass --project-id." >&2
  exit 1
fi

if [[ "$DRY_RUN" != "true" && "$AUTO_APPROVE" != "true" ]]; then
  echo
  echo "WARNING: This destroys all Terraform-managed Scaleway resources for:"
  echo "  TF_DIR=$TF_DIR"
  echo "  TF_VARS_FILE=$TF_VARS_FILE"
  echo "  scw_project_id=$TF_PROJECT_ID"
  echo
  read -r -p "Type 'destroy' to continue: " CONFIRM
  if [[ "$CONFIRM" != "destroy" ]]; then
    log "Aborted."
    exit 1
  fi
fi

log "Initializing Terraform in $TF_DIR"
terraform -chdir="$TF_DIR" init -input=false >/dev/null

if [[ "$DRY_RUN" == "true" ]]; then
  log "Running dry-run destroy plan"
  terraform -chdir="$TF_DIR" plan \
    -destroy \
    -refresh=false \
    -input=false \
    -var-file="$TF_VARS_FILE" \
    -var "scw_project_id=$TF_PROJECT_ID"

  if [[ "$PURGE_LEFTOVERS" == "true" ]]; then
    purge_leftovers "dry-run"
  fi

  exit 0
fi

log "Destroying Terraform-managed Scaleway resources"
set +e
terraform -chdir="$TF_DIR" destroy \
  -input=false \
  -auto-approve \
  -var-file="$TF_VARS_FILE" \
  -var "scw_project_id=$TF_PROJECT_ID"
destroy_exit_code=$?
set -e

if [[ $destroy_exit_code -ne 0 ]]; then
  log "Initial destroy failed; attempting Helm state cleanup fallback and retry"
  remove_helm_state_if_present

  terraform -chdir="$TF_DIR" destroy \
    -input=false \
    -auto-approve \
    -var-file="$TF_VARS_FILE" \
    -var "scw_project_id=$TF_PROJECT_ID"
fi

log "Verifying Terraform state is empty"
if terraform -chdir="$TF_DIR" state list 2>/dev/null | grep -q .; then
  echo "Terraform state still has resources. Run 'terraform -chdir=$TF_DIR state list' to inspect." >&2
  exit 1
fi

if [[ "$PURGE_LEFTOVERS" == "true" ]]; then
  purge_leftovers "destroy"
fi

log "Scaleway Terraform teardown complete."
