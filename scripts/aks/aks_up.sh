#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# ---------------------------------------------------------------------------
# Read infrastructure values from Terraform outputs
# Output names are cloud-agnostic; azure_* / scw_* prefixes for cloud-specific.
# ---------------------------------------------------------------------------
TF_DIR="${TF_DIR:-$ROOT_DIR/terraform}"
if [[ -d "$TF_DIR" ]] && command -v terraform >/dev/null 2>&1; then
  TF_OUTPUT="$(cd "$TF_DIR" && terraform output -json 2>/dev/null || true)"
  if [[ -n "$TF_OUTPUT" && "$TF_OUTPUT" != "{}" ]]; then
    # Active cloud (drives conditional logic below)
    CLOUD_PROVIDER="${CLOUD_PROVIDER:-$(echo "$TF_OUTPUT" | jq -r '.cloud_provider.value // "azure"')}"

    # Cross-cloud unified outputs
    AKS_CLUSTER_NAME="${AKS_CLUSTER_NAME:-$(echo "$TF_OUTPUT" | jq -r '.cluster_name.value // empty')}"
    ACR_NAME="${ACR_NAME:-$(echo "$TF_OUTPUT" | jq -r '.registry_name.value // empty')}"
    ACR_LOGIN_SERVER="${ACR_LOGIN_SERVER:-$(echo "$TF_OUTPUT" | jq -r '.registry_login_server.value // empty')}"
    INGRESS_PIP_IP="${INGRESS_PIP_IP:-$(echo "$TF_OUTPUT" | jq -r '.ingress_public_ip.value // empty')}"
    FRONTEND_DOMAIN="${FRONTEND_DOMAIN:-$(echo "$TF_OUTPUT" | jq -r '.dns_zone_name.value // empty')}"
    AKS_KEY_VAULT_NAME="${AKS_KEY_VAULT_NAME:-$(echo "$TF_OUTPUT" | jq -r '.secrets_store_name.value // empty')}"
    KUBE_CONFIG_COMMAND="${KUBE_CONFIG_COMMAND:-$(echo "$TF_OUTPUT" | jq -r '.kube_config_command.value // empty')}"

    # Azure-specific outputs
    AKS_RESOURCE_GROUP="${AKS_RESOURCE_GROUP:-$(echo "$TF_OUTPUT" | jq -r '.azure_resource_group_name.value // empty')}"
    AKS_KEY_VAULT_PROVIDER_CLIENT_ID="${AKS_KEY_VAULT_PROVIDER_CLIENT_ID:-$(echo "$TF_OUTPUT" | jq -r '.azure_kv_provider_identity_client_id.value // empty')}"
    AKS_TENANT_ID="${AKS_TENANT_ID:-$(echo "$TF_OUTPUT" | jq -r '.azure_tenant_id.value // empty')}"
    NODE_RESOURCE_GROUP="${NODE_RESOURCE_GROUP:-$(echo "$TF_OUTPUT" | jq -r '.azure_node_resource_group.value // empty')}"

    # Scaleway-specific outputs
    SCW_REGION="${SCW_REGION:-$(echo "$TF_OUTPUT" | jq -r '.scw_region.value // empty')}"
  fi
fi

# Fallback defaults (used when Terraform outputs are unavailable)
CLOUD_PROVIDER="${CLOUD_PROVIDER:-azure}"   # azure | scaleway
SCW_REGION="${SCW_REGION:-nl-ams}"
AKS_RESOURCE_GROUP="${AKS_RESOURCE_GROUP:-ai-trial-rg}"
AKS_CLUSTER_NAME="${AKS_CLUSTER_NAME:-ai-trial-aks}"
NAMESPACE="${NAMESPACE:-odp-dev}"
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
FRONTEND_DOMAIN="${FRONTEND_DOMAIN:-eu-sovereigndataplatform.com}"

AKS_VITE_KEYCLOAK_URL="${AKS_VITE_KEYCLOAK_URL:-https://keycloak.${FRONTEND_DOMAIN}}"
AKS_VITE_PORTAL_API_URL="${AKS_VITE_PORTAL_API_URL:-https://portal-api.${FRONTEND_DOMAIN}}"
AKS_VITE_DBT_DOCS_URL="${AKS_VITE_DBT_DOCS_URL:-https://dbt-docs.${FRONTEND_DOMAIN}}"
AKS_VITE_KEYCLOAK_REALM="${AKS_VITE_KEYCLOAK_REALM:-${VITE_KEYCLOAK_REALM:-odp}}"
AKS_VITE_KEYCLOAK_CLIENT_ID="${AKS_VITE_KEYCLOAK_CLIENT_ID:-${VITE_KEYCLOAK_CLIENT_ID:-portal}}"
LETSENCRYPT_EMAIL="${LETSENCRYPT_EMAIL:-karel.goense@freshminds.nl}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300s}"
AIRFLOW_INIT_JOB_TIMEOUT="${AIRFLOW_INIT_JOB_TIMEOUT:-960s}"
AIRFLOW_DEPLOYMENT_TIMEOUT="${AIRFLOW_DEPLOYMENT_TIMEOUT:-600s}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-${KUBECONFIG:-$HOME/.kube/config}}"
AKS_WAIT_RETRIES="${AKS_WAIT_RETRIES:-6}"
AKS_WAIT_RETRY_DELAY_SECONDS="${AKS_WAIT_RETRY_DELAY_SECONDS:-10}"
DATAHUB_SETUP_JOB_TIMEOUT="${DATAHUB_SETUP_JOB_TIMEOUT:-1200s}"
DATAHUB_ELASTICSEARCH_SETUP_JOB_TIMEOUT="${DATAHUB_ELASTICSEARCH_SETUP_JOB_TIMEOUT:-300s}"
AKS_USE_KEY_VAULT="${AKS_USE_KEY_VAULT:-true}"
AKS_KEY_VAULT_NAME="${AKS_KEY_VAULT_NAME:-}"
AKS_KEY_VAULT_SECRET_NAME="${AKS_KEY_VAULT_SECRET_NAME:-odp-env}"
AKS_KEY_VAULT_PROVIDER_CLASS_NAME="${AKS_KEY_VAULT_PROVIDER_CLASS_NAME:-odp-env-keyvault}"
AKS_KEY_VAULT_SYNC_DEPLOYMENT_NAME="${AKS_KEY_VAULT_SYNC_DEPLOYMENT_NAME:-odp-env-keyvault-sync}"
AKS_KEY_VAULT_SYNC_TIMEOUT_SECONDS="${AKS_KEY_VAULT_SYNC_TIMEOUT_SECONDS:-300}"
AKS_KEY_VAULT_SECRET_SET_RETRIES="${AKS_KEY_VAULT_SECRET_SET_RETRIES:-18}"
AKS_KEY_VAULT_SECRET_SET_RETRY_DELAY_SECONDS="${AKS_KEY_VAULT_SECRET_SET_RETRY_DELAY_SECONDS:-10}"
KOMPOSE_OVERRIDE_FILE="${KOMPOSE_OVERRIDE_FILE:-$ROOT_DIR/docker-compose.k8s.yml}"
AKS_HELPERS_LIB="$ROOT_DIR/scripts/aks/aks_up_lib.sh"
KOMPOSE_LIB="$ROOT_DIR/scripts/k8s/k8s_kompose_lib.sh"

MINIMAL_DEPLOY="${MINIMAL_DEPLOY:-false}"
SKIP_IMAGE_BUILD="${SKIP_IMAGE_BUILD:-false}"
for arg in "$@"; do
  case "$arg" in
    --minimal) MINIMAL_DEPLOY=true ;;
    --skip-image-build) SKIP_IMAGE_BUILD=true ;;
  esac
done

log() {
  echo "[aks-up] $*"
}

enforce_odp_env_secret_name() {
  local fixed_secret_name="odp-env"
  if [[ "$AKS_KEY_VAULT_SECRET_NAME" != "$fixed_secret_name" ]]; then
    log "AKS_KEY_VAULT_SECRET_NAME='${AKS_KEY_VAULT_SECRET_NAME}' is not supported in this flow; using '${fixed_secret_name}' because manifests reference it directly."
    AKS_KEY_VAULT_SECRET_NAME="$fixed_secret_name"
  fi
}

warn_on_legacy_vite_url_vars() {
  local legacy_var
  for legacy_var in VITE_KEYCLOAK_URL VITE_PORTAL_API_URL VITE_DBT_DOCS_URL; do
    if [[ -n "${!legacy_var:-}" ]]; then
      log "Ignoring ${legacy_var} for AKS frontend build; use AKS_${legacy_var} to override AKS public URL defaults."
    fi
  done
}

# KUBE_CONTEXT is set after kubeconfig is fetched in the cloud-specific section.
# Bash resolves variable values at call time, so the function can be defined here.
KUBE_CONTEXT="${KUBE_CONTEXT:-$AKS_CLUSTER_NAME}"
kubectl_ctx() {
  kubectl --context "$KUBE_CONTEXT" "$@"
}

current_deployment_image() {
  local deployment_name="$1"
  kubectl_ctx -n "$NAMESPACE" get deployment "$deployment_name" -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true
}

reuse_existing_image_or_fail() {
  local deployment_name="$1"
  local image_label="$2"
  local image_var_name="$3"
  local existing_image

  existing_image="$(current_deployment_image "$deployment_name")"
  if [[ -z "$existing_image" ]]; then
    log "ERROR: SKIP_IMAGE_BUILD=true but deployment '$deployment_name' has no current image to reuse." >&2
    log "       Run without SKIP_IMAGE_BUILD once, or provide explicit image tags and disable this fast path." >&2
    exit 1
  fi

  printf -v "$image_var_name" '%s' "$existing_image"
  export "$image_var_name"
  log "Reusing ${image_label} image from deployment '$deployment_name': ${existing_image}"
}

ODP_ENV_KEYS=()
ODP_ENV_VALUES=()
ODP_ENV_KEY_VAULT_SECRET_NAMES=()
ODP_ENV_SKIPPED_EMPTY_KEYS=()
ODP_ENV_SKIPPED_EMPTY_KEY_VAULT_SECRET_NAMES=()

render_and_apply() {
  local manifest="$1"
  local rendered_file
  rendered_file="$(mktemp)"

  sed \
    -e "s|__NAMESPACE__|${NAMESPACE}|g" \
    -e "s|__AIRFLOW_IMAGE__|${AIRFLOW_IMAGE}|g" \
    -e "s|__FRONTEND_IMAGE__|${FRONTEND_IMAGE}|g" \
    -e "s|__DBT_DOCS_GENERATOR_IMAGE__|${AIRFLOW_IMAGE}|g" \
    -e "s|__DBT_DOCS_BUILD_ID__|${AIRFLOW_IMAGE_TAG}|g" \
    -e "s|__FRONTEND_DOMAIN__|${FRONTEND_DOMAIN}|g" \
    -e "s|__LETSENCRYPT_EMAIL__|${LETSENCRYPT_EMAIL}|g" \
    -e "s|__AKS_KEY_VAULT_PROVIDER_CLASS_NAME__|${AKS_KEY_VAULT_PROVIDER_CLASS_NAME}|g" \
    -e "s|__AKS_KEY_VAULT_SYNC_DEPLOYMENT_NAME__|${AKS_KEY_VAULT_SYNC_DEPLOYMENT_NAME}|g" \
    "$manifest" > "$rendered_file"

  unresolved_placeholders="$(perl -ne 'while (/(?<![A-Z0-9_])(__[A-Z0-9_]+__)(?![A-Z0-9_])/g) { print "$1\n" }' "$rendered_file" | sort -u)"
  if [[ -n "$unresolved_placeholders" ]]; then
    echo "[aks-up] ERROR: unresolved template placeholder(s) in manifest: $manifest" >&2
    echo "$unresolved_placeholders" >&2
    rm -f "$rendered_file"
    return 1
  fi

  kubectl_ctx apply -f "$rendered_file"
  rm -f "$rendered_file"
}

env_key_to_key_vault_secret_name() {
  local env_key="$1"
  local kv_secret_name

  kv_secret_name="$(printf '%s' "$env_key" | tr '[:upper:]' '[:lower:]' | tr '_' '-' | tr -cd 'a-z0-9-')"
  kv_secret_name="$(printf '%s' "$kv_secret_name" | sed -E 's/-+/-/g; s/^-+//; s/-+$//')"

  printf '%s' "$kv_secret_name"
}

normalise_env_assignment_value() {
  local raw_value="$1"
  local normalised_value

  # This normalises .env assignment values (trim + one wrapping quote pair),
  # not arbitrary shell syntax; quoted inner content is preserved as-is.
  normalised_value="$(printf '%s' "$raw_value" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"
  if [[ ${#normalised_value} -ge 2 ]]; then
    if [[ "$normalised_value" == \"*\" && "$normalised_value" == *\" ]]; then
      normalised_value="${normalised_value:1:${#normalised_value}-2}"
    elif [[ "$normalised_value" == \'*\' && "$normalised_value" == *\' ]]; then
      normalised_value="${normalised_value:1:${#normalised_value}-2}"
    fi
  fi

  printf '%s' "$normalised_value"
}

load_env_entries_for_key_vault() {
  local line
  local key
  local value
  local kv_secret_name
  local existing_index
  local idx
  local i
  local j
  local filtered_keys=()
  local filtered_values=()
  local filtered_secret_names=()

  ODP_ENV_KEYS=()
  ODP_ENV_VALUES=()
  ODP_ENV_KEY_VAULT_SECRET_NAMES=()
  ODP_ENV_SKIPPED_EMPTY_KEYS=()
  ODP_ENV_SKIPPED_EMPTY_KEY_VAULT_SECRET_NAMES=()

  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "$line" || "$line" == \#* ]] && continue
    [[ "$line" != *=* ]] && continue

    key="${line%%=*}"
    value="${line#*=}"
    key="$(printf '%s' "$key" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"
    key="${key#export }"
    value="$(normalise_env_assignment_value "$value")"
    [[ -z "$key" ]] && continue

    kv_secret_name="$(env_key_to_key_vault_secret_name "$key")"
    if [[ -z "$kv_secret_name" ]]; then
      echo "[aks-up] ERROR: could not derive a valid Key Vault secret name from env key '$key'." >&2
      return 1
    fi

    existing_index=-1
    for idx in "${!ODP_ENV_KEYS[@]}"; do
      if [[ "${ODP_ENV_KEYS[$idx]}" == "$key" ]]; then
        existing_index="$idx"
        break
      fi
    done

    if (( existing_index >= 0 )); then
      ODP_ENV_VALUES[$existing_index]="$value"
      ODP_ENV_KEY_VAULT_SECRET_NAMES[$existing_index]="$kv_secret_name"
    else
      ODP_ENV_KEYS+=("$key")
      ODP_ENV_VALUES+=("$value")
      ODP_ENV_KEY_VAULT_SECRET_NAMES+=("$kv_secret_name")
    fi
  done < "$ROOT_DIR/.env"

  if [[ "${#ODP_ENV_KEYS[@]}" -eq 0 ]]; then
    echo "[aks-up] ERROR: no key/value entries found in $ROOT_DIR/.env." >&2
    return 1
  fi

  for i in "${!ODP_ENV_KEY_VAULT_SECRET_NAMES[@]}"; do
    for j in "${!ODP_ENV_KEY_VAULT_SECRET_NAMES[@]}"; do
      if (( i >= j )); then
        continue
      fi
      if [[ "${ODP_ENV_KEY_VAULT_SECRET_NAMES[$i]}" == "${ODP_ENV_KEY_VAULT_SECRET_NAMES[$j]}" && "${ODP_ENV_KEYS[$i]}" != "${ODP_ENV_KEYS[$j]}" ]]; then
        echo "[aks-up] ERROR: env keys '${ODP_ENV_KEYS[$i]}' and '${ODP_ENV_KEYS[$j]}' map to the same Key Vault secret name '${ODP_ENV_KEY_VAULT_SECRET_NAMES[$i]}'." >&2
        return 1
      fi
    done
  done

  for idx in "${!ODP_ENV_KEYS[@]}"; do
    if [[ -z "${ODP_ENV_VALUES[$idx]}" ]]; then
      ODP_ENV_SKIPPED_EMPTY_KEYS+=("${ODP_ENV_KEYS[$idx]}")
      ODP_ENV_SKIPPED_EMPTY_KEY_VAULT_SECRET_NAMES+=("${ODP_ENV_KEY_VAULT_SECRET_NAMES[$idx]}")
      continue
    fi

    filtered_keys+=("${ODP_ENV_KEYS[$idx]}")
    filtered_values+=("${ODP_ENV_VALUES[$idx]}")
    filtered_secret_names+=("${ODP_ENV_KEY_VAULT_SECRET_NAMES[$idx]}")
  done

  ODP_ENV_KEYS=("${filtered_keys[@]}")
  ODP_ENV_VALUES=("${filtered_values[@]}")
  ODP_ENV_KEY_VAULT_SECRET_NAMES=("${filtered_secret_names[@]}")

  if [[ "${#ODP_ENV_KEYS[@]}" -eq 0 ]]; then
    echo "[aks-up] ERROR: no non-empty key/value entries found in $ROOT_DIR/.env for Key Vault sync." >&2
    return 1
  fi

}

create_odp_env_secret_from_env_file() {
  kubectl_ctx -n "$NAMESPACE" create secret generic "$AKS_KEY_VAULT_SECRET_NAME" \
    --from-env-file="$ROOT_DIR/.env" \
    --dry-run=client -o yaml | kubectl_ctx apply -f -
}

render_key_vault_secret_provider_class_manifest() {
  local output_path="$1"
  local idx

  cat > "$output_path" <<EOF
apiVersion: secrets-store.csi.x-k8s.io/v1
kind: SecretProviderClass
metadata:
  name: ${AKS_KEY_VAULT_PROVIDER_CLASS_NAME}
  namespace: ${NAMESPACE}
spec:
  provider: azure
  parameters:
    usePodIdentity: "false"
    useVMManagedIdentity: "true"
    userAssignedIdentityID: "${AKS_KEY_VAULT_PROVIDER_CLIENT_ID}"
    keyvaultName: "${AKS_KEY_VAULT_NAME}"
    tenantId: "${AKS_TENANT_ID}"
    objects: |
      array:
EOF

  for idx in "${!ODP_ENV_KEYS[@]}"; do
    cat >> "$output_path" <<EOF
        - |
          objectName: ${ODP_ENV_KEY_VAULT_SECRET_NAMES[$idx]}
          objectType: secret
EOF
  done

  cat >> "$output_path" <<EOF
  secretObjects:
    - secretName: ${AKS_KEY_VAULT_SECRET_NAME}
      type: Opaque
      data:
EOF

  for idx in "${!ODP_ENV_KEYS[@]}"; do
    cat >> "$output_path" <<EOF
        - objectName: ${ODP_ENV_KEY_VAULT_SECRET_NAMES[$idx]}
          key: ${ODP_ENV_KEYS[$idx]}
EOF
  done
}

wait_for_synced_secret_key() {
  local secret_name="$1"
  local key_name="$2"
  local timeout_seconds="${3:-300}"
  local elapsed=0

  while (( elapsed < timeout_seconds )); do
    if [[ -n "$(kubectl_ctx -n "$NAMESPACE" get secret "$secret_name" -o "jsonpath={.data.${key_name}}" 2>/dev/null || true)" ]]; then
      return 0
    fi
    sleep 5
    elapsed=$((elapsed + 5))
  done

  echo "[aks-up] ERROR: timed out waiting for secret '$secret_name' key '$key_name' to be synced from Key Vault." >&2
  return 1
}

set_key_vault_secret_with_retry() {
  local vault_name="$1"
  local secret_name="$2"
  local secret_value="$3"
  local attempt=1
  local output

  while (( attempt <= AKS_KEY_VAULT_SECRET_SET_RETRIES )); do
    if output="$(az keyvault secret set --vault-name "$vault_name" --name "$secret_name" --value "$secret_value" -o none 2>&1)"; then
      return 0
    fi

    if [[ "$output" == *"ForbiddenByRbac"* || "$output" == *"Caller is not authorized to perform action on resource."* ]]; then
      if (( attempt == AKS_KEY_VAULT_SECRET_SET_RETRIES )); then
        echo "[aks-up] ERROR: failed to set Key Vault secret '$secret_name' in '$vault_name' after ${AKS_KEY_VAULT_SECRET_SET_RETRIES} attempts." >&2
        echo "[aks-up] Ensure the signed-in principal has 'Key Vault Secrets Officer' or 'Key Vault Administrator' on '$vault_name'." >&2
        echo "$output" >&2
        return 1
      fi
      log "Waiting for Key Vault RBAC propagation before writing secret '$secret_name' (attempt ${attempt}/${AKS_KEY_VAULT_SECRET_SET_RETRIES})..."
      sleep "$AKS_KEY_VAULT_SECRET_SET_RETRY_DELAY_SECONDS"
      attempt=$((attempt + 1))
      continue
    fi

    echo "$output" >&2
    return 1
  done
}

ensure_key_vault_secret_sync() {
  # Key Vault creation, addon enabling, and RBAC role assignments are managed
  # by Terraform. This function only seeds secrets from .env and sets up the
  # Kubernetes SecretProviderClass + sync deployment.
  local secret_provider_manifest
  local idx
  local kv_secret_name
  local value

  if [[ -z "$AKS_KEY_VAULT_NAME" ]]; then
    # Derive from subscription hash on Azure; on Scaleway this path should not be reached.
    AKS_KEY_VAULT_NAME="aitrialkv${SUB_HASH:-}"
  fi
  AKS_KEY_VAULT_NAME="$(echo "$AKS_KEY_VAULT_NAME" | tr '[:upper:]' '[:lower:]')"

  if [[ -z "${AKS_TENANT_ID:-}" ]]; then
    AKS_TENANT_ID="$(az account show --query tenantId -o tsv)"
  fi

  if [[ -z "${AKS_KEY_VAULT_PROVIDER_CLIENT_ID:-}" ]]; then
    AKS_KEY_VAULT_PROVIDER_CLIENT_ID="$(az aks show \
      --resource-group "$AKS_RESOURCE_GROUP" \
      --name "$AKS_CLUSTER_NAME" \
      --query addonProfiles.azureKeyvaultSecretsProvider.identity.clientId \
      -o tsv 2>/dev/null || true)"
  fi

  load_env_entries_for_key_vault

  log "Syncing ${#ODP_ENV_KEYS[@]} non-empty .env entries into Key Vault '$AKS_KEY_VAULT_NAME'..."
  if [[ "${#ODP_ENV_SKIPPED_EMPTY_KEYS[@]}" -gt 0 ]]; then
    log "Skipping ${#ODP_ENV_SKIPPED_EMPTY_KEYS[@]} empty .env entries for Key Vault sync (Azure Key Vault does not allow empty secret values): ${ODP_ENV_SKIPPED_EMPTY_KEYS[*]}"
  fi

  for idx in "${!ODP_ENV_KEYS[@]}"; do
    kv_secret_name="${ODP_ENV_KEY_VAULT_SECRET_NAMES[$idx]}"
    value="${ODP_ENV_VALUES[$idx]}"
    set_key_vault_secret_with_retry "$AKS_KEY_VAULT_NAME" "$kv_secret_name" "$value"
  done

  secret_provider_manifest="$TMP_DIR/secretproviderclass-odp-env.yaml"
  render_key_vault_secret_provider_class_manifest "$secret_provider_manifest"
  kubectl_ctx apply -f "$secret_provider_manifest"

  kubectl_ctx -n "$NAMESPACE" delete secret "$AKS_KEY_VAULT_SECRET_NAME" --ignore-not-found >/dev/null 2>&1 || true
  render_and_apply "$ROOT_DIR/k8s/aks/keyvault-sync.yaml"
  kubectl_ctx -n "$NAMESPACE" rollout restart "deployment/${AKS_KEY_VAULT_SYNC_DEPLOYMENT_NAME}" >/dev/null 2>&1 || true
  wait_for_deployment "$AKS_KEY_VAULT_SYNC_DEPLOYMENT_NAME" "${AKS_KEY_VAULT_SYNC_TIMEOUT_SECONDS}s"
  wait_for_synced_secret_key "$AKS_KEY_VAULT_SECRET_NAME" "AIRFLOW_DB_USER" "$AKS_KEY_VAULT_SYNC_TIMEOUT_SECONDS"
}

datahub_gms_has_mysql_host_auth_error() {
  kubectl_ctx -n "$NAMESPACE" logs deploy/datahub-gms --tail=250 2>/dev/null \
    | grep -E -q "Host '.*' is not allowed to connect to this MySQL server"
}

datahub_gms_has_mysql_unknown_database_error() {
  kubectl_ctx -n "$NAMESPACE" logs deploy/datahub-gms --tail=300 2>/dev/null \
    | grep -E -q "Unknown database 'datahub'"
}

datahub_setup_job_timeout() {
  local job_name="$1"
  case "$job_name" in
    datahub-elasticsearch-setup)
      echo "$DATAHUB_ELASTICSEARCH_SETUP_JOB_TIMEOUT"
      ;;
    *)
      echo "$DATAHUB_SETUP_JOB_TIMEOUT"
      ;;
  esac
}

run_datahub_setup_jobs() {
  local log_message="${1:-Running DataHub setup jobs...}"
  local job
  local job_timeout

  log "$log_message"
  for job in "${DATAHUB_SETUP_JOBS[@]}"; do
    job_timeout="$(datahub_setup_job_timeout "$job")"
    kubectl_ctx -n "$NAMESPACE" delete job "$job" --ignore-not-found
    kubectl_ctx -n "$NAMESPACE" wait --for=delete "job/${job}" --timeout=180s || true
    apply_namespaced_manifest "$ROOT_DIR/k8s/dev/${job}-job.yaml"
    wait_for_job_complete "$job" "$job_timeout"
  done
}

self_heal_datahub_mysql_host_auth() {
  local mysql_pod
  local app_user_b64
  local app_password_b64
  local app_database_b64

  mysql_pod="$(kubectl_ctx -n "$NAMESPACE" get pods -l io.kompose.service=datahub-mysql --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "$mysql_pod" ]]; then
    echo "[aks-up] ERROR: datahub-mysql pod not found; cannot self-heal DataHub MySQL host grants." >&2
    return 1
  fi

  app_user_b64="$(kubectl_ctx -n "$NAMESPACE" get secret "$AKS_KEY_VAULT_SECRET_NAME" -o jsonpath='{.data.DATAHUB_MYSQL_USER}' 2>/dev/null || true)"
  app_password_b64="$(kubectl_ctx -n "$NAMESPACE" get secret "$AKS_KEY_VAULT_SECRET_NAME" -o jsonpath='{.data.DATAHUB_MYSQL_PASSWORD}' 2>/dev/null || true)"
  app_database_b64="$(kubectl_ctx -n "$NAMESPACE" get secret "$AKS_KEY_VAULT_SECRET_NAME" -o jsonpath='{.data.DATAHUB_MYSQL_DATABASE}' 2>/dev/null || true)"
  if [[ -z "$app_user_b64" || -z "$app_password_b64" || -z "$app_database_b64" ]]; then
    echo "[aks-up] ERROR: DATAHUB_MYSQL_USER/DATAHUB_MYSQL_PASSWORD/DATAHUB_MYSQL_DATABASE missing in '$AKS_KEY_VAULT_SECRET_NAME' secret; cannot self-heal DataHub MySQL host grants." >&2
    return 1
  fi

  log "Applying DataHub MySQL host-auth self-heal (creating/updating app-user grant for DataHub schema) ..."
  # Build CREATE/ALTER/GRANT dynamically with QUOTE(...) and escaped schema
  # identifiers so secret values with special characters remain safe.
  kubectl_ctx -n "$NAMESPACE" exec "pod/${mysql_pod}" -- sh -lc "mysql -uroot -e \"SET @app_user = CAST(FROM_BASE64('${app_user_b64}') AS CHAR); SET @app_pw = CAST(FROM_BASE64('${app_password_b64}') AS CHAR); SET @app_db = CAST(FROM_BASE64('${app_database_b64}') AS CHAR); SET @app_host = '%'; SET @escaped_db = REPLACE(@app_db, CHAR(96), CONCAT(CHAR(96), CHAR(96))); SET @create_stmt = CONCAT('CREATE USER IF NOT EXISTS ', QUOTE(@app_user), '@', QUOTE(@app_host), ' IDENTIFIED WITH mysql_native_password BY ', QUOTE(@app_pw)); PREPARE stmt FROM @create_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt; SET @alter_stmt = CONCAT('ALTER USER ', QUOTE(@app_user), '@', QUOTE(@app_host), ' IDENTIFIED WITH mysql_native_password BY ', QUOTE(@app_pw)); PREPARE stmt FROM @alter_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt; SET @grant_stmt = CONCAT('GRANT ALL PRIVILEGES ON ', CHAR(96), @escaped_db, CHAR(96), '.* TO ', QUOTE(@app_user), '@', QUOTE(@app_host)); PREPARE stmt FROM @grant_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt; FLUSH PRIVILEGES;\""

  log "Restarting datahub-gms deployment after MySQL host-auth self-heal..."
  kubectl_ctx -n "$NAMESPACE" rollout restart deployment/datahub-gms
  wait_for_deployment datahub-gms "600s"
}

self_heal_datahub_mysql_missing_database() {
  local mysql_pod
  local app_user_b64
  local app_password_b64
  local app_database_b64

  mysql_pod="$(kubectl_ctx -n "$NAMESPACE" get pods -l io.kompose.service=datahub-mysql --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "$mysql_pod" ]]; then
    echo "[aks-up] ERROR: datahub-mysql pod not found; cannot self-heal missing DataHub database." >&2
    return 1
  fi

  app_user_b64="$(kubectl_ctx -n "$NAMESPACE" get secret "$AKS_KEY_VAULT_SECRET_NAME" -o jsonpath='{.data.DATAHUB_MYSQL_USER}' 2>/dev/null || true)"
  app_password_b64="$(kubectl_ctx -n "$NAMESPACE" get secret "$AKS_KEY_VAULT_SECRET_NAME" -o jsonpath='{.data.DATAHUB_MYSQL_PASSWORD}' 2>/dev/null || true)"
  app_database_b64="$(kubectl_ctx -n "$NAMESPACE" get secret "$AKS_KEY_VAULT_SECRET_NAME" -o jsonpath='{.data.DATAHUB_MYSQL_DATABASE}' 2>/dev/null || true)"
  if [[ -z "$app_user_b64" || -z "$app_password_b64" || -z "$app_database_b64" ]]; then
    echo "[aks-up] ERROR: DATAHUB_MYSQL_USER/DATAHUB_MYSQL_PASSWORD/DATAHUB_MYSQL_DATABASE missing in '$AKS_KEY_VAULT_SECRET_NAME' secret; cannot self-heal missing DataHub database." >&2
    return 1
  fi

  log "Applying DataHub MySQL missing-database self-heal (create schema + app-user grant with secret-backed credentials) ..."
  # Build CREATE DATABASE/USER/GRANT dynamically with escaped schema
  # identifiers and QUOTE(...) so special characters remain safe.
  kubectl_ctx -n "$NAMESPACE" exec "pod/${mysql_pod}" -- sh -lc "mysql -uroot -e \"SET @app_user = CAST(FROM_BASE64('${app_user_b64}') AS CHAR); SET @app_pw = CAST(FROM_BASE64('${app_password_b64}') AS CHAR); SET @app_db = CAST(FROM_BASE64('${app_database_b64}') AS CHAR); SET @app_host = '%'; SET @escaped_db = REPLACE(@app_db, CHAR(96), CONCAT(CHAR(96), CHAR(96))); SET @create_db_stmt = CONCAT('CREATE DATABASE IF NOT EXISTS ', CHAR(96), @escaped_db, CHAR(96), ' CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci'); PREPARE stmt FROM @create_db_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt; SET @create_user_stmt = CONCAT('CREATE USER IF NOT EXISTS ', QUOTE(@app_user), '@', QUOTE(@app_host), ' IDENTIFIED WITH mysql_native_password BY ', QUOTE(@app_pw)); PREPARE stmt FROM @create_user_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt; SET @alter_user_stmt = CONCAT('ALTER USER ', QUOTE(@app_user), '@', QUOTE(@app_host), ' IDENTIFIED WITH mysql_native_password BY ', QUOTE(@app_pw)); PREPARE stmt FROM @alter_user_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt; SET @grant_stmt = CONCAT('GRANT ALL PRIVILEGES ON ', CHAR(96), @escaped_db, CHAR(96), '.* TO ', QUOTE(@app_user), '@', QUOTE(@app_host)); PREPARE stmt FROM @grant_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt; FLUSH PRIVILEGES;\""

  run_datahub_setup_jobs "Re-running DataHub setup jobs after MySQL schema self-heal..."

  log "Restarting datahub-gms deployment after MySQL schema self-heal..."
  kubectl_ctx -n "$NAMESPACE" rollout restart deployment/datahub-gms
  wait_for_deployment datahub-gms "600s"
}

if [[ ! -f "$AKS_HELPERS_LIB" ]]; then
  echo "Missing AKS helper library: $AKS_HELPERS_LIB" >&2
  exit 1
fi

# shellcheck source=scripts/aks/aks_up_lib.sh
source "$AKS_HELPERS_LIB"

require_cmd kubectl
require_cmd docker
require_cmd curl
require_cmd openssl
require_cmd kompose
require_cmd yq
require_cmd jq
# Cloud-specific CLI tools
if [[ "$CLOUD_PROVIDER" == "azure" ]]; then
  require_cmd az
elif [[ "$CLOUD_PROVIDER" == "scaleway" ]]; then
  :
else
  echo "[aks-up] ERROR: Unsupported CLOUD_PROVIDER='$CLOUD_PROVIDER'. Must be 'azure' or 'scaleway'." >&2
  exit 1
fi

if [[ ! -f "$KOMPOSE_LIB" ]]; then
  echo "Missing kompose shared library: $KOMPOSE_LIB" >&2
  exit 1
fi

# shellcheck source=scripts/k8s/k8s_kompose_lib.sh
source "$KOMPOSE_LIB"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

if [[ ! -f "$ROOT_DIR/.env" ]]; then
  echo "Missing $ROOT_DIR/.env. Create it first (for example: cp .env.template .env)." >&2
  exit 1
fi

enforce_odp_env_secret_name

# ---------------------------------------------------------------------------
# Resolve infrastructure values and configure kubeconfig / registry login
# Branches on CLOUD_PROVIDER (azure | scaleway).
# ---------------------------------------------------------------------------
export KUBECONFIG="$KUBECONFIG_PATH"
mkdir -p "$(dirname "$KUBECONFIG_PATH")"

if [[ "$CLOUD_PROVIDER" == "azure" ]]; then
  # ---- Azure: derive ACR name from subscription hash if not set by TF ----
  SUBSCRIPTION_ID="$(az account show --query id -o tsv)"
  SUB_HASH="$(echo "$SUBSCRIPTION_ID" | tr -d '-' | cut -c1-8)"
  ACR_NAME="${ACR_NAME:-aitrial${SUB_HASH}}"
  ACR_LOGIN_SERVER="${ACR_LOGIN_SERVER:-${ACR_NAME}.azurecr.io}"

  # Ingress IP fallback — read from AKS node resource group if TF output missing
  if [[ -z "${INGRESS_PIP_IP:-}" ]]; then
    NODE_RESOURCE_GROUP="${NODE_RESOURCE_GROUP:-$(az aks show \
      --resource-group "$AKS_RESOURCE_GROUP" \
      --name "$AKS_CLUSTER_NAME" \
      --query nodeResourceGroup -o tsv)}"
    INGRESS_PIP_IP="$(az network public-ip show \
      --resource-group "$NODE_RESOURCE_GROUP" \
      --name "${INGRESS_PIP_NAME:-ai-trial-ingress-pip}" \
      --query ipAddress -o tsv)"
  fi

  log "Using Azure subscription: $(az account show --query name -o tsv)"
  log "Infrastructure managed by Terraform — skipping resource provisioning."
  log "Ingress Public IP: $INGRESS_PIP_IP"

  # Kubeconfig for the AKS cluster
  log "Fetching kubectl credentials for AKS cluster '$AKS_CLUSTER_NAME'..."
  az aks get-credentials \
    --resource-group "$AKS_RESOURCE_GROUP" \
    --name "$AKS_CLUSTER_NAME" \
    --overwrite-existing \
    -o none
  kubectl config use-context "$AKS_CLUSTER_NAME" >/dev/null
  KUBE_CONTEXT="$AKS_CLUSTER_NAME"

  # ACR login
  log "Logging in to ACR '$ACR_NAME'..."
  az acr login --name "$ACR_NAME" -o none

elif [[ "$CLOUD_PROVIDER" == "scaleway" ]]; then
  # ---- Scaleway: registry + kubeconfig from Terraform outputs ----
  if [[ -z "${ACR_LOGIN_SERVER:-}" ]]; then
    log "ERROR: registry_login_server not available from Terraform outputs." >&2
    log "       Run 'make tf-apply ENVIRONMENT=scaleway-dev' first." >&2
    exit 1
  fi
  if [[ -z "${INGRESS_PIP_IP:-}" ]]; then
    log "ERROR: ingress_public_ip not available from Terraform outputs." >&2
    log "       Run 'make tf-apply ENVIRONMENT=scaleway-dev' first." >&2
    exit 1
  fi

  log "Using Scaleway cloud (region: $SCW_REGION)"
  log "Infrastructure managed by Terraform — skipping resource provisioning."
  log "Ingress Public IP: $INGRESS_PIP_IP"

  # Kubeconfig via Scaleway CLI (uses cluster ID from kube_config_command output)
  log "Fetching kubectl credentials for Scaleway cluster '$AKS_CLUSTER_NAME'..."
  if [[ -n "${KUBE_CONFIG_COMMAND:-}" ]]; then
    eval "$KUBE_CONFIG_COMMAND" >/dev/null
  else
    require_cmd scw
    log "WARNING: kube_config_command not available; cannot auto-configure kubectl." >&2
    log "         Set KUBE_CONTEXT manually or run 'make tf-apply' first." >&2
  fi
  # After 'scw k8s kubeconfig install', the current context is set to the cluster's context.
  KUBE_CONTEXT="${KUBE_CONTEXT:-$(kubectl config current-context 2>/dev/null || echo "$AKS_CLUSTER_NAME")}"

  # Scaleway Container Registry login (username is always "nologin", password = secret key)
  if [[ -z "${SCW_SECRET_KEY:-}" ]] && command -v scw >/dev/null 2>&1; then
    SCW_SECRET_KEY="$(scw config get secret-key 2>/dev/null || true)"
  fi
  if [[ -z "${SCW_SECRET_KEY:-}" ]]; then
    log "ERROR: SCW_SECRET_KEY is not set and could not be read from Scaleway CLI config." >&2
    log "       Set SCW_SECRET_KEY env var, or install/configure 'scw' via 'scw init'." >&2
    exit 1
  fi
  log "Logging in to Scaleway Container Registry '$ACR_LOGIN_SERVER'..."
  echo "${SCW_SECRET_KEY}" | docker login "$ACR_LOGIN_SERVER" -u nologin --password-stdin
fi

AIRFLOW_IMAGE="${ACR_LOGIN_SERVER}/${AIRFLOW_IMAGE_REPO}:${AIRFLOW_IMAGE_TAG}"
FRONTEND_IMAGE="${ACR_LOGIN_SERVER}/${FRONTEND_IMAGE_REPO}:${FRONTEND_IMAGE_TAG}"
PORTAL_API_IMAGE="${ACR_LOGIN_SERVER}/${PORTAL_API_IMAGE_REPO}:${PORTAL_API_IMAGE_TAG}"
JUPYTER_IMAGE="${ACR_LOGIN_SERVER}/${JUPYTER_IMAGE_REPO}:${JUPYTER_IMAGE_TAG}"
MINIO_SSO_BRIDGE_IMAGE="${ACR_LOGIN_SERVER}/${MINIO_SSO_BRIDGE_IMAGE_REPO}:${MINIO_SSO_BRIDGE_IMAGE_TAG}"

warn_on_legacy_vite_url_vars

if [[ "$SKIP_IMAGE_BUILD" == "true" ]]; then
  log "SKIP_IMAGE_BUILD=true: skipping Docker build/push and reusing currently deployed images."
  reuse_existing_image_or_fail "airflow-webserver" "Airflow" "AIRFLOW_IMAGE"
  reuse_existing_image_or_fail "portal" "Frontend" "FRONTEND_IMAGE"
  reuse_existing_image_or_fail "portal-api" "Portal API" "PORTAL_API_IMAGE"
  if [[ "$MINIMAL_DEPLOY" != "true" ]]; then
    reuse_existing_image_or_fail "jupyter" "Jupyter" "JUPYTER_IMAGE"
  fi
  reuse_existing_image_or_fail "minio-sso-bridge" "MinIO SSO bridge" "MINIO_SSO_BRIDGE_IMAGE"
else
  build_and_push_image "$AIRFLOW_IMAGE" "$ROOT_DIR/airflow/Dockerfile" "$ROOT_DIR" "Airflow"
  build_and_push_image "$FRONTEND_IMAGE" "$ROOT_DIR/frontend/Dockerfile.k8s" "$ROOT_DIR/frontend" "Frontend" \
    --build-arg "VITE_KEYCLOAK_URL=${AKS_VITE_KEYCLOAK_URL}" \
    --build-arg "VITE_KEYCLOAK_REALM=${AKS_VITE_KEYCLOAK_REALM}" \
    --build-arg "VITE_KEYCLOAK_CLIENT_ID=${AKS_VITE_KEYCLOAK_CLIENT_ID}" \
    --build-arg "VITE_PORTAL_API_URL=${AKS_VITE_PORTAL_API_URL}" \
    --build-arg "VITE_DBT_DOCS_URL=${AKS_VITE_DBT_DOCS_URL}"
  build_and_push_image "$PORTAL_API_IMAGE" "$ROOT_DIR/ops/portal-api/Dockerfile" "$ROOT_DIR" "Portal API"
  if [[ "$MINIMAL_DEPLOY" != "true" ]]; then
    build_and_push_image "$JUPYTER_IMAGE" "$ROOT_DIR/notebooks/Dockerfile" "$ROOT_DIR/notebooks" "Jupyter"
  fi
  build_and_push_image "$MINIO_SSO_BRIDGE_IMAGE" "$ROOT_DIR/ops/minio-sso-bridge/Dockerfile" "$ROOT_DIR" "MinIO SSO bridge"
fi

log "Applying namespace..."
render_and_apply "$ROOT_DIR/k8s/aks/namespace.yaml"

if [[ "$CLOUD_PROVIDER" == "azure" && "$AKS_USE_KEY_VAULT" == "true" ]]; then
  log "Creating/updating Kubernetes secret '$AKS_KEY_VAULT_SECRET_NAME' from Azure Key Vault..."
  ensure_key_vault_secret_sync
elif [[ "$CLOUD_PROVIDER" == "scaleway" ]]; then
  # On Scaleway, secrets are synced by external-secrets-operator (ESO) using the
  # IAM API key that Terraform provisioned. ESO must be installed separately.
  # Direct seeding from .env is used as a fallback / initial seed here.
  log "Scaleway: creating/updating Kubernetes secret '$AKS_KEY_VAULT_SECRET_NAME' directly from .env..."
  log "         (external-secrets-operator will sync from Scaleway Secret Manager at runtime)"
  create_odp_env_secret_from_env_file
else
  log "AKS_USE_KEY_VAULT=false; creating/updating Kubernetes secret '$AKS_KEY_VAULT_SECRET_NAME' directly from .env..."
  create_odp_env_secret_from_env_file
fi

if [[ -z "$(kubectl_ctx -n "$NAMESPACE" get secret "$AKS_KEY_VAULT_SECRET_NAME" -o jsonpath='{.data.AIRFLOW_OAUTH_DEFAULT_ROLE}' 2>/dev/null || true)" ]]; then
  log "AIRFLOW_OAUTH_DEFAULT_ROLE missing in .env; defaulting '$AKS_KEY_VAULT_SECRET_NAME' secret value to 'Viewer' (least privilege)."
  kubectl_ctx -n "$NAMESPACE" patch secret "$AKS_KEY_VAULT_SECRET_NAME" --type merge -p '{"stringData":{"AIRFLOW_OAUTH_DEFAULT_ROLE":"Viewer"}}'
  if [[ "$CLOUD_PROVIDER" == "azure" && "$AKS_USE_KEY_VAULT" == "true" ]]; then
    set_key_vault_secret_with_retry \
      "$AKS_KEY_VAULT_NAME" \
      "$(env_key_to_key_vault_secret_name AIRFLOW_OAUTH_DEFAULT_ROLE)" \
      "Viewer"
  fi
fi

log "Creating/updating Airflow webserver config ConfigMap..."
kubectl_ctx -n "$NAMESPACE" create configmap airflow-webserver-config \
  --from-file=webserver_config.py="$ROOT_DIR/airflow/webserver_config.py" \
  --dry-run=client -o yaml | kubectl_ctx apply -f -

if [[ "$MINIMAL_DEPLOY" != "true" ]]; then
  log "Creating/updating Alertmanager config ConfigMap..."
  kubectl_ctx -n "$NAMESPACE" create configmap alertmanager-config \
    --from-file=alertmanager.yml="$ROOT_DIR/ops/observability/alertmanager.yml" \
    --dry-run=client -o yaml | kubectl_ctx apply -f -

  log "Creating/updating observability config ConfigMaps..."
  kubectl_ctx -n "$NAMESPACE" create configmap loki-config \
    --from-file=local-config.yaml="$ROOT_DIR/ops/observability/loki.yml" \
    --dry-run=client -o yaml | kubectl_ctx apply -f -
  kubectl_ctx -n "$NAMESPACE" create configmap prometheus-config \
    --from-file=prometheus.yml="$ROOT_DIR/ops/observability/prometheus.yml" \
    --from-file=alerts.yml="$ROOT_DIR/ops/observability/alerts.yml" \
    --dry-run=client -o yaml | kubectl_ctx apply -f -
  kubectl_ctx -n "$NAMESPACE" create configmap promtail-config \
    --from-file=config.yml="$ROOT_DIR/ops/observability/promtail.yml" \
    --dry-run=client -o yaml | kubectl_ctx apply -f -
  kubectl_ctx -n "$NAMESPACE" create configmap tempo-config \
    --from-file=tempo.yml="$ROOT_DIR/ops/observability/tempo.yml" \
    --dry-run=client -o yaml | kubectl_ctx apply -f -
  kubectl_ctx -n "$NAMESPACE" create configmap otel-collector-config \
    --from-file=otel-collector.yml="$ROOT_DIR/ops/observability/otel-collector.yml" \
    --dry-run=client -o yaml | kubectl_ctx apply -f -
  kubectl_ctx -n "$NAMESPACE" create configmap grafana-config \
    --from-file=datasources.yml="$ROOT_DIR/ops/observability/grafana/datasources.yml" \
    --from-file=dashboards.yml="$ROOT_DIR/ops/observability/grafana/dashboards.yml" \
    --dry-run=client -o yaml | kubectl_ctx apply -f -
  kubectl_ctx -n "$NAMESPACE" create configmap grafana-dashboards \
    --from-file=data_quality.json="$ROOT_DIR/ops/observability/grafana/dashboards/data_quality.json" \
    --from-file=platform_overview.json="$ROOT_DIR/ops/observability/grafana/dashboards/platform_overview.json" \
    --dry-run=client -o yaml | kubectl_ctx apply -f -
fi

log "Applying core services (postgres, warehouse, minio, keycloak)..."
render_and_apply "$ROOT_DIR/k8s/aks/postgres-airflow.yaml"
render_and_apply "$ROOT_DIR/k8s/aks/warehouse.yaml"
render_and_apply "$ROOT_DIR/k8s/aks/minio.yaml"
render_and_apply "$ROOT_DIR/k8s/aks/keycloak.yaml"

log "Refreshing warehouse pod to clear any stale/corrupt ephemeral database state..."
kubectl_ctx -n "$NAMESPACE" rollout restart deployment/warehouse || true

log "Waiting for core deployments..."
for deployment in postgres warehouse minio keycloak; do
  wait_for_deployment "$deployment" "$WAIT_TIMEOUT"
done

log "Running MinIO bucket init job..."
kubectl_ctx -n "$NAMESPACE" delete job minio-create-buckets --ignore-not-found
render_and_apply "$ROOT_DIR/k8s/aks/minio-create-buckets-job.yaml"
wait_for_job_complete "minio-create-buckets" "$WAIT_TIMEOUT"

log "Running Airflow init job..."
kubectl_ctx -n "$NAMESPACE" delete job airflow-init --ignore-not-found
render_and_apply "$ROOT_DIR/k8s/aks/airflow-init-job.yaml"
wait_for_job_complete "airflow-init" "$AIRFLOW_INIT_JOB_TIMEOUT"

log "Applying Airflow webserver + scheduler..."
log "Recreating airflow-webserver deployment to avoid stale env merge conflicts from prior manifests..."
kubectl_ctx -n "$NAMESPACE" delete deployment airflow-webserver --ignore-not-found
kubectl_ctx -n "$NAMESPACE" wait --for=delete deployment/airflow-webserver --timeout=120s || true
render_and_apply "$ROOT_DIR/k8s/aks/airflow-webserver.yaml"
render_and_apply "$ROOT_DIR/k8s/aks/airflow-scheduler.yaml"

log "Waiting for Airflow deployments..."
if ! wait_for_deployment "airflow-webserver" "$AIRFLOW_DEPLOYMENT_TIMEOUT"; then
  if kubectl_ctx -n "$NAMESPACE" logs deploy/airflow-webserver --tail=200 2>/dev/null | grep -q "You need to initialize the database"; then
    log "Detected uninitialized Airflow metadata DB during webserver rollout; rerunning airflow-init and retrying Airflow deployments once..."
    kubectl_ctx -n "$NAMESPACE" delete job airflow-init --ignore-not-found
    render_and_apply "$ROOT_DIR/k8s/aks/airflow-init-job.yaml"
    wait_for_job_complete "airflow-init" "$AIRFLOW_INIT_JOB_TIMEOUT"

    kubectl_ctx -n "$NAMESPACE" rollout restart deployment/airflow-webserver deployment/airflow-scheduler
    wait_for_deployment "airflow-webserver" "$AIRFLOW_DEPLOYMENT_TIMEOUT"
    wait_for_deployment "airflow-scheduler" "$AIRFLOW_DEPLOYMENT_TIMEOUT"
  else
    exit 1
  fi
else
  wait_for_deployment "airflow-scheduler" "$AIRFLOW_DEPLOYMENT_TIMEOUT"
fi

NODE_ARCH="$(kubectl_ctx get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}' 2>/dev/null || echo unknown)"
SKIP_MSTEAMS=false
if [[ "$NODE_ARCH" == "arm64" ]]; then
  SKIP_MSTEAMS=true
  log "Detected arm64 AKS node architecture; prometheus-msteams image is amd64-only and will be skipped."
fi

log "Generating full-stack manifests from docker-compose for AKS parity..."
KOMPOSE_OUT_DIR="$TMP_DIR"
KOMPOSE_OVERRIDE="$KOMPOSE_OVERRIDE_FILE"
KOMPOSE_LOG_SOURCE="aks-up"
export ROOT_DIR KOMPOSE_OUT_DIR KOMPOSE_OVERRIDE SKIP_MSTEAMS NAMESPACE KOMPOSE_LOG_SOURCE
export FRONTEND_IMAGE PORTAL_API_IMAGE JUPYTER_IMAGE MINIO_SSO_BRIDGE_IMAGE

kompose_generate
kompose_remove_phase_a
kompose_postprocess_aks
kompose_normalise_services
kompose_fix_deployments

if [[ "$MINIMAL_DEPLOY" == "true" ]]; then
  log "Minimal deploy: removing DataHub, heavy observability, and jupyter manifests..."
  kompose_remove_non_essential
fi

if [[ "$MINIMAL_DEPLOY" != "true" ]]; then
  GMS_MANIFEST="$TMP_DIR/datahub-gms-deployment.yaml"
  FRONTEND_MANIFEST="$TMP_DIR/datahub-frontend-deployment.yaml"
  kompose_hold_datahub
fi

log "Applying AKS ${MINIMAL_DEPLOY:+minimal }manifests..."
kubectl_ctx -n "$NAMESPACE" apply -f "$TMP_DIR"

if [[ "$MINIMAL_DEPLOY" != "true" ]]; then
  if [[ "$SKIP_MSTEAMS" == "true" ]]; then
    kubectl_ctx -n "$NAMESPACE" delete deployment prometheus-msteams service prometheus-msteams --ignore-not-found
  fi

  log "Waiting for DataHub dependencies..."
  for deployment in "${DATAHUB_DEPS[@]}"; do
    wait_for_deployment "$deployment" "600s"
  done

  run_datahub_setup_jobs

  HAS_GMS_HOLD=false
  HAS_FRONTEND_HOLD=false
  [[ -f "$TMP_DIR/.datahub-gms-deployment.hold" ]] && HAS_GMS_HOLD=true
  [[ -f "$TMP_DIR/.datahub-frontend-deployment.hold" ]] && HAS_FRONTEND_HOLD=true

  if [[ "$HAS_GMS_HOLD" == "true" || "$HAS_FRONTEND_HOLD" == "true" ]]; then
    kompose_restore_datahub
  fi

  if [[ "$HAS_GMS_HOLD" == "true" ]]; then
    kubectl_ctx -n "$NAMESPACE" apply -f "$GMS_MANIFEST"
  fi
  if [[ "$HAS_FRONTEND_HOLD" == "true" ]]; then
    kubectl_ctx -n "$NAMESPACE" apply -f "$FRONTEND_MANIFEST"
  fi
fi

log "Deploying AKS dbt-docs service (regenerated at rollout via initContainer)..."
render_and_apply "$ROOT_DIR/k8s/aks/dbt-docs.yaml"

if [[ "$MINIMAL_DEPLOY" == "true" ]]; then
  ACTIVE_DEPLOYMENTS=("${MINIMAL_DEPLOYMENTS[@]}")
else
  ACTIVE_DEPLOYMENTS=("${EXTENDED_DEPLOYMENTS[@]}")
fi

log "Waiting for ${MINIMAL_DEPLOY:+minimal }deployments..."
datahub_gms_heal_attempted=false
for deployment in "${ACTIVE_DEPLOYMENTS[@]}" dbt-docs; do
  if [[ "$deployment" == "datahub-gms" ]]; then
    if ! wait_for_deployment "$deployment" "600s"; then
      if [[ "$datahub_gms_heal_attempted" == "false" ]]; then
        datahub_gms_heal_attempted=true
        if datahub_gms_has_mysql_host_auth_error; then
          log "Detected DataHub GMS MySQL host-auth failure; attempting one-time self-heal..."
          self_heal_datahub_mysql_host_auth
        elif datahub_gms_has_mysql_unknown_database_error; then
          log "Detected DataHub GMS missing-MySQL-schema failure; attempting one-time self-heal..."
          self_heal_datahub_mysql_missing_database
        else
          exit 1
        fi
      else
        exit 1
      fi
    fi
  else
    wait_for_deployment "$deployment" "600s"
  fi
done

if [[ "$MINIMAL_DEPLOY" != "true" && "$SKIP_MSTEAMS" == "false" ]]; then
  wait_for_deployment prometheus-msteams "600s"
fi

log "Applying cert issuer + ingress..."
render_and_apply "$ROOT_DIR/k8s/aks/cert-issuer-letsencrypt-prod.yaml"
if [[ "$MINIMAL_DEPLOY" == "true" ]]; then
  render_and_apply "$ROOT_DIR/k8s/aks/frontend-ingress-minimal.yaml"
else
  render_and_apply "$ROOT_DIR/k8s/aks/frontend-ingress.yaml"
  render_and_apply "$ROOT_DIR/k8s/aks/keycloak-ingress.yaml"
  render_and_apply "$ROOT_DIR/k8s/aks/datahub-ingress.yaml"
fi
render_and_apply "$ROOT_DIR/k8s/aks/minio-sso-login-ingress.yaml"

log "Waiting for TLS certificate to be Ready..."
kubectl_ctx -n "$NAMESPACE" wait --for=condition=Ready certificate/frontend-tls --timeout=600s
if [[ "$MINIMAL_DEPLOY" != "true" ]]; then
  kubectl_ctx -n "$NAMESPACE" wait --for=condition=Ready certificate/keycloak-tls --timeout=600s
fi

log "Smoke test (bypass DNS with --resolve)..."
curl -sS -o /dev/null -D - --resolve "${FRONTEND_DOMAIN}:80:${INGRESS_PIP_IP}" "http://${FRONTEND_DOMAIN}" | head -n 1
curl -sS -o /dev/null -D - --resolve "${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://${FRONTEND_DOMAIN}" | head -n 1
curl -sS -o /dev/null -D - --resolve "airflow.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://airflow.${FRONTEND_DOMAIN}/health" | head -n 1
curl -sS -o /dev/null -D - --resolve "minio.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://minio.${FRONTEND_DOMAIN}/" | head -n 1
curl -sS -o /dev/null -D - --resolve "minio-api.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://minio-api.${FRONTEND_DOMAIN}/minio/health/live" | head -n 1
curl -sS -o /dev/null -D - --resolve "keycloak.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://keycloak.${FRONTEND_DOMAIN}/" | head -n 1
curl -sS -o /dev/null -D - --resolve "superset.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://superset.${FRONTEND_DOMAIN}/health" | head -n 1
curl -sS -o /dev/null -D - --resolve "dbt-docs.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://dbt-docs.${FRONTEND_DOMAIN}/" | head -n 1
if [[ "$MINIMAL_DEPLOY" != "true" ]]; then
  curl -sS -o /dev/null -D - --resolve "datahub.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://datahub.${FRONTEND_DOMAIN}/" | head -n 1
  curl -sS -o /dev/null -D - --resolve "grafana.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://grafana.${FRONTEND_DOMAIN}/api/health" | head -n 1
  curl -sS -o /dev/null -D - --resolve "prometheus.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://prometheus.${FRONTEND_DOMAIN}/-/healthy" | head -n 1
fi
echo | openssl s_client -servername "${FRONTEND_DOMAIN}" -connect "${INGRESS_PIP_IP}:443" 2>/dev/null | openssl x509 -noout -subject -issuer | sed -n '1,2p'

if [[ "$CLOUD_PROVIDER" == "azure" && "$AKS_USE_KEY_VAULT" == "true" ]]; then
  SECRET_SOURCE_SUMMARY="Azure Key Vault (${AKS_KEY_VAULT_NAME}) -> Kubernetes secret ${AKS_KEY_VAULT_SECRET_NAME}"
elif [[ "$CLOUD_PROVIDER" == "scaleway" ]]; then
  SECRET_SOURCE_SUMMARY="Scaleway Secret Manager (${AKS_KEY_VAULT_NAME}) + .env seed -> Kubernetes secret ${AKS_KEY_VAULT_SECRET_NAME}"
else
  SECRET_SOURCE_SUMMARY=".env -> Kubernetes secret ${AKS_KEY_VAULT_SECRET_NAME} (AKS_USE_KEY_VAULT=false)"
fi

if [[ "$CLOUD_PROVIDER" == "azure" ]]; then
  CLEANUP_CMD="  az group delete --name ${AKS_RESOURCE_GROUP} --yes --no-wait"
else
  CLEANUP_CMD="  make tf-destroy ENVIRONMENT=scaleway-dev   # or your active environment"
fi

DEPLOY_MODE="full"
[[ "$MINIMAL_DEPLOY" == "true" ]] && DEPLOY_MODE="minimal"

cat <<EOT

Cluster deployment is up! (cloud: $CLOUD_PROVIDER, mode: $DEPLOY_MODE)

Cluster:       $AKS_CLUSTER_NAME
Namespace:     $NAMESPACE
Secret source: $SECRET_SOURCE_SUMMARY
Airflow image: $AIRFLOW_IMAGE
Frontend URL:  https://$FRONTEND_DOMAIN
Airflow URL:   https://airflow.$FRONTEND_DOMAIN
MinIO URL:     https://minio.$FRONTEND_DOMAIN
MinIO API URL: https://minio-api.$FRONTEND_DOMAIN
Keycloak URL:  https://keycloak.$FRONTEND_DOMAIN
Superset URL:  https://superset.$FRONTEND_DOMAIN
dbt Docs URL:  https://dbt-docs.$FRONTEND_DOMAIN
Portal API URL:https://portal-api.$FRONTEND_DOMAIN
EOT

if [[ "$MINIMAL_DEPLOY" != "true" ]]; then
  cat <<EOT
DataHub URL:   https://datahub.$FRONTEND_DOMAIN
Grafana URL:   https://grafana.$FRONTEND_DOMAIN
Jupyter URL:   https://jupyter.$FRONTEND_DOMAIN
Prometheus URL:https://prometheus.$FRONTEND_DOMAIN
EOT
fi

cat <<EOT

Access services with port-forward from your machine:
  kubectl -n $NAMESPACE port-forward svc/airflow-webserver 8080:8080
  kubectl -n $NAMESPACE port-forward svc/minio 9000:9000 9001:9001
  kubectl -n $NAMESPACE port-forward svc/warehouse 5433:5432

Cleanup when done (to avoid costs):
$CLEANUP_CMD

EOT
