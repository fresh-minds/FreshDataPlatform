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
DNS_RESOURCE_GROUP="${DNS_RESOURCE_GROUP:-$AKS_RESOURCE_GROUP}"
LETSENCRYPT_EMAIL="${LETSENCRYPT_EMAIL:-karel.goense@freshminds.nl}"
INGRESS_PIP_NAME="${INGRESS_PIP_NAME:-ai-trial-ingress-pip}"
INGRESS_NGINX_VERSION="${INGRESS_NGINX_VERSION:-controller-v1.14.3}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.19.3}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300s}"
AIRFLOW_INIT_JOB_TIMEOUT="${AIRFLOW_INIT_JOB_TIMEOUT:-960s}"
AIRFLOW_DEPLOYMENT_TIMEOUT="${AIRFLOW_DEPLOYMENT_TIMEOUT:-600s}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-${KUBECONFIG:-$HOME/.kube/config}}"
AKS_FORCE_ATTACH_ACR="${AKS_FORCE_ATTACH_ACR:-false}"
AKS_WAIT_RETRIES="${AKS_WAIT_RETRIES:-6}"
AKS_WAIT_RETRY_DELAY_SECONDS="${AKS_WAIT_RETRY_DELAY_SECONDS:-10}"
DATAHUB_SETUP_JOB_TIMEOUT="${DATAHUB_SETUP_JOB_TIMEOUT:-1200s}"
DATAHUB_ELASTICSEARCH_SETUP_JOB_TIMEOUT="${DATAHUB_ELASTICSEARCH_SETUP_JOB_TIMEOUT:-300s}"
AKS_USE_KEY_VAULT="${AKS_USE_KEY_VAULT:-true}"
AKS_KEY_VAULT_RESOURCE_GROUP="${AKS_KEY_VAULT_RESOURCE_GROUP:-$AKS_RESOURCE_GROUP}"
AKS_KEY_VAULT_NAME="${AKS_KEY_VAULT_NAME:-}"
AKS_KEY_VAULT_SECRET_NAME="${AKS_KEY_VAULT_SECRET_NAME:-odp-env}"
AKS_KEY_VAULT_PROVIDER_CLASS_NAME="${AKS_KEY_VAULT_PROVIDER_CLASS_NAME:-odp-env-keyvault}"
AKS_KEY_VAULT_SYNC_DEPLOYMENT_NAME="${AKS_KEY_VAULT_SYNC_DEPLOYMENT_NAME:-odp-env-keyvault-sync}"
AKS_KEY_VAULT_SYNC_TIMEOUT_SECONDS="${AKS_KEY_VAULT_SYNC_TIMEOUT_SECONDS:-300}"
AKS_KEY_VAULT_SECRET_SET_RETRIES="${AKS_KEY_VAULT_SECRET_SET_RETRIES:-18}"
AKS_KEY_VAULT_SECRET_SET_RETRY_DELAY_SECONDS="${AKS_KEY_VAULT_SECRET_SET_RETRY_DELAY_SECONDS:-10}"
KEY_VAULT_SECRETS_USER_ROLE_ID="4633458b-17de-408a-b874-0445c86b69e6"
KEY_VAULT_SECRETS_OFFICER_ROLE_ID="b86a8fe4-44ce-4948-aee5-eccb2c155cd7"
KEY_VAULT_ADMIN_ROLE_ID="00482a5a-887f-4fb3-b363-3b7fe8e74483"
KOMPOSE_OVERRIDE_FILE="${KOMPOSE_OVERRIDE_FILE:-$ROOT_DIR/docker-compose.k8s.yml}"
AKS_HELPERS_LIB="$ROOT_DIR/scripts/aks/aks_up_lib.sh"
KOMPOSE_LIB="$ROOT_DIR/scripts/k8s/k8s_kompose_lib.sh"

log() {
  echo "[aks-up] $*"
}

validate_boolean_env() {
  local var_name="$1"
  local var_value="$2"
  if [[ "$var_value" != "true" && "$var_value" != "false" ]]; then
    echo "[aks-up] ERROR: ${var_name} must be 'true' or 'false' (got '${var_value}')." >&2
    exit 1
  fi
}

validate_positive_integer_env() {
  local var_name="$1"
  local var_value="$2"
  if [[ ! "$var_value" =~ ^[0-9]+$ ]] || (( var_value < 1 )); then
    echo "[aks-up] ERROR: ${var_name} must be a positive integer (got '${var_value}')." >&2
    exit 1
  fi
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

kubectl_ctx() {
  kubectl --context "$AKS_CLUSTER_NAME" "$@"
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

  kv_secret_name="$(echo "$env_key" | tr '[:upper:]' '[:lower:]' | tr '_' '-' | tr -cd 'a-z0-9-')"
  kv_secret_name="$(echo "$kv_secret_name" | sed -E 's/-+/-/g; s/^-+//; s/-+$//')"

  printf '%s' "$kv_secret_name"
}

normalise_env_assignment_value() {
  local raw_value="$1"
  local normalised_value

  normalised_value="$(echo "$raw_value" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"
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
    key="$(echo "$key" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"
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

resolve_current_azure_principal_object_id() {
  local principal_object_id=""
  local access_token

  if ! command -v python3 >/dev/null 2>&1; then
    printf ''
    return 0
  fi

  access_token="$(az account get-access-token --resource https://management.azure.com/ --query accessToken -o tsv 2>/dev/null || true)"
  if [[ -z "$access_token" ]]; then
    printf ''
    return 0
  fi

  principal_object_id="$(
    ARM_ACCESS_TOKEN="$access_token" python3 - <<'PY'
import base64
import json
import os
import sys

token = os.environ.get("ARM_ACCESS_TOKEN", "")
parts = token.split(".")
if len(parts) < 2:
    sys.exit(0)

payload = parts[1] + "=" * (-len(parts[1]) % 4)
try:
    claims = json.loads(base64.urlsafe_b64decode(payload.encode()).decode())
except Exception:
    sys.exit(0)

principal_oid = claims.get("oid", "")
if principal_oid:
    print(principal_oid)
PY
  )"

  printf '%s' "$principal_object_id"
}

role_assignment_count_for_role_id() {
  local assignee_object_id="$1"
  local scope="$2"
  local role_id="$3"

  az role assignment list \
    --assignee-object-id "$assignee_object_id" \
    --scope "$scope" \
    --include-inherited \
    --fill-principal-name false \
    --fill-role-definition-name false \
    --query "[?contains(roleDefinitionId, '$role_id')] | length(@)" \
    -o tsv 2>/dev/null || echo 0
}

create_role_assignment_via_rest() {
  local scope="$1"
  local principal_id="$2"
  local role_definition_id="$3"
  local principal_type="${4:-}"
  local assignment_id
  local request_body

  if command -v uuidgen >/dev/null 2>&1; then
    assignment_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
  else
    assignment_id="$(
      python3 - <<'PY'
import uuid
print(uuid.uuid4())
PY
    )"
  fi

  if [[ -n "$principal_type" ]]; then
    request_body="$(printf '{"properties":{"roleDefinitionId":"%s","principalId":"%s","principalType":"%s"}}' "$role_definition_id" "$principal_id" "$principal_type")"
  else
    request_body="$(printf '{"properties":{"roleDefinitionId":"%s","principalId":"%s"}}' "$role_definition_id" "$principal_id")"
  fi

  az rest \
    --method put \
    --url "https://management.azure.com${scope}/providers/Microsoft.Authorization/roleAssignments/${assignment_id}?api-version=2022-04-01" \
    --body "$request_body" \
    -o none
}

ensure_current_principal_can_seed_key_vault() {
  local key_vault_id="$1"
  local deployer_object_id
  local deployer_officer_count
  local deployer_admin_count
  local role_definition_id
  local assignment_error

  deployer_object_id="$(resolve_current_azure_principal_object_id)"
  if [[ -z "$deployer_object_id" ]]; then
    log "Could not resolve the signed-in Azure principal object ID; expecting pre-existing Key Vault secret write permissions."
    return 0
  fi

  deployer_officer_count="$(role_assignment_count_for_role_id "$deployer_object_id" "$key_vault_id" "$KEY_VAULT_SECRETS_OFFICER_ROLE_ID")"
  deployer_admin_count="$(role_assignment_count_for_role_id "$deployer_object_id" "$key_vault_id" "$KEY_VAULT_ADMIN_ROLE_ID")"
  if [[ "${deployer_officer_count:-0}" != "0" || "${deployer_admin_count:-0}" != "0" ]]; then
    return 0
  fi

  log "Signed-in Azure principal is missing Key Vault secret write role on '$AKS_KEY_VAULT_NAME'; attempting to grant 'Key Vault Secrets Officer'..."
  role_definition_id="/subscriptions/${SUBSCRIPTION_ID}/providers/Microsoft.Authorization/roleDefinitions/${KEY_VAULT_SECRETS_OFFICER_ROLE_ID}"
  if assignment_error="$(create_role_assignment_via_rest "$key_vault_id" "$deployer_object_id" "$role_definition_id" 2>&1)"; then
    log "Granted 'Key Vault Secrets Officer' to signed-in Azure principal for '$AKS_KEY_VAULT_NAME'."
  elif [[ "$assignment_error" == *"RoleAssignmentExists"* ]]; then
    log "Signed-in Azure principal already has an equivalent Key Vault role assignment."
  else
    log "Could not auto-assign 'Key Vault Secrets Officer'. If secret sync fails, grant 'Key Vault Secrets Officer' or 'Key Vault Administrator' on '$AKS_KEY_VAULT_NAME' and rerun."
  fi
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
  local key_vault_id
  local key_vault_rbac_enabled
  local addon_identity_client_id=""
  local addon_identity_object_id=""
  local role_assignment_count
  local secret_provider_manifest
  local idx
  local kv_secret_name
  local value
  local deleted_vault_count
  local addon_enabled
  local role_definition_id
  local assignment_error

  if [[ -z "$AKS_KEY_VAULT_NAME" ]]; then
    AKS_KEY_VAULT_NAME="aitrialkv${SUB_HASH}"
  fi
  AKS_KEY_VAULT_NAME="$(echo "$AKS_KEY_VAULT_NAME" | tr '[:upper:]' '[:lower:]')"

  if [[ ! "$AKS_KEY_VAULT_NAME" =~ ^[a-z][a-z0-9-]{1,22}[a-z0-9]$ ]]; then
    echo "[aks-up] ERROR: invalid AKS_KEY_VAULT_NAME '$AKS_KEY_VAULT_NAME'." >&2
    echo "[aks-up] Use 3-24 chars, start with a letter, end with a letter/digit, and include only lowercase letters, digits, or '-'." >&2
    return 1
  fi

  load_env_entries_for_key_vault

  log "Ensuring Key Vault resource group '$AKS_KEY_VAULT_RESOURCE_GROUP' exists..."
  az group create --name "$AKS_KEY_VAULT_RESOURCE_GROUP" --location "$AKS_LOCATION" -o none

  if az keyvault show --name "$AKS_KEY_VAULT_NAME" >/dev/null 2>&1; then
    log "Key Vault '$AKS_KEY_VAULT_NAME' already exists."
  else
    deleted_vault_count="$(az keyvault list-deleted --query "[?name=='${AKS_KEY_VAULT_NAME}'] | length(@)" -o tsv 2>/dev/null || echo 0)"
    if [[ "${deleted_vault_count:-0}" != "0" ]]; then
      log "Recovering soft-deleted Key Vault '$AKS_KEY_VAULT_NAME'..."
      az keyvault recover --name "$AKS_KEY_VAULT_NAME" -o none
    else
      log "Creating Key Vault '$AKS_KEY_VAULT_NAME'..."
      az keyvault create \
        --name "$AKS_KEY_VAULT_NAME" \
        --resource-group "$AKS_KEY_VAULT_RESOURCE_GROUP" \
        --location "$AKS_LOCATION" \
        --enable-rbac-authorization true \
        -o none
    fi
  fi

  key_vault_id="$(az keyvault show --name "$AKS_KEY_VAULT_NAME" --query id -o tsv)"
  key_vault_rbac_enabled="$(az keyvault show --name "$AKS_KEY_VAULT_NAME" --query properties.enableRbacAuthorization -o tsv)"
  AKS_TENANT_ID="$(az account show --query tenantId -o tsv)"

  addon_enabled="$(az aks show --resource-group "$AKS_RESOURCE_GROUP" --name "$AKS_CLUSTER_NAME" --query addonProfiles.azureKeyvaultSecretsProvider.enabled -o tsv 2>/dev/null || echo false)"
  if [[ "$(printf '%s' "$addon_enabled" | tr '[:upper:]' '[:lower:]')" == "true" ]]; then
    log "AKS Key Vault provider add-on is already enabled."
  else
    log "Enabling AKS Key Vault provider add-on..."
    az aks enable-addons \
      --addons azure-keyvault-secrets-provider \
      --resource-group "$AKS_RESOURCE_GROUP" \
      --name "$AKS_CLUSTER_NAME" \
      -o none
  fi

  for _ in {1..24}; do
    addon_identity_client_id="$(az aks show --resource-group "$AKS_RESOURCE_GROUP" --name "$AKS_CLUSTER_NAME" --query addonProfiles.azureKeyvaultSecretsProvider.identity.clientId -o tsv 2>/dev/null || true)"
    addon_identity_object_id="$(az aks show --resource-group "$AKS_RESOURCE_GROUP" --name "$AKS_CLUSTER_NAME" --query addonProfiles.azureKeyvaultSecretsProvider.identity.objectId -o tsv 2>/dev/null || true)"
    if [[ -n "$addon_identity_client_id" && -n "$addon_identity_object_id" ]]; then
      break
    fi
    sleep 5
  done

  if [[ -z "$addon_identity_client_id" || -z "$addon_identity_object_id" ]]; then
    echo "[aks-up] ERROR: unable to resolve AKS Key Vault provider identity after enabling add-on." >&2
    return 1
  fi

  AKS_KEY_VAULT_PROVIDER_CLIENT_ID="$addon_identity_client_id"

  if [[ "$key_vault_rbac_enabled" == "true" ]]; then
    role_assignment_count="$(role_assignment_count_for_role_id "$addon_identity_object_id" "$key_vault_id" "$KEY_VAULT_SECRETS_USER_ROLE_ID")"
    if [[ "${role_assignment_count:-0}" == "0" ]]; then
      log "Granting 'Key Vault Secrets User' to AKS Key Vault provider identity..."
      role_definition_id="/subscriptions/${SUBSCRIPTION_ID}/providers/Microsoft.Authorization/roleDefinitions/${KEY_VAULT_SECRETS_USER_ROLE_ID}"
      if assignment_error="$(create_role_assignment_via_rest "$key_vault_id" "$addon_identity_object_id" "$role_definition_id" "ServicePrincipal" 2>&1)"; then
        :
      elif [[ "$assignment_error" == *"RoleAssignmentExists"* ]]; then
        log "AKS Key Vault provider identity already has 'Key Vault Secrets User' role on '$AKS_KEY_VAULT_NAME'."
      else
        echo "$assignment_error" >&2
        return 1
      fi
    else
      log "AKS Key Vault provider identity already has 'Key Vault Secrets User' role on '$AKS_KEY_VAULT_NAME'."
    fi

    ensure_current_principal_can_seed_key_vault "$key_vault_id"
  else
    log "Key Vault '$AKS_KEY_VAULT_NAME' uses access policies; granting get/list secret permissions to AKS provider identity..."
    az keyvault set-policy \
      --name "$AKS_KEY_VAULT_NAME" \
      --object-id "$addon_identity_object_id" \
      --secret-permissions get list \
      -o none
  fi

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
  local mysql_password_b64

  mysql_pod="$(kubectl_ctx -n "$NAMESPACE" get pods -l io.kompose.service=datahub-mysql -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "$mysql_pod" ]]; then
    echo "[aks-up] ERROR: datahub-mysql pod not found; cannot self-heal DataHub MySQL host grants." >&2
    return 1
  fi

  mysql_password_b64="$(kubectl_ctx -n "$NAMESPACE" get secret "$AKS_KEY_VAULT_SECRET_NAME" -o jsonpath='{.data.DATAHUB_MYSQL_ROOT_PASSWORD}' 2>/dev/null || true)"
  if [[ -z "$mysql_password_b64" ]]; then
    echo "[aks-up] ERROR: DATAHUB_MYSQL_ROOT_PASSWORD not found in '$AKS_KEY_VAULT_SECRET_NAME' secret; cannot self-heal DataHub MySQL host grants." >&2
    return 1
  fi

  log "Applying DataHub MySQL host-auth self-heal (creating/updating root@'%' grant) ..."
  kubectl_ctx -n "$NAMESPACE" exec "pod/${mysql_pod}" -- sh -lc "mysql -uroot -e \"SET @pw = CAST(FROM_BASE64('${mysql_password_b64}') AS CHAR); SET @create_stmt = CONCAT('CREATE USER IF NOT EXISTS ''root''@''%'' IDENTIFIED WITH mysql_native_password BY ', QUOTE(@pw)); PREPARE stmt FROM @create_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt; SET @alter_stmt = CONCAT('ALTER USER ''root''@''%'' IDENTIFIED WITH mysql_native_password BY ', QUOTE(@pw)); PREPARE stmt FROM @alter_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt; GRANT ALL PRIVILEGES ON *.* TO ''root''@''%'' WITH GRANT OPTION; FLUSH PRIVILEGES;\""

  log "Restarting datahub-gms deployment after MySQL host-auth self-heal..."
  kubectl_ctx -n "$NAMESPACE" rollout restart deployment/datahub-gms
  wait_for_deployment datahub-gms "600s"
}

self_heal_datahub_mysql_missing_database() {
  local mysql_pod

  mysql_pod="$(kubectl_ctx -n "$NAMESPACE" get pods -l io.kompose.service=datahub-mysql -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "$mysql_pod" ]]; then
    echo "[aks-up] ERROR: datahub-mysql pod not found; cannot self-heal missing DataHub database." >&2
    return 1
  fi

  log "Applying DataHub MySQL missing-database self-heal (create schema + root@'%' grant) ..."
  kubectl_ctx -n "$NAMESPACE" exec "pod/${mysql_pod}" -- sh -lc "mysql -uroot -e \"CREATE DATABASE IF NOT EXISTS datahub CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci; CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY ''; ALTER USER 'root'@'%' IDENTIFIED BY ''; GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION; FLUSH PRIVILEGES;\""

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

require_cmd az
require_cmd kubectl
require_cmd docker
require_cmd curl
require_cmd openssl
require_cmd kompose
require_cmd yq

if [[ ! -f "$KOMPOSE_LIB" ]]; then
  echo "Missing kompose shared library: $KOMPOSE_LIB" >&2
  exit 1
fi

# shellcheck source=scripts/k8s/k8s_kompose_lib.sh
source "$KOMPOSE_LIB"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

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

ensure_aks_nodepool_capacity() {
  local desired_count="$AKS_NODE_COUNT"
  local system_pool
  local autoscaling_enabled
  local pool_count
  local pool_min_count
  local pool_max_count
  local adjusted_max_count

  system_pool="$(az aks nodepool list \
    --resource-group "$AKS_RESOURCE_GROUP" \
    --cluster-name "$AKS_CLUSTER_NAME" \
    --query "[?mode=='System'] | [0].name" \
    -o tsv 2>/dev/null || true)"
  if [[ -z "$system_pool" ]]; then
    log "Could not resolve a System nodepool for AKS cluster '$AKS_CLUSTER_NAME'; skipping nodepool capacity reconciliation."
    return 0
  fi

  autoscaling_enabled="$(az aks nodepool show --resource-group "$AKS_RESOURCE_GROUP" --cluster-name "$AKS_CLUSTER_NAME" --name "$system_pool" --query enableAutoScaling -o tsv 2>/dev/null || echo false)"
  pool_count="$(az aks nodepool show --resource-group "$AKS_RESOURCE_GROUP" --cluster-name "$AKS_CLUSTER_NAME" --name "$system_pool" --query count -o tsv 2>/dev/null || echo 0)"

  if [[ "$(printf '%s' "$autoscaling_enabled" | tr '[:upper:]' '[:lower:]')" == "true" ]]; then
    pool_min_count="$(az aks nodepool show --resource-group "$AKS_RESOURCE_GROUP" --cluster-name "$AKS_CLUSTER_NAME" --name "$system_pool" --query minCount -o tsv 2>/dev/null || echo 0)"
    pool_max_count="$(az aks nodepool show --resource-group "$AKS_RESOURCE_GROUP" --cluster-name "$AKS_CLUSTER_NAME" --name "$system_pool" --query maxCount -o tsv 2>/dev/null || echo 0)"
    if (( pool_min_count < desired_count )); then
      adjusted_max_count="$pool_max_count"
      if (( adjusted_max_count < desired_count )); then
        adjusted_max_count="$desired_count"
      fi
      log "Raising AKS autoscaler minimum for nodepool '$system_pool' from $pool_min_count to $desired_count (max=$adjusted_max_count)..."
      az aks nodepool update \
        --resource-group "$AKS_RESOURCE_GROUP" \
        --cluster-name "$AKS_CLUSTER_NAME" \
        --name "$system_pool" \
        --update-cluster-autoscaler \
        --min-count "$desired_count" \
        --max-count "$adjusted_max_count" \
        -o none
    fi
  else
    if (( pool_count < desired_count )); then
      log "Scaling AKS nodepool '$system_pool' from $pool_count to $desired_count nodes..."
      az aks nodepool scale \
        --resource-group "$AKS_RESOURCE_GROUP" \
        --cluster-name "$AKS_CLUSTER_NAME" \
        --name "$system_pool" \
        --node-count "$desired_count" \
        -o none
    fi
  fi
}

if [[ ! -f "$ROOT_DIR/.env" ]]; then
  echo "Missing $ROOT_DIR/.env. Create it first (for example: cp .env.template .env)." >&2
  exit 1
fi

validate_boolean_env "AKS_USE_KEY_VAULT" "$AKS_USE_KEY_VAULT"
validate_boolean_env "AKS_FORCE_ATTACH_ACR" "$AKS_FORCE_ATTACH_ACR"
validate_positive_integer_env "AKS_NODE_COUNT" "$AKS_NODE_COUNT"
enforce_odp_env_secret_name

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

ensure_aks_nodepool_capacity

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
wait_for_deployment_in_namespace ingress-nginx ingress-nginx-controller "$WAIT_TIMEOUT"

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

for cname in www airflow minio minio-api keycloak datahub superset grafana jupyter prometheus alertmanager dbt-docs portal-api; do
  az network dns record-set cname create --resource-group "$DNS_RESOURCE_GROUP" --zone-name "$FRONTEND_DOMAIN" --name "$cname" --ttl 300 -o none || true
  az network dns record-set cname set-record --resource-group "$DNS_RESOURCE_GROUP" --zone-name "$FRONTEND_DOMAIN" --record-set-name "$cname" --cname "$FRONTEND_DOMAIN" -o none
done

log "Installing/upgrading cert-manager ($CERT_MANAGER_VERSION)..."
kubectl_ctx apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"

log "Waiting for cert-manager to be ready..."
wait_for_deployment_in_namespace cert-manager cert-manager "$WAIT_TIMEOUT"
wait_for_deployment_in_namespace cert-manager cert-manager-cainjector "$WAIT_TIMEOUT"
wait_for_deployment_in_namespace cert-manager cert-manager-webhook "$WAIT_TIMEOUT"

log "Logging in to ACR '$ACR_NAME'..."
az acr login --name "$ACR_NAME" -o none

warn_on_legacy_vite_url_vars

build_and_push_image "$AIRFLOW_IMAGE" "$ROOT_DIR/airflow/Dockerfile" "$ROOT_DIR" "Airflow"
build_and_push_image "$FRONTEND_IMAGE" "$ROOT_DIR/frontend/Dockerfile.k8s" "$ROOT_DIR/frontend" "Frontend" \
  --build-arg "VITE_KEYCLOAK_URL=${AKS_VITE_KEYCLOAK_URL}" \
  --build-arg "VITE_KEYCLOAK_REALM=${AKS_VITE_KEYCLOAK_REALM}" \
  --build-arg "VITE_KEYCLOAK_CLIENT_ID=${AKS_VITE_KEYCLOAK_CLIENT_ID}" \
  --build-arg "VITE_PORTAL_API_URL=${AKS_VITE_PORTAL_API_URL}" \
  --build-arg "VITE_DBT_DOCS_URL=${AKS_VITE_DBT_DOCS_URL}"
build_and_push_image "$PORTAL_API_IMAGE" "$ROOT_DIR/ops/portal-api/Dockerfile" "$ROOT_DIR" "Portal API"
build_and_push_image "$JUPYTER_IMAGE" "$ROOT_DIR/notebooks/Dockerfile" "$ROOT_DIR/notebooks" "Jupyter"
build_and_push_image "$MINIO_SSO_BRIDGE_IMAGE" "$ROOT_DIR/ops/minio-sso-bridge/Dockerfile" "$ROOT_DIR" "MinIO SSO bridge"

log "Applying namespace..."
render_and_apply "$ROOT_DIR/k8s/aks/namespace.yaml"

if [[ "$AKS_USE_KEY_VAULT" == "true" ]]; then
  log "Creating/updating Kubernetes secret '$AKS_KEY_VAULT_SECRET_NAME' from Azure Key Vault..."
  ensure_key_vault_secret_sync
else
  log "AKS_USE_KEY_VAULT=false; creating/updating Kubernetes secret '$AKS_KEY_VAULT_SECRET_NAME' directly from .env..."
  create_odp_env_secret_from_env_file
fi

if [[ -z "$(kubectl_ctx -n "$NAMESPACE" get secret "$AKS_KEY_VAULT_SECRET_NAME" -o jsonpath='{.data.AIRFLOW_OAUTH_DEFAULT_ROLE}' 2>/dev/null || true)" ]]; then
  log "AIRFLOW_OAUTH_DEFAULT_ROLE missing in .env; defaulting '$AKS_KEY_VAULT_SECRET_NAME' secret value to 'Op' for Airflow DAG trigger permissions."
  kubectl_ctx -n "$NAMESPACE" patch secret "$AKS_KEY_VAULT_SECRET_NAME" --type merge -p '{"stringData":{"AIRFLOW_OAUTH_DEFAULT_ROLE":"Op"}}'
  if [[ "$AKS_USE_KEY_VAULT" == "true" ]]; then
    set_key_vault_secret_with_retry \
      "$AKS_KEY_VAULT_NAME" \
      "$(env_key_to_key_vault_secret_name AIRFLOW_OAUTH_DEFAULT_ROLE)" \
      "Op"
  fi
fi

log "Creating/updating Airflow webserver config ConfigMap..."
kubectl_ctx -n "$NAMESPACE" create configmap airflow-webserver-config \
  --from-file=webserver_config.py="$ROOT_DIR/airflow/webserver_config.py" \
  --dry-run=client -o yaml | kubectl_ctx apply -f -

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

GMS_MANIFEST="$TMP_DIR/datahub-gms-deployment.yaml"
FRONTEND_MANIFEST="$TMP_DIR/datahub-frontend-deployment.yaml"
kompose_hold_datahub

log "Applying AKS full-stack parity manifests..."
kubectl_ctx -n "$NAMESPACE" apply -f "$TMP_DIR"

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

log "Deploying AKS dbt-docs service (regenerated at rollout via initContainer)..."
render_and_apply "$ROOT_DIR/k8s/aks/dbt-docs.yaml"

log "Waiting for extended deployments..."
datahub_gms_heal_attempted=false
for deployment in "${EXTENDED_DEPLOYMENTS[@]}" dbt-docs; do
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

if [[ "$SKIP_MSTEAMS" == "false" ]]; then
  wait_for_deployment prometheus-msteams "600s"
fi

log "Applying cert issuer + ingress..."
render_and_apply "$ROOT_DIR/k8s/aks/cert-issuer-letsencrypt-prod.yaml"
render_and_apply "$ROOT_DIR/k8s/aks/frontend-ingress.yaml"
render_and_apply "$ROOT_DIR/k8s/aks/datahub-ingress.yaml"
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
curl -sS -o /dev/null -D - --resolve "datahub.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://datahub.${FRONTEND_DOMAIN}/" | head -n 1
curl -sS -o /dev/null -D - --resolve "superset.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://superset.${FRONTEND_DOMAIN}/health" | head -n 1
curl -sS -o /dev/null -D - --resolve "grafana.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://grafana.${FRONTEND_DOMAIN}/api/health" | head -n 1
curl -sS -o /dev/null -D - --resolve "prometheus.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://prometheus.${FRONTEND_DOMAIN}/-/healthy" | head -n 1
curl -sS -o /dev/null -D - --resolve "dbt-docs.${FRONTEND_DOMAIN}:443:${INGRESS_PIP_IP}" "https://dbt-docs.${FRONTEND_DOMAIN}/" | head -n 1
echo | openssl s_client -servername "${FRONTEND_DOMAIN}" -connect "${INGRESS_PIP_IP}:443" 2>/dev/null | openssl x509 -noout -subject -issuer | sed -n '1,2p'

if [[ "$AKS_USE_KEY_VAULT" == "true" ]]; then
  SECRET_SOURCE_SUMMARY="Azure Key Vault (${AKS_KEY_VAULT_NAME}) -> Kubernetes secret ${AKS_KEY_VAULT_SECRET_NAME}"
else
  SECRET_SOURCE_SUMMARY=".env -> Kubernetes secret ${AKS_KEY_VAULT_SECRET_NAME} (AKS_USE_KEY_VAULT=false)"
fi

cat <<EOT

AKS deployment is up.

Cluster:       $AKS_CLUSTER_NAME
ResourceGroup: $AKS_RESOURCE_GROUP
Namespace:     $NAMESPACE
Secret source: $SECRET_SOURCE_SUMMARY
Airflow image: $AIRFLOW_IMAGE
Frontend URL:  https://$FRONTEND_DOMAIN
Airflow URL:   https://airflow.$FRONTEND_DOMAIN
MinIO URL:     https://minio.$FRONTEND_DOMAIN
MinIO API URL: https://minio-api.$FRONTEND_DOMAIN
Keycloak URL:  https://keycloak.$FRONTEND_DOMAIN
DataHub URL:   https://datahub.$FRONTEND_DOMAIN
Superset URL:  https://superset.$FRONTEND_DOMAIN
Grafana URL:   https://grafana.$FRONTEND_DOMAIN
Jupyter URL:   https://jupyter.$FRONTEND_DOMAIN
Prometheus URL:https://prometheus.$FRONTEND_DOMAIN
dbt Docs URL:  https://dbt-docs.$FRONTEND_DOMAIN
Portal API URL:https://portal-api.$FRONTEND_DOMAIN

Access services with port-forward from your machine:
  kubectl -n $NAMESPACE port-forward svc/airflow-webserver 8080:8080
  kubectl -n $NAMESPACE port-forward svc/minio 9000:9000 9001:9001
  kubectl -n $NAMESPACE port-forward svc/warehouse 5433:5432

Cleanup when done (to avoid costs):
  az group delete --name $AKS_RESOURCE_GROUP --yes --no-wait

EOT
