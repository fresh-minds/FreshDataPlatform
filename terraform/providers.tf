# ---------------------------------------------------------------------------
# Azure provider
# Credentials: az login  OR  ARM_CLIENT_ID / ARM_CLIENT_SECRET / ARM_TENANT_ID
# Leave azure_subscription_id empty ("") when cloud_provider = "scaleway"
# ---------------------------------------------------------------------------
provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
  }
  # When false, skip Azure CLI auth entirely (use ARM_* credentials instead).
  # This is useful for Scaleway-only runs in tenants where Graph token issuance
  # via Azure CLI is blocked by Conditional Access.
  use_cli = var.azure_use_cli

  # Empty string falls back to ARM_SUBSCRIPTION_ID env var.
  # When cloud_provider = "scaleway" no Azure resources are created so
  # the provider initialises but makes zero API calls.
  # Prefer the explicit override (populated when use_cli = false to avoid Graph API calls).
  # Falls back to azure_subscription_id (which itself falls back to ARM_SUBSCRIPTION_ID env var).
  subscription_id = var.azure_subscription_id_override != "" ? var.azure_subscription_id_override : var.azure_subscription_id
  # When use_cli = false the provider cannot auto-detect the tenant; supply it explicitly.
  # Empty string here means "let the provider fall back to ARM_TENANT_ID env var".
  tenant_id = var.azure_tenant_id_override

  # NOTE: when use_cli=true, azurerm may call Azure CLI for Graph token flows.
  # If blocked by Conditional Access, either switch to service principal auth
  # (ARM_CLIENT_ID/ARM_CLIENT_SECRET/ARM_TENANT_ID/ARM_SUBSCRIPTION_ID) or
  # set azure_use_cli=false for non-Azure deployments.
}

# ---------------------------------------------------------------------------
# Scaleway provider
# Credentials: SCW_ACCESS_KEY / SCW_SECRET_KEY / SCW_DEFAULT_PROJECT_ID  OR
#              var.scw_access_key / var.scw_secret_key / var.scw_project_id
# Leave scw_access_key empty ("") when cloud_provider = "azure"
# ---------------------------------------------------------------------------
provider "scaleway" {
  # When cloud_provider = "azure", Scaleway resources have count = 0 and no
  # API calls are made. The provider still validates UUID fields, so we fall
  # back to a zero-UUID placeholder to pass format validation.
  access_key = var.scw_access_key != "" ? var.scw_access_key : "SCWXXXXXXXXXXXXXXXXX"
  secret_key = var.scw_secret_key != "" ? var.scw_secret_key : "00000000-0000-0000-0000-000000000000"
  project_id = var.scw_project_id != "" ? var.scw_project_id : "00000000-0000-0000-0000-000000000000"
  region     = var.scw_region != "" ? var.scw_region : "nl-ams"
  zone       = var.scw_zone != "" ? var.scw_zone : "nl-ams-1"
}

# ---------------------------------------------------------------------------
# Helm & Kubernetes providers — cloud-agnostic, always active
# Authentication is driven by whichever cluster module was created.
# ---------------------------------------------------------------------------
provider "helm" {
  kubernetes {
    host                   = local.kube_config_host
    cluster_ca_certificate = base64decode(local.kube_config_cluster_ca_certificate)
    token                  = local.kube_config_token
    client_certificate     = local.kube_config_client_certificate != "" ? base64decode(local.kube_config_client_certificate) : null
    client_key             = local.kube_config_client_key != "" ? base64decode(local.kube_config_client_key) : null
  }
}

provider "kubernetes" {
  host                   = local.kube_config_host
  cluster_ca_certificate = base64decode(local.kube_config_cluster_ca_certificate)
  token                  = local.kube_config_token
  client_certificate     = local.kube_config_client_certificate != "" ? base64decode(local.kube_config_client_certificate) : null
  client_key             = local.kube_config_client_key != "" ? base64decode(local.kube_config_client_key) : null
}
