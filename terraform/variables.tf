# ---------------------------------------------------------------------------
# Cloud selector
# ---------------------------------------------------------------------------
variable "cloud_provider" {
  description = "Cloud provider to deploy to: 'azure' or 'scaleway'"
  type        = string
  default     = "azure"

  validation {
    condition     = contains(["azure", "scaleway"], var.cloud_provider)
    error_message = "cloud_provider must be 'azure' or 'scaleway'."
  }
}

# ---------------------------------------------------------------------------
# Shared / cloud-agnostic variables
# ---------------------------------------------------------------------------
variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "cluster_name" {
  description = "Kubernetes cluster name (used by both AKS and Kapsule)"
  type        = string
  default     = "ai-trial"
}

variable "frontend_domain" {
  description = "Root domain for DNS zone and TLS certificate"
  type        = string
  default     = "eu-sovereigndataplatform.com"
}

variable "dns_cname_subdomains" {
  description = "Subdomains to create as CNAMEs pointing to the root domain"
  type        = list(string)
  default = [
    "www",
    "airflow",
    "minio",
    "minio-api",
    "keycloak",
    "datahub",
    "superset",
    "grafana",
    "jupyter",
    "prometheus",
    "alertmanager",
    "dbt-docs",
    "portal-api",
  ]
}

variable "node_count" {
  description = "Number of nodes in the default node pool"
  type        = number
  default     = 1
}

variable "ingress_nginx_chart_version" {
  description = "Helm chart version for ingress-nginx"
  type        = string
  default     = "4.12.3"
}

variable "cert_manager_version" {
  description = "Helm chart version for cert-manager"
  type        = string
  default     = "v1.19.3"
}

variable "enable_secrets_manager" {
  description = "Create a cloud-managed secret store (Key Vault / Scaleway Secret Manager)"
  type        = bool
  default     = true
}

variable "create_dns_zone" {
  description = "Create a managed DNS zone for the frontend_domain. Set to false when using IP-based domains (sslip.io, nip.io) or Azure-provided cloudapp.azure.com hostnames."
  type        = bool
  default     = true
}

variable "azure_pip_dns_label" {
  description = "Optional DNS label for the ingress Public IP, giving a stable hostname: <label>.<region>.cloudapp.azure.com. Leave empty to skip."
  type        = string
  default     = ""
}

# ---------------------------------------------------------------------------
# Azure-specific variables
# (ignored / can be left empty when cloud_provider = "scaleway")
# ---------------------------------------------------------------------------
variable "azure_subscription_id" {
  description = "Azure subscription ID. Falls back to ARM_SUBSCRIPTION_ID env var if empty."
  type        = string
  default     = ""
}

variable "azure_location" {
  description = "Azure region for all resources"
  type        = string
  default     = "westeurope"
}

variable "azure_resource_group_name" {
  description = "Azure resource group name"
  type        = string
  default     = "ai-trial-rg"
}

variable "azure_dns_resource_group" {
  description = "Resource group for the Azure DNS zone (uses main RG if empty)"
  type        = string
  default     = ""
}

variable "azure_acr_name" {
  description = "ACR name — derived from subscription hash if empty"
  type        = string
  default     = ""
}

variable "azure_acr_sku" {
  description = "ACR SKU (Basic, Standard, Premium)"
  type        = string
  default     = "Basic"
}

variable "azure_aks_dns_prefix" {
  description = "DNS prefix for the AKS cluster"
  type        = string
  default     = "ai-trial"
}

variable "azure_node_vm_size" {
  description = "VM size for AKS nodes"
  type        = string
  default     = "Standard_B2s"
}

variable "azure_aks_sku_tier" {
  description = "AKS pricing tier (Free or Standard)"
  type        = string
  default     = "Free"
}

variable "azure_ingress_pip_name" {
  description = "Name of the static public IP for ingress"
  type        = string
  default     = "ai-trial-ingress-pip"
}

variable "azure_key_vault_name" {
  description = "Key Vault name — derived from subscription hash if empty"
  type        = string
  default     = ""
}

variable "azure_tags" {
  description = "Tags to apply to all Azure resources"
  type        = map(string)
  default     = {}
}

variable "azure_tenant_id_override" {
  description = "Azure AD tenant ID. When non-empty, data.azurerm_client_config is skipped entirely (avoids Graph API calls that may be blocked by Conditional Access policies). Obtain via: az account show --query tenantId -o tsv"
  type        = string
  default     = ""
}

variable "azure_subscription_id_override" {
  description = "Azure subscription ID. When non-empty, used directly instead of querying the azurerm_client_config data source. Obtain via: az account show --query id -o tsv"
  type        = string
  default     = ""
}

variable "azure_deployer_object_id" {
  description = "Object ID of the deployer principal for Key Vault RBAC (Secrets Officer role). Required when enable_secrets_manager = true and azure_tenant_id_override is set (the data source that would normally supply this value is skipped). Obtain via: az ad signed-in-user show --query id -o tsv"
  type        = string
  default     = ""
}

variable "azure_use_cli" {
  description = "Allow the azurerm provider to use Azure CLI for authentication. Set to false when running in environments where the CLI cannot obtain all required tokens (e.g. Conditional Access blocks the Graph API). When false, provide ARM_ACCESS_TOKEN + ARM_TENANT_ID + ARM_SUBSCRIPTION_ID environment variables."
  type        = bool
  default     = true
}

# ---------------------------------------------------------------------------
# Scaleway-specific variables
# (ignored / can be left empty when cloud_provider = "azure")
# ---------------------------------------------------------------------------
variable "scw_access_key" {
  description = "Scaleway access key. Falls back to SCW_ACCESS_KEY env var if empty."
  type        = string
  default     = ""
  sensitive   = true
}

variable "scw_secret_key" {
  description = "Scaleway secret key. Falls back to SCW_SECRET_KEY env var if empty."
  type        = string
  default     = ""
  sensitive   = true
}

variable "scw_project_id" {
  description = "Scaleway project ID. Falls back to SCW_DEFAULT_PROJECT_ID env var if empty."
  type        = string
  default     = ""
}

variable "scw_region" {
  description = "Scaleway region (fr-par, nl-ams, pl-waw)"
  type        = string
  default     = "nl-ams"
}

variable "scw_zone" {
  description = "Scaleway zone (e.g. nl-ams-1)"
  type        = string
  default     = "nl-ams-1"
}

variable "scw_registry_name" {
  description = "Scaleway container registry namespace name"
  type        = string
  default     = "ai-trial"
}

variable "scw_node_type" {
  description = "Scaleway Kapsule node type (e.g. DEV1-M, GP1-S, PRO2-S)"
  type        = string
  default     = "DEV1-M"
}

variable "scw_kubernetes_version" {
  description = "Kubernetes version for Kapsule cluster"
  type        = string
  default     = "1.30"
}

variable "scw_tags" {
  description = "Tags (list of strings) to apply to Scaleway resources"
  type        = list(string)
  default     = []
}
