variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "westeurope"
}

variable "resource_group_name" {
  description = "Name of the main resource group"
  type        = string
  default     = "ai-trial-rg"
}

variable "aks_cluster_name" {
  description = "Name of the AKS cluster"
  type        = string
  default     = "ai-trial-aks"
}

variable "aks_dns_prefix" {
  description = "DNS prefix for the AKS cluster"
  type        = string
  default     = "ai-trial"
}

variable "aks_node_count" {
  description = "Number of nodes in the default node pool"
  type        = number
  default     = 1
}

variable "aks_node_vm_size" {
  description = "VM size for AKS nodes"
  type        = string
  default     = "Standard_B2s"
}

variable "aks_sku_tier" {
  description = "AKS pricing tier (Free or Standard)"
  type        = string
  default     = "Free"
}

variable "acr_name" {
  description = "Name of the Azure Container Registry (derived from subscription if empty)"
  type        = string
  default     = ""
}

variable "acr_sku" {
  description = "ACR SKU (Basic, Standard, Premium)"
  type        = string
  default     = "Basic"
}

variable "frontend_domain" {
  description = "Domain name for the frontend and DNS zone"
  type        = string
  default     = "eu-sovereigndataplatform.com"
}

variable "dns_resource_group" {
  description = "Resource group for the DNS zone (uses main RG if empty)"
  type        = string
  default     = ""
}

variable "dns_cname_subdomains" {
  description = "List of CNAME subdomains pointing to the root domain"
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

variable "ingress_pip_name" {
  description = "Name of the static public IP for ingress"
  type        = string
  default     = "ai-trial-ingress-pip"
}

variable "enable_key_vault" {
  description = "Whether to create and use Azure Key Vault for secret management"
  type        = bool
  default     = true
}

variable "key_vault_name" {
  description = "Name of the Azure Key Vault (derived from subscription if empty)"
  type        = string
  default     = ""
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

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}
