# ---------------------------------------------------------------------------
# Cloud-agnostic root module
#
# Set cloud_provider = "azure" or "scaleway" in your .tfvars file.
# Only the selected provider's modules are created (count = 0 for the other).
# Unified locals expose a single interface consumed by outputs.tf and
# providers.tf (helm/kubernetes auth).
# ---------------------------------------------------------------------------

locals {
  is_azure    = var.cloud_provider == "azure"
  is_scaleway = var.cloud_provider == "scaleway"

  # ---- Azure auth overrides ----
  # When azure_tenant_id_override is non-empty, data.azurerm_client_config is not
  # fetched (count = 0), avoiding Graph API calls blocked by Conditional Access.
  # All three effective_* values fall back to the data source when no override is set.
  effective_tenant_id = var.azure_tenant_id_override != "" ? var.azure_tenant_id_override : (
    length(data.azurerm_client_config.current) > 0 ? data.azurerm_client_config.current[0].tenant_id : ""
  )
  effective_subscription_id = var.azure_subscription_id_override != "" ? var.azure_subscription_id_override : (
    length(data.azurerm_client_config.current) > 0 ? data.azurerm_client_config.current[0].subscription_id : ""
  )
  effective_deployer_object_id = var.azure_deployer_object_id != "" ? var.azure_deployer_object_id : (
    length(data.azurerm_client_config.current) > 0 ? data.azurerm_client_config.current[0].object_id : ""
  )

  # ---- Azure derived names ----
  azure_sub_hash = local.is_azure ? substr(replace(local.effective_subscription_id, "-", ""), 0, 8) : ""
  azure_acr_name = var.azure_acr_name != "" ? var.azure_acr_name : "aitrial${local.azure_sub_hash}"
  azure_kv_name  = var.azure_key_vault_name != "" ? var.azure_key_vault_name : "aitrialkv${local.azure_sub_hash}"

  azure_tags = merge(var.azure_tags, {
    environment = var.environment
    managed_by  = "terraform"
    cloud       = "azure"
  })

  # Scaleway tags are lists, not maps
  scaleway_tags = distinct(concat(var.scw_tags, ["env:${var.environment}", "managed-by:terraform", "cloud:scaleway"]))

  # ---- Unified kubeconfig (consumed by providers.tf) ----
  kube_config_host = local.is_azure ? (
    length(module.azure_aks) > 0 ? module.azure_aks[0].kube_config_host : ""
    ) : (
    length(module.scaleway_kubernetes) > 0 ? module.scaleway_kubernetes[0].kube_config_host : ""
  )
  kube_config_cluster_ca_certificate = local.is_azure ? (
    length(module.azure_aks) > 0 ? module.azure_aks[0].kube_config_cluster_ca_certificate : ""
    ) : (
    length(module.scaleway_kubernetes) > 0 ? module.scaleway_kubernetes[0].kube_config_cluster_ca_certificate : ""
  )
  # Azure uses cert+key auth; Scaleway uses bearer token auth
  kube_config_client_certificate = local.is_azure ? (
    length(module.azure_aks) > 0 ? module.azure_aks[0].kube_config_client_certificate : ""
  ) : ""
  kube_config_client_key = local.is_azure ? (
    length(module.azure_aks) > 0 ? module.azure_aks[0].kube_config_client_key : ""
  ) : ""
  kube_config_token = local.is_scaleway ? (
    length(module.scaleway_kubernetes) > 0 ? module.scaleway_kubernetes[0].kube_config_token : ""
  ) : ""

  # ---- Unified cross-cloud outputs ----
  registry_login_server = local.is_azure ? (
    length(module.azure_acr) > 0 ? module.azure_acr[0].login_server : ""
    ) : (
    length(module.scaleway_registry) > 0 ? module.scaleway_registry[0].login_server : ""
  )
  registry_name = local.is_azure ? (
    length(module.azure_acr) > 0 ? module.azure_acr[0].name : ""
    ) : (
    length(module.scaleway_registry) > 0 ? module.scaleway_registry[0].name : ""
  )
  k8s_cluster_name = local.is_azure ? (
    length(module.azure_aks) > 0 ? module.azure_aks[0].name : ""
    ) : (
    length(module.scaleway_kubernetes) > 0 ? module.scaleway_kubernetes[0].name : ""
  )
  ingress_public_ip = local.is_azure ? (
    length(module.azure_networking) > 0 ? module.azure_networking[0].ip_address : ""
    ) : (
    length(module.scaleway_networking) > 0 ? module.scaleway_networking[0].ip_address : ""
  )
  # When no DNS zone is created (create_dns_zone = false), fall back to frontend_domain
  # so shell scripts can still read a domain name from Terraform outputs.
  dns_zone_name = local.is_azure ? (
    length(module.azure_dns) > 0 ? module.azure_dns[0].zone_name : var.frontend_domain
    ) : (
    length(module.scaleway_dns) > 0 ? module.scaleway_dns[0].zone_name : var.frontend_domain
  )
  dns_name_servers = local.is_azure ? (
    length(module.azure_dns) > 0 ? module.azure_dns[0].name_servers : []
    ) : (
    length(module.scaleway_dns) > 0 ? module.scaleway_dns[0].name_servers : []
  )

  # Azure: stable FQDN from public IP DNS label (empty when no dns_label is set)
  azure_pip_fqdn = local.is_azure && length(module.azure_networking) > 0 ? module.azure_networking[0].fqdn : ""
  secrets_store_name = local.is_azure ? (
    var.enable_secrets_manager && length(module.azure_keyvault) > 0 ? module.azure_keyvault[0].name : ""
    ) : (
    var.enable_secrets_manager && length(module.scaleway_secrets) > 0 ? module.scaleway_secrets[0].secret_name : ""
  )
  kube_config_command = local.is_azure ? (
    "az aks get-credentials --resource-group ${var.azure_resource_group_name} --name ${local.k8s_cluster_name} --overwrite-existing"
    ) : (
    length(module.scaleway_kubernetes) > 0 ? "scw k8s kubeconfig install ${module.scaleway_kubernetes[0].id} --region ${var.scw_region}" : ""
  )
}

# ---------------------------------------------------------------------------
# Azure data sources (only queried when cloud_provider = "azure")
# ---------------------------------------------------------------------------
data "azurerm_client_config" "current" {
  # Skip when override variables are provided — avoids Graph API calls that
  # can be blocked by Conditional Access policies ("AADSTS53003").
  count = local.is_azure && var.azure_tenant_id_override == "" ? 1 : 0
}

# ===========================================================================
# AZURE MODULES
# ===========================================================================

module "azure_resource_group" {
  count    = local.is_azure ? 1 : 0
  source   = "./modules/azure/resource_group"
  name     = var.azure_resource_group_name
  location = var.azure_location
  tags     = local.azure_tags
}

module "azure_acr" {
  count               = local.is_azure ? 1 : 0
  source              = "./modules/azure/acr"
  name                = local.azure_acr_name
  resource_group_name = module.azure_resource_group[0].name
  location            = module.azure_resource_group[0].location
  sku                 = var.azure_acr_sku
  tags                = local.azure_tags
}

module "azure_aks" {
  count                             = local.is_azure ? 1 : 0
  source                            = "./modules/azure/aks"
  cluster_name                      = var.cluster_name
  resource_group_name               = module.azure_resource_group[0].name
  location                          = module.azure_resource_group[0].location
  dns_prefix                        = var.azure_aks_dns_prefix
  sku_tier                          = var.azure_aks_sku_tier
  node_count                        = var.node_count
  node_vm_size                      = var.azure_node_vm_size
  acr_id                            = module.azure_acr[0].id
  enable_key_vault_secrets_provider = var.enable_secrets_manager
  tags                              = local.azure_tags
}

module "azure_networking" {
  count               = local.is_azure ? 1 : 0
  source              = "./modules/azure/networking"
  name                = var.azure_ingress_pip_name
  resource_group_name = module.azure_aks[0].node_resource_group
  location            = module.azure_resource_group[0].location
  dns_label           = var.azure_pip_dns_label
  tags                = local.azure_tags
}

module "azure_dns" {
  count               = local.is_azure && var.create_dns_zone ? 1 : 0
  source              = "./modules/azure/dns"
  zone_name           = var.frontend_domain
  resource_group_name = var.azure_dns_resource_group != "" ? var.azure_dns_resource_group : module.azure_resource_group[0].name
  ingress_ip_address  = module.azure_networking[0].ip_address
  cname_subdomains    = var.dns_cname_subdomains
  tags                = local.azure_tags
}

module "azure_keyvault" {
  count                              = local.is_azure && var.enable_secrets_manager ? 1 : 0
  source                             = "./modules/azure/keyvault"
  name                               = local.azure_kv_name
  resource_group_name                = module.azure_resource_group[0].name
  location                           = module.azure_resource_group[0].location
  tenant_id                          = local.effective_tenant_id
  aks_kv_provider_identity_object_id = module.azure_aks[0].key_vault_secrets_provider_identity_object_id
  deployer_object_id                 = local.effective_deployer_object_id
  tags                               = local.azure_tags
}

module "azure_helm_releases" {
  count                       = local.is_azure ? 1 : 0
  source                      = "./modules/azure/helm_releases"
  ingress_nginx_chart_version = var.ingress_nginx_chart_version
  cert_manager_version        = var.cert_manager_version
  ingress_public_ip           = module.azure_networking[0].ip_address
  node_resource_group         = module.azure_aks[0].node_resource_group
  ingress_pip_name            = module.azure_networking[0].name

  depends_on = [module.azure_aks]
}

# ===========================================================================
# SCALEWAY MODULES
# ===========================================================================

module "scaleway_registry" {
  count      = local.is_scaleway ? 1 : 0
  source     = "./modules/scaleway/registry"
  name       = var.scw_registry_name
  region     = var.scw_region
  project_id = var.scw_project_id
}

module "scaleway_kubernetes" {
  count              = local.is_scaleway ? 1 : 0
  source             = "./modules/scaleway/kubernetes"
  cluster_name       = var.cluster_name
  region             = var.scw_region
  project_id         = var.scw_project_id
  kubernetes_version = var.scw_kubernetes_version
  node_type          = var.scw_node_type
  node_count         = var.node_count
  tags               = local.scaleway_tags
}

module "scaleway_networking" {
  count      = local.is_scaleway ? 1 : 0
  source     = "./modules/scaleway/networking"
  region     = var.scw_region
  project_id = var.scw_project_id
}

module "scaleway_dns" {
  count              = local.is_scaleway ? 1 : 0
  source             = "./modules/scaleway/dns"
  zone_name          = var.frontend_domain
  ingress_ip_address = module.scaleway_networking[0].ip_address
  cname_subdomains   = var.dns_cname_subdomains
}

module "scaleway_secrets" {
  count        = local.is_scaleway && var.enable_secrets_manager ? 1 : 0
  source       = "./modules/scaleway/secrets"
  cluster_name = var.cluster_name
  region       = var.scw_region
  project_id   = var.scw_project_id
  tags         = local.scaleway_tags
}

module "scaleway_helm_releases" {
  count                       = local.is_scaleway ? 1 : 0
  source                      = "./modules/scaleway/helm_releases"
  ingress_nginx_chart_version = var.ingress_nginx_chart_version
  cert_manager_version        = var.cert_manager_version
  ingress_public_ip           = module.scaleway_networking[0].ip_address

  depends_on = [module.scaleway_kubernetes]
}
