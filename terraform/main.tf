data "azurerm_client_config" "current" {}

locals {
  sub_hash = substr(replace(data.azurerm_client_config.current.subscription_id, "-", ""), 0, 8)
  acr_name = var.acr_name != "" ? var.acr_name : "aitrial${local.sub_hash}"
  kv_name  = var.key_vault_name != "" ? var.key_vault_name : "aitrialkv${local.sub_hash}"

  common_tags = merge(var.tags, {
    environment = var.environment
    managed_by  = "terraform"
  })
}

module "resource_group" {
  source   = "./modules/resource_group"
  name     = var.resource_group_name
  location = var.location
  tags     = local.common_tags
}

module "acr" {
  source              = "./modules/acr"
  name                = local.acr_name
  resource_group_name = module.resource_group.name
  location            = module.resource_group.location
  sku                 = var.acr_sku
  tags                = local.common_tags
}

module "aks" {
  source                            = "./modules/aks"
  cluster_name                      = var.aks_cluster_name
  resource_group_name               = module.resource_group.name
  location                          = module.resource_group.location
  dns_prefix                        = var.aks_dns_prefix
  sku_tier                          = var.aks_sku_tier
  node_count                        = var.aks_node_count
  node_vm_size                      = var.aks_node_vm_size
  acr_id                            = module.acr.id
  enable_key_vault_secrets_provider = var.enable_key_vault
  tags                              = local.common_tags
}

module "networking" {
  source              = "./modules/networking"
  name                = var.ingress_pip_name
  resource_group_name = module.aks.node_resource_group
  location            = module.resource_group.location
  tags                = local.common_tags
}

module "dns" {
  source              = "./modules/dns"
  zone_name           = var.frontend_domain
  resource_group_name = var.dns_resource_group != "" ? var.dns_resource_group : module.resource_group.name
  ingress_ip_address  = module.networking.ip_address
  cname_subdomains    = var.dns_cname_subdomains
  tags                = local.common_tags
}

module "keyvault" {
  count                              = var.enable_key_vault ? 1 : 0
  source                             = "./modules/keyvault"
  name                               = local.kv_name
  resource_group_name                = module.resource_group.name
  location                           = module.resource_group.location
  tenant_id                          = data.azurerm_client_config.current.tenant_id
  aks_kv_provider_identity_object_id = module.aks.key_vault_secrets_provider_identity_object_id
  deployer_object_id                 = data.azurerm_client_config.current.object_id
  tags                               = local.common_tags
}

module "helm_releases" {
  source                      = "./modules/helm_releases"
  ingress_nginx_chart_version = var.ingress_nginx_chart_version
  cert_manager_version        = var.cert_manager_version
  ingress_public_ip           = module.networking.ip_address
  node_resource_group         = module.aks.node_resource_group
  ingress_pip_name            = module.networking.name

  depends_on = [module.aks]
}
