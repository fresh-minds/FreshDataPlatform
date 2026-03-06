output "resource_group_name" {
  description = "Name of the resource group"
  value       = module.resource_group.name
}

output "acr_login_server" {
  description = "ACR login server URL"
  value       = module.acr.login_server
}

output "acr_name" {
  description = "Name of the Azure Container Registry"
  value       = module.acr.name
}

output "aks_cluster_name" {
  description = "Name of the AKS cluster"
  value       = module.aks.name
}

output "aks_node_resource_group" {
  description = "AKS node resource group (MC_*)"
  value       = module.aks.node_resource_group
}

output "ingress_public_ip" {
  description = "Static public IP for ingress"
  value       = module.networking.ip_address
}

output "dns_zone_name" {
  description = "DNS zone name"
  value       = module.dns.zone_name
}

output "dns_name_servers" {
  description = "DNS zone name servers"
  value       = module.dns.name_servers
}

output "key_vault_name" {
  description = "Name of the Azure Key Vault"
  value       = var.enable_key_vault ? module.keyvault[0].name : ""
}

output "key_vault_uri" {
  description = "URI of the Azure Key Vault"
  value       = var.enable_key_vault ? module.keyvault[0].vault_uri : ""
}

output "aks_kv_provider_identity_client_id" {
  description = "Client ID of the AKS Key Vault secrets provider identity"
  value       = module.aks.key_vault_secrets_provider_identity_client_id
}

output "tenant_id" {
  description = "Azure AD tenant ID"
  value       = data.azurerm_client_config.current.tenant_id
}

output "kube_config_command" {
  description = "Command to configure kubectl"
  value       = "az aks get-credentials --resource-group ${module.resource_group.name} --name ${module.aks.name} --overwrite-existing"
}
