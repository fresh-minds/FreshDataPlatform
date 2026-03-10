# ---------------------------------------------------------------------------
# Cloud-agnostic outputs
#
# Unified locals in main.tf abstract away provider differences.
# Azure-specific outputs are empty strings when cloud_provider = "scaleway".
# Scaleway-specific outputs are empty strings when cloud_provider = "azure".
#
# Shell scripts read these via: terraform output -json | jq -r '.<key>.value'
# ---------------------------------------------------------------------------

# ---- Active cloud ----

output "cloud_provider" {
  description = "Active cloud provider (azure or scaleway)"
  value       = var.cloud_provider
}

# ---- Cross-cloud unified outputs ----

output "cluster_name" {
  description = "Kubernetes cluster name"
  value       = local.k8s_cluster_name
}

output "registry_login_server" {
  description = "Container registry login server URL"
  value       = local.registry_login_server
}

output "registry_name" {
  description = "Container registry name"
  value       = local.registry_name
}

output "ingress_public_ip" {
  description = "Static public IP address for the ingress controller"
  value       = local.ingress_public_ip
}

output "dns_zone_name" {
  description = "DNS zone / root domain name"
  value       = local.dns_zone_name
}

output "dns_name_servers" {
  description = "Authoritative name servers for the DNS zone"
  value       = local.dns_name_servers
}

output "secrets_store_name" {
  description = "Name of the cloud-managed secret store (Key Vault name or Scaleway secret name)"
  value       = local.secrets_store_name
}

output "kube_config_command" {
  description = "CLI command to fetch/update local kubeconfig for the provisioned cluster"
  value       = local.kube_config_command
}

# ---- Azure-specific outputs (empty string when cloud_provider = "scaleway") ----

output "azure_resource_group_name" {
  description = "Azure resource group name (empty for Scaleway)"
  value       = local.is_azure ? var.azure_resource_group_name : ""
}

output "azure_node_resource_group" {
  description = "AKS node resource group (MC_* pattern — empty for Scaleway)"
  value       = local.is_azure && length(module.azure_aks) > 0 ? module.azure_aks[0].node_resource_group : ""
}

output "azure_pip_fqdn" {
  description = "Azure-provided FQDN for the ingress public IP (<dns_label>.<region>.cloudapp.azure.com — empty if no dns_label or for Scaleway)"
  value       = local.azure_pip_fqdn
}

output "azure_key_vault_uri" {
  description = "Azure Key Vault URI (empty for Scaleway)"
  value       = local.is_azure && var.enable_secrets_manager && length(module.azure_keyvault) > 0 ? module.azure_keyvault[0].vault_uri : ""
}

output "azure_kv_provider_identity_client_id" {
  description = "Client ID of the AKS Key Vault secrets provider managed identity (empty for Scaleway)"
  value       = local.is_azure && length(module.azure_aks) > 0 ? module.azure_aks[0].key_vault_secrets_provider_identity_client_id : ""
}

output "azure_tenant_id" {
  description = "Azure AD tenant ID (empty for Scaleway)"
  value       = local.is_azure ? local.effective_tenant_id : ""
}

# ---- Scaleway-specific outputs (empty string when cloud_provider = "azure") ----

output "scw_region" {
  description = "Active Scaleway region (empty for Azure)"
  value       = local.is_scaleway ? var.scw_region : ""
}

output "scw_cluster_id" {
  description = "Scaleway Kapsule cluster ID — used in scw k8s kubeconfig install (empty for Azure)"
  value       = local.is_scaleway && length(module.scaleway_kubernetes) > 0 ? module.scaleway_kubernetes[0].id : ""
}

output "scw_reader_api_access_key" {
  description = "Scaleway IAM access key for the Kapsule secrets reader (for external-secrets-operator — empty for Azure)"
  value       = local.is_scaleway && var.enable_secrets_manager && length(module.scaleway_secrets) > 0 ? module.scaleway_secrets[0].reader_api_access_key : ""
}

output "scw_reader_api_secret_key" {
  description = "Scaleway IAM secret key for the Kapsule secrets reader (sensitive — empty for Azure)"
  value       = local.is_scaleway && var.enable_secrets_manager && length(module.scaleway_secrets) > 0 ? module.scaleway_secrets[0].reader_api_secret_key : ""
  sensitive   = true
}
