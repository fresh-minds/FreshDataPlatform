resource "azurerm_key_vault" "main" {
  name                       = var.name
  location                   = var.location
  resource_group_name        = var.resource_group_name
  tenant_id                  = var.tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true
  soft_delete_retention_days = 90
  purge_protection_enabled   = false
  tags                       = var.tags
}

# Allow AKS Key Vault provider identity to read secrets
resource "azurerm_role_assignment" "kv_secrets_user" {
  principal_id                     = var.aks_kv_provider_identity_object_id
  role_definition_name             = "Key Vault Secrets User"
  scope                            = azurerm_key_vault.main.id
  skip_service_principal_aad_check = true
}

# Allow deployer to write secrets
resource "azurerm_role_assignment" "kv_secrets_officer" {
  count                = var.deployer_object_id != "" ? 1 : 0
  principal_id         = var.deployer_object_id
  role_definition_name = "Key Vault Secrets Officer"
  scope                = azurerm_key_vault.main.id
}
