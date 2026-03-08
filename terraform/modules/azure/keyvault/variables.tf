variable "name" {
  description = "Name of the Key Vault"
  type        = string
}

variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "tenant_id" {
  description = "Azure AD tenant ID"
  type        = string
}

variable "aks_kv_provider_identity_object_id" {
  description = "Object ID of the AKS Key Vault secrets provider identity"
  type        = string
}

variable "deployer_object_id" {
  description = "Object ID of the deployer principal for Key Vault Secrets Officer role"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Tags to apply"
  type        = map(string)
  default     = {}
}
