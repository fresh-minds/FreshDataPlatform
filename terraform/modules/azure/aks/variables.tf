variable "cluster_name" {
  description = "Name of the AKS cluster"
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

variable "dns_prefix" {
  description = "DNS prefix for the AKS cluster"
  type        = string
}

variable "sku_tier" {
  description = "AKS pricing tier (Free or Standard)"
  type        = string
  default     = "Free"
}

variable "node_count" {
  description = "Number of nodes in the default node pool"
  type        = number
  default     = 1
}

variable "node_vm_size" {
  description = "VM size for the default node pool"
  type        = string
  default     = "Standard_B2s"
}

variable "acr_id" {
  description = "Resource ID of the ACR to attach"
  type        = string
}

variable "enable_key_vault_secrets_provider" {
  description = "Enable the AKS Key Vault secrets provider addon"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags to apply"
  type        = map(string)
  default     = {}
}
