variable "name" {
  description = "Name of the public IP resource"
  type        = string
}

variable "resource_group_name" {
  description = "Name of the resource group (AKS node resource group)"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "tags" {
  description = "Tags to apply"
  type        = map(string)
  default     = {}
}
