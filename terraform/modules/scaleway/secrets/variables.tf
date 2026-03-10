variable "secret_name" {
  description = "Name of the Scaleway secret (analogous to the Key Vault name)"
  type        = string
  default     = "odp-env"
}

variable "cluster_name" {
  description = "Name of the Kapsule cluster (used to name IAM resources)"
  type        = string
}

variable "region" {
  description = "Scaleway region"
  type        = string
  default     = "nl-ams"
}

variable "project_id" {
  description = "Scaleway project ID"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Tags for the secret"
  type        = list(string)
  default     = []
}
