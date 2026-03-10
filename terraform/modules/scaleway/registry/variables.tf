variable "name" {
  description = "Name of the container registry namespace"
  type        = string
}

variable "region" {
  description = "Scaleway region (e.g. fr-par, nl-ams, pl-waw)"
  type        = string
  default     = "nl-ams"
}

variable "project_id" {
  description = "Scaleway project ID"
  type        = string
  default     = ""
}
