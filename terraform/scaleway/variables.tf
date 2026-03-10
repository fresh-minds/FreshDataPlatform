variable "cloud_provider" {
  type    = string
  default = "scaleway"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "cluster_name" {
  type    = string
  default = "ai-trial"
}

variable "frontend_domain" {
  type = string
}
variable "dns_cname_subdomains" {
  type = list(string)
  default = ["www","airflow","minio","minio-api","keycloak","datahub","superset","grafana","jupyter","prometheus","alertmanager","dbt-docs","portal-api"]
}
variable "create_dns_zone" {
  type    = bool
  default = false
}
variable "node_count" {
  type    = number
  default = 1
}

variable "ingress_nginx_chart_version" {
  type    = string
  default = "4.12.3"
}

variable "cert_manager_version" {
  type    = string
  default = "v1.19.3"
}

variable "enable_helm_releases" {
  type    = bool
  default = true
}

variable "enable_secrets_manager" {
  type    = bool
  default = true
}

variable "scw_access_key" {
  type      = string
  default   = ""
  sensitive = true
}

variable "scw_secret_key" {
  type      = string
  default   = ""
  sensitive = true
}

variable "scw_project_id" {
  type = string
}

variable "scw_region" {
  type    = string
  default = "nl-ams"
}

variable "scw_zone" {
  type    = string
  default = "nl-ams-1"
}

variable "scw_registry_name" {
  type    = string
  default = "ai-trial"
}

variable "scw_node_type" {
  type    = string
  default = "DEV1-M"
}

variable "scw_kubernetes_version" {
  type    = string
  default = ""
}

variable "scw_tags" {
  type    = list(string)
  default = []
}
