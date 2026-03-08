variable "zone_name" {
  description = "DNS zone (root domain), e.g. eu-sovereigndataplatform.com"
  type        = string
}

variable "ingress_ip_address" {
  description = "Public IP address for the A record"
  type        = string
}

variable "ttl" {
  description = "TTL for DNS records in seconds"
  type        = number
  default     = 300
}

variable "cname_subdomains" {
  description = "List of CNAME subdomains pointing to the root domain"
  type        = list(string)
  default = [
    "www",
    "airflow",
    "minio",
    "minio-api",
    "keycloak",
    "datahub",
    "superset",
    "grafana",
    "jupyter",
    "prometheus",
    "alertmanager",
    "dbt-docs",
    "portal-api",
  ]
}
