variable "ingress_nginx_chart_version" {
  description = "Helm chart version for ingress-nginx"
  type        = string
  default     = "4.12.3"
}

variable "cert_manager_version" {
  description = "Helm chart version for cert-manager"
  type        = string
  default     = "v1.19.3"
}

variable "ingress_public_ip" {
  description = "Reserved Scaleway LB IP for ingress controller"
  type        = string
}

variable "ingress_replica_count" {
  description = "Number of ingress-nginx controller replicas"
  type        = number
  default     = 1
}
