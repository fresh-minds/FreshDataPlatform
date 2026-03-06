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
  description = "Static public IP for the ingress controller load balancer"
  type        = string
}

variable "node_resource_group" {
  description = "AKS node resource group name (for Azure LB annotation)"
  type        = string
}

variable "ingress_pip_name" {
  description = "Name of the Azure public IP resource (for azure-pip-name annotation)"
  type        = string
}
