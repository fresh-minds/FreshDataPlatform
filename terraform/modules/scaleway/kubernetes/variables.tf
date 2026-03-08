variable "cluster_name" {
  description = "Name of the Kapsule cluster"
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

variable "kubernetes_version" {
  description = "Kubernetes version (e.g. '1.30')"
  type        = string
  default     = "1.30"
}

variable "cni" {
  description = "CNI plugin (cilium, calico, weave, flannel)"
  type        = string
  default     = "cilium"
}

variable "node_type" {
  description = "Scaleway node type (e.g. DEV1-M, GP1-S, PRO2-S)"
  type        = string
  default     = "DEV1-M"
}

variable "node_count" {
  description = "Number of nodes in the pool"
  type        = number
  default     = 1
}

variable "node_max_count" {
  description = "Maximum nodes when autoscaling is enabled"
  type        = number
  default     = 3
}

variable "autoscaling_enabled" {
  description = "Enable cluster autoscaler"
  type        = bool
  default     = false
}

variable "auto_upgrade_enabled" {
  description = "Enable automatic Kubernetes version upgrades"
  type        = bool
  default     = true
}

variable "maintenance_window_start_hour" {
  description = "Start hour (0-23) for maintenance window"
  type        = number
  default     = 3
}

variable "maintenance_window_day" {
  description = "Day of week for maintenance window (monday…sunday, any)"
  type        = string
  default     = "sunday"
}

variable "tags" {
  description = "Tags to apply to the cluster and pool"
  type        = list(string)
  default     = []
}
