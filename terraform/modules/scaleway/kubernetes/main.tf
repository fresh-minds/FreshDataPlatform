# Scaleway Kubernetes (Kapsule) cluster + node pool
# Equivalent to: modules/azure/aks

resource "scaleway_vpc_private_network" "k8s" {
  name       = "${var.cluster_name}-pn"
  region     = var.region
  project_id = var.project_id
  tags       = var.tags
}

resource "scaleway_k8s_cluster" "main" {
  name    = var.cluster_name
  version = var.kubernetes_version != "" ? var.kubernetes_version : null
  cni     = var.cni

  region     = var.region
  project_id = var.project_id
  private_network_id = scaleway_vpc_private_network.k8s.id

  # Required: whether to delete additional resources (LBs, volumes) on destroy
  delete_additional_resources = true

  # Auto-upgrade minor versions within a channel
  auto_upgrade {
    enable                        = var.auto_upgrade_enabled
    maintenance_window_start_hour = var.maintenance_window_start_hour
    maintenance_window_day        = var.maintenance_window_day
  }

  tags = var.tags
}

resource "scaleway_k8s_pool" "main" {
  cluster_id = scaleway_k8s_cluster.main.id
  name       = "default"
  node_type  = var.node_type
  size       = var.node_count
  region     = var.region

  autoscaling       = var.autoscaling_enabled
  min_size          = var.autoscaling_enabled ? var.node_count : null
  max_size          = var.autoscaling_enabled ? var.node_max_count : null
  autohealing       = true
  container_runtime = "containerd"

  tags = var.tags
}
