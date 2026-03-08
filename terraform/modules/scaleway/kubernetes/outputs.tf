output "id" {
  value = scaleway_k8s_cluster.main.id
}

output "name" {
  value = scaleway_k8s_cluster.main.name
}

output "kubeconfig_raw" {
  description = "Raw kubeconfig YAML for the cluster"
  value       = scaleway_k8s_cluster.main.kubeconfig[0].config_file
  sensitive   = true
}

output "kube_config_host" {
  value     = scaleway_k8s_cluster.main.kubeconfig[0].host
  sensitive = true
}

output "kube_config_token" {
  value     = scaleway_k8s_cluster.main.kubeconfig[0].token
  sensitive = true
}

output "kube_config_cluster_ca_certificate" {
  value     = scaleway_k8s_cluster.main.kubeconfig[0].cluster_ca_certificate
  sensitive = true
}

output "pool_id" {
  value = scaleway_k8s_pool.main.id
}

output "status" {
  value = scaleway_k8s_cluster.main.status
}
