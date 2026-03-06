output "id" {
  value = azurerm_kubernetes_cluster.main.id
}

output "name" {
  value = azurerm_kubernetes_cluster.main.name
}

output "node_resource_group" {
  value = azurerm_kubernetes_cluster.main.node_resource_group
}

output "kubelet_identity_object_id" {
  value = azurerm_kubernetes_cluster.main.kubelet_identity[0].object_id
}

output "kube_config_host" {
  value     = azurerm_kubernetes_cluster.main.kube_config[0].host
  sensitive = true
}

output "kube_config_client_certificate" {
  value     = azurerm_kubernetes_cluster.main.kube_config[0].client_certificate
  sensitive = true
}

output "kube_config_client_key" {
  value     = azurerm_kubernetes_cluster.main.kube_config[0].client_key
  sensitive = true
}

output "kube_config_cluster_ca_certificate" {
  value     = azurerm_kubernetes_cluster.main.kube_config[0].cluster_ca_certificate
  sensitive = true
}

output "key_vault_secrets_provider_identity_client_id" {
  value = try(
    azurerm_kubernetes_cluster.main.key_vault_secrets_provider[0].secret_identity[0].client_id,
    ""
  )
}

output "key_vault_secrets_provider_identity_object_id" {
  value = try(
    azurerm_kubernetes_cluster.main.key_vault_secrets_provider[0].secret_identity[0].object_id,
    ""
  )
}
