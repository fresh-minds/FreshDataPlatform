output "cloud_provider" {
	value = "scaleway"
}

output "cluster_name" {
	value = module.scaleway_kubernetes.name
}

output "registry_login_server" {
	value = module.scaleway_registry.login_server
}

output "registry_name" {
	value = module.scaleway_registry.name
}

output "ingress_public_ip" {
	value = module.scaleway_networking.ip_address
}

output "dns_zone_name" {
	value = var.create_dns_zone ? module.scaleway_dns[0].zone_name : var.frontend_domain
}

output "dns_name_servers" {
	value = var.create_dns_zone ? module.scaleway_dns[0].name_servers : []
}

output "secrets_store_name" {
	value = var.enable_secrets_manager ? module.scaleway_secrets[0].secret_name : ""
}

output "kube_config_command" {
	value = "scw k8s kubeconfig install ${module.scaleway_kubernetes.id} --region ${var.scw_region}"
}

output "kube_config_host" {
	value     = module.scaleway_kubernetes.kube_config_host
	sensitive = true
}

output "kube_config_token" {
	value     = module.scaleway_kubernetes.kube_config_token
	sensitive = true
}

output "kube_config_cluster_ca_certificate" {
	value     = module.scaleway_kubernetes.kube_config_cluster_ca_certificate
	sensitive = true
}

output "scw_region" {
	value = var.scw_region
}

output "scw_cluster_id" {
	value = module.scaleway_kubernetes.id
}

output "scw_reader_api_access_key" {
	value = var.enable_secrets_manager ? module.scaleway_secrets[0].reader_api_access_key : ""
}

output "scw_reader_api_secret_key" {
	value     = var.enable_secrets_manager ? module.scaleway_secrets[0].reader_api_secret_key : ""
	sensitive = true
}
