output "ingress_nginx_status" {
  value = helm_release.ingress_nginx.status
}

output "cert_manager_status" {
  value = helm_release.cert_manager.status
}
