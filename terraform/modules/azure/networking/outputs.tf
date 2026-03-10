output "id" {
  value = azurerm_public_ip.ingress.id
}

output "ip_address" {
  value = azurerm_public_ip.ingress.ip_address
}

output "name" {
  value = azurerm_public_ip.ingress.name
}

output "fqdn" {
  description = "Azure-provided FQDN for the public IP (<dns_label>.<region>.cloudapp.azure.com) — empty if no dns_label was set"
  value       = azurerm_public_ip.ingress.fqdn != null ? azurerm_public_ip.ingress.fqdn : ""
}
