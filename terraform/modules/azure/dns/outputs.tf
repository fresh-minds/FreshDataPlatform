output "zone_id" {
  value = azurerm_dns_zone.main.id
}

output "zone_name" {
  value = azurerm_dns_zone.main.name
}

output "name_servers" {
  value = azurerm_dns_zone.main.name_servers
}
