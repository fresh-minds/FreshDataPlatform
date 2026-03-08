resource "azurerm_dns_zone" "main" {
  name                = var.zone_name
  resource_group_name = var.resource_group_name
  tags                = var.tags
}

resource "azurerm_dns_a_record" "root" {
  name                = "@"
  zone_name           = azurerm_dns_zone.main.name
  resource_group_name = var.resource_group_name
  ttl                 = var.ttl
  records             = [var.ingress_ip_address]
}

resource "azurerm_dns_cname_record" "subdomains" {
  for_each            = toset(var.cname_subdomains)
  name                = each.value
  zone_name           = azurerm_dns_zone.main.name
  resource_group_name = var.resource_group_name
  ttl                 = var.ttl
  record              = var.zone_name
}
