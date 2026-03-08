resource "azurerm_public_ip" "ingress" {
  name                = var.name
  resource_group_name = var.resource_group_name
  location            = var.location
  allocation_method   = "Static"
  sku                 = "Standard"

  # Optional stable Azure-provided hostname: <dns_label>.<region>.cloudapp.azure.com
  domain_name_label = var.dns_label != "" ? var.dns_label : null

  tags = var.tags
}
