# Scaleway DNS zone + A/CNAME records
# Equivalent to: modules/azure/dns
#
# Scaleway Domains & DNS manages zones for domains registered or delegated
# to Scaleway. The zone is identified by its root domain name.

resource "scaleway_domain_zone" "main" {
  domain    = var.zone_name
  subdomain = "@"
}

resource "scaleway_domain_record" "root_a" {
  dns_zone = scaleway_domain_zone.main.domain
  name     = ""
  type     = "A"
  data     = var.ingress_ip_address
  ttl      = var.ttl
}

resource "scaleway_domain_record" "subdomains" {
  for_each = toset(var.cname_subdomains)

  dns_zone = scaleway_domain_zone.main.domain
  name     = each.value
  type     = "CNAME"
  data     = "${var.zone_name}."
  ttl      = var.ttl
}
