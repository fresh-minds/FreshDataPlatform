output "zone_name" {
  value = scaleway_domain_zone.main.domain
}

output "name_servers" {
  value = scaleway_domain_zone.main.ns
}
