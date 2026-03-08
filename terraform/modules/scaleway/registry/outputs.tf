output "id" {
  value = scaleway_registry_namespace.main.id
}

output "name" {
  value = scaleway_registry_namespace.main.name
}

# Login server URL: e.g. rg.nl-ams.scw.cloud/<name>
output "login_server" {
  value = scaleway_registry_namespace.main.endpoint
}

output "endpoint" {
  value = scaleway_registry_namespace.main.endpoint
}
