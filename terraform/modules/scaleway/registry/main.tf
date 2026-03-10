# Scaleway Container Registry namespace
# Equivalent to: modules/azure/acr

resource "scaleway_registry_namespace" "main" {
  name       = var.name
  region     = var.region
  is_public  = false
  project_id = var.project_id
}
