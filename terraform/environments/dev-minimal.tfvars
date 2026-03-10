# ---------------------------------------------------------------------------
# Azure — dev-minimal environment (no DataHub, no heavy observability, no jupyter)
# Usage: make tf-apply ENVIRONMENT=dev-minimal
# ---------------------------------------------------------------------------
cloud_provider = "azure"
environment    = "dev"

# Cluster sizing
node_count         = 1
azure_node_vm_size = "Standard_B2s"
azure_aks_sku_tier = "Free"

# Registry
azure_acr_sku = "Basic"

# Networking / DNS
frontend_domain = "eu-sovereigndataplatform.com"

# Reduced subdomains — only services deployed in --minimal mode
dns_cname_subdomains = [
  "www",
  "airflow",
  "minio",
  "minio-api",
  "keycloak",
  "superset",
  "dbt-docs",
  "portal-api",
]

# Secrets management
enable_secrets_manager = true
