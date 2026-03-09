# ---------------------------------------------------------------------------
# Scaleway — dev-minimal environment (no DataHub, no heavy observability, no jupyter)
# Usage: make tf-apply ENVIRONMENT=scaleway-dev-minimal
#
# Credentials are read from environment variables (preferred) or tfvars:
#   SCW_ACCESS_KEY, SCW_SECRET_KEY, SCW_DEFAULT_PROJECT_ID
# Do NOT commit scw_access_key / scw_secret_key to version control.
# ---------------------------------------------------------------------------
cloud_provider = "scaleway"
environment    = "dev"

# Cluster
cluster_name           = "ai-trial"
node_count             = 1
scw_node_type          = "DEV1-M"
scw_kubernetes_version = "1.35"

# Registry
scw_registry_name = "ai-trial"

# Region / zone
scw_region = "nl-ams"
scw_zone   = "nl-ams-1"

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

# Minimal profile skips Terraform-managed Helm control-plane add-ons
# (ingress-nginx + cert-manager) to keep redeploy scope lightweight.
enable_helm_releases = false

# Tagging
scw_tags = ["project:ai-trial", "env:dev", "managed-by:terraform"]
