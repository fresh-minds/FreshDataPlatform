# ---------------------------------------------------------------------------
# Scaleway — dev environment
# Usage: make tf-apply ENVIRONMENT=scaleway-dev
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

# Secrets management
enable_secrets_manager = true

# Tagging
scw_tags = ["project:ai-trial", "env:dev", "managed-by:terraform"]
