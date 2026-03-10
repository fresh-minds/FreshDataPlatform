# ---------------------------------------------------------------------------
# Scaleway — staging environment
# Usage: make tf-apply ENVIRONMENT=scaleway-staging
# ---------------------------------------------------------------------------
cloud_provider = "scaleway"
environment    = "staging"

# Cluster
cluster_name           = "ai-trial-staging"
node_count             = 2
scw_node_type          = "GP1-S"
scw_kubernetes_version = "1.35"

# Registry
scw_registry_name = "ai-trial-staging"

# Region / zone
scw_region = "nl-ams"
scw_zone   = "nl-ams-1"

# Networking / DNS
frontend_domain = "staging.eu-sovereigndataplatform.com"

# Secrets management
enable_secrets_manager = true

# Tagging
scw_tags = ["project:ai-trial", "env:staging", "managed-by:terraform"]
