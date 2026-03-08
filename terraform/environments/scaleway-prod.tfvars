# ---------------------------------------------------------------------------
# Scaleway — production environment
# Usage: make tf-apply ENVIRONMENT=scaleway-prod
# ---------------------------------------------------------------------------
cloud_provider = "scaleway"
environment    = "prod"

# Cluster
cluster_name           = "ai-trial-prod"
node_count             = 3
scw_node_type          = "PRO2-S"
scw_kubernetes_version = "1.35"

# Registry
scw_registry_name = "ai-trial-prod"

# Region / zone
scw_region = "nl-ams"
scw_zone   = "nl-ams-1"

# Networking / DNS
frontend_domain = "eu-sovereigndataplatform.com"

# Secrets management
enable_secrets_manager = true

# Tagging
scw_tags = ["project:ai-trial", "env:prod", "managed-by:terraform"]
