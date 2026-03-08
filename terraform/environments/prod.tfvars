# ---------------------------------------------------------------------------
# Azure — production environment
# Usage: make tf-apply ENVIRONMENT=prod
# ---------------------------------------------------------------------------
cloud_provider = "azure"
environment    = "prod"

# Resource group (overrides default "ai-trial-rg")
azure_resource_group_name = "ai-trial-prod-rg"

# Cluster sizing
node_count         = 3
azure_node_vm_size = "Standard_D4s_v3"
azure_aks_sku_tier = "Standard"

# Registry — Standard tier for geo-replication support
azure_acr_sku = "Standard"

# Networking / DNS
frontend_domain = "eu-sovereigndataplatform.com"

# Secrets management
enable_secrets_manager = true
