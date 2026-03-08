# ---------------------------------------------------------------------------
# Azure — staging environment
# Usage: make tf-apply ENVIRONMENT=staging
# ---------------------------------------------------------------------------
cloud_provider = "azure"
environment    = "staging"

# Resource group (overrides default "ai-trial-rg")
azure_resource_group_name = "ai-trial-staging-rg"

# Cluster sizing
node_count         = 2
azure_node_vm_size = "Standard_D2s_v3"
azure_aks_sku_tier = "Standard"

# Registry
azure_acr_sku = "Basic"

# Networking / DNS
frontend_domain = "staging.eu-sovereigndataplatform.com"

# Secrets management
enable_secrets_manager = true
