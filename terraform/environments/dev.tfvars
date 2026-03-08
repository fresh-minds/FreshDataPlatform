# ---------------------------------------------------------------------------
# Azure — dev environment
# Usage: make tf-apply ENVIRONMENT=dev
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

# Secrets management
enable_secrets_manager = true
