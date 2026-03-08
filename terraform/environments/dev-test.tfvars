# ---------------------------------------------------------------------------
# Azure — dev-test environment (dev-opendataplatform-rg)
#
# Uses sslip.io instead of a purchased domain:
#   After 'terraform apply', get the ingress IP with:
#     terraform output -raw ingress_public_ip
#   Then run aks_up.sh with:
#     FRONTEND_DOMAIN=<ip>.sslip.io make k8s-aks-up
#
# The azure_pip_fqdn output gives the stable cloudapp.azure.com hostname
# for direct browser access to the root domain.
# ---------------------------------------------------------------------------
cloud_provider = "azure"
environment    = "dev"

# Test resource group
azure_resource_group_name = "dev-opendataplatform-rg"

# Unique names to avoid conflict with existing ai-trial-rg resources
cluster_name           = "dev-odp-aks"
azure_acr_name         = "devopendataplatform"
azure_key_vault_name   = "dev-odp-kv"
azure_ingress_pip_name = "dev-odp-ingress-pip"
azure_aks_dns_prefix   = "dev-odp"

# Stable Azure-provided hostname (no domain purchase needed)
azure_pip_dns_label = "dev-odp-ingress"

# Skip DNS zone — using sslip.io / cloudapp.azure.com instead
create_dns_zone = false

# frontier_domain is used as a label only (no DNS zone created)
frontend_domain = "placeholder.sslip.io"

# Cluster sizing — single small node for testing
node_count         = 1
azure_node_vm_size = "Standard_B4ms"
azure_aks_sku_tier = "Free"

# Registry
azure_acr_sku = "Basic"

# Secrets management — disabled for this test to keep the deployment simple
# (also avoids needing azure_deployer_object_id when using the override path)
enable_secrets_manager = false

# Azure auth — use default CLI auth (az login must have a cached Graph API token).
# If Terraform fails with AADSTS53003, run once in your terminal:
#   az login --scope https://graph.microsoft.com/.default
# then re-run the plan. The azurerm v4 provider always calls the Graph API via
# the CLI during initialization and cannot use a raw access token.
azure_tenant_id_override       = "fedcef2f-0c85-40dd-8f55-e23143dcb367"
azure_subscription_id_override = "4910a5a6-aec6-405d-9294-c7f2845512a4"

# Tags
azure_tags = {
  purpose = "terraform-test"
  managed = "terraform"
}
