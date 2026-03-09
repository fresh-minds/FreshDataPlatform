# ---------------------------------------------------------------------------
# Azure — dev-opendataplatform-minimal environment
# Standalone minimal deployment to dev-opendataplatform-rg for testing.
#
# Uses sslip.io for IP-embedded subdomain routing (no custom DNS zone needed).
# After first terraform apply, get the public IP and derive the domain:
#   IP=$(terraform -chdir=terraform output -raw ingress_public_ip)
#   FRONTEND_DOMAIN="${IP//./-}.sslip.io"
# Then run:
#   TF_WORKSPACE=dev-opendataplatform FRONTEND_DOMAIN="$FRONTEND_DOMAIN" make k8s-aks-up-minimal
# ---------------------------------------------------------------------------
cloud_provider = "azure"
environment    = "dev"

# Resource group
azure_resource_group_name = "dev-opendataplatform-rg"

# Cluster — distinct name from the existing ai-trial cluster
cluster_name         = "odp-minimal"
azure_aks_dns_prefix = "odp-minimal"

# Cluster sizing
node_count         = 1
azure_node_vm_size = "Standard_B2s"
azure_aks_sku_tier = "Free"

# Registry — explicit globally-unique name (cannot collide with ai-trial ACR)
azure_acr_name = "odpminimalacr"
azure_acr_sku  = "Basic"

# Key Vault — explicit globally-unique name (≤24 chars, alphanumeric + hyphens)
azure_key_vault_name = "odp-minimal-kv"

# Public IP — separate name to avoid any naming collision
azure_ingress_pip_name = "odp-minimal-ingress-pip"

# DNS label for stable Azure FQDN (optional):
#   odp-dev-minimal.westeurope.cloudapp.azure.com
azure_pip_dns_label = "odp-dev-minimal"

# No managed DNS zone — sslip.io provides free IP-embedded subdomain routing.
# The frontend_domain placeholder is returned by `terraform output dns_zone_name`
# and will be overridden by the FRONTEND_DOMAIN env var when running aks_up.sh.
create_dns_zone = false
frontend_domain = "52-233-252-231.sslip.io"

# Reduced subdomains (not used for DNS zone, kept for reference / future use)
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

# Azure auth — bypass Graph API calls blocked by Conditional Access.
# Pass ARM_ACCESS_TOKEN, ARM_TENANT_ID, ARM_SUBSCRIPTION_ID as env vars.
azure_use_cli                  = true
azure_tenant_id_override       = "fedcef2f-0c85-40dd-8f55-e23143dcb367"
azure_subscription_id_override = "4910a5a6-aec6-405d-9294-c7f2845512a4"
azure_deployer_object_id       = "32bd563b-aa5e-47ed-89b0-7b24476e9785"

# Tagging
azure_tags = {
  project    = "odp-minimal"
  env        = "dev"
  managed_by = "terraform"
}
