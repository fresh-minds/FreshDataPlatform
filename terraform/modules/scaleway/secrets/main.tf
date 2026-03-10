# Scaleway Secret Manager
# Equivalent to: modules/azure/keyvault
#
# Scaleway Secret Manager stores secrets as versioned objects.
# Unlike Azure Key Vault CSI, secrets are injected via the
# external-secrets-operator (ESO) or fetched at runtime via the API.
#
# This module creates one container secret per environment variable group.
# Individual secret values are seeded by the deploy script (aks_up.sh),
# mirroring the Key Vault seeding approach.

resource "scaleway_secret" "odp_env" {
  name        = var.secret_name
  region      = var.region
  project_id  = var.project_id
  description = "ODP environment variables for Kubernetes workloads"
  tags        = var.tags
}

# IAM policy to allow the Kapsule node pool to read this secret at runtime.
# This requires Scaleway IAM (applications + policies).
resource "scaleway_iam_application" "kapsule_secrets_reader" {
  name        = "${var.cluster_name}-secrets-reader"
  description = "Application identity for Kapsule nodes to read ODP secrets"
}

resource "scaleway_iam_policy" "kapsule_secrets_reader" {
  name           = "${var.cluster_name}-secrets-reader-policy"
  application_id = scaleway_iam_application.kapsule_secrets_reader.id

  rule {
    project_ids          = var.project_id != "" ? [var.project_id] : []
    permission_set_names = ["SecretManagerReadOnly"]
  }
}

resource "scaleway_iam_api_key" "kapsule_secrets_reader" {
  application_id = scaleway_iam_application.kapsule_secrets_reader.id
  description    = "API key for Kapsule nodes to read ODP secrets"
  expires_at     = "2026-12-31T00:00:00Z"
}
