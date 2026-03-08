output "secret_id" {
  value = scaleway_secret.odp_env.id
}

output "secret_name" {
  value = scaleway_secret.odp_env.name
}

output "reader_application_id" {
  value = scaleway_iam_application.kapsule_secrets_reader.id
}

output "reader_api_key_id" {
  description = "Terraform resource ID of the IAM API key"
  value       = scaleway_iam_api_key.kapsule_secrets_reader.id
}

output "reader_api_access_key" {
  description = "Access key (public part) for the Kapsule secrets reader — used as ESO store access_key"
  value       = scaleway_iam_api_key.kapsule_secrets_reader.access_key
}

output "reader_api_secret_key" {
  description = "Secret key for the Kapsule node pool IAM application (used by external-secrets-operator)"
  value       = scaleway_iam_api_key.kapsule_secrets_reader.secret_key
  sensitive   = true
}
