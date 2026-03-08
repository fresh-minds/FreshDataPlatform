# Azure CLI Deploy Scripts (Legacy)

These scripts provision **all Azure infrastructure and Kubernetes workloads** using the `az` CLI directly — no Terraform involved.

This folder preserves the original deployment approach before Terraform was introduced. Use these scripts if you want a self-contained, single-command deployment without any Terraform dependency.

## Scripts

| Script | Purpose |
|---|---|
| `aks_up.sh` | Full deployment: Resource Group, ACR, AKS, DNS, Key Vault, ingress-nginx, cert-manager, all workloads |
| `aks_down.sh` | Teardown: in-cluster workloads and optionally infrastructure resources |
| `aks_up_lib.sh` | Shared library: wait/retry helpers, image build, manifest application |
| `aks_update_images.sh` | Incremental: rebuild and push images, patch running deployments only |

## Usage

```bash
# Full deploy (same as before)
./scripts/azure_cli_deploy/aks_up.sh

# Or via make (still works with original variables)
AKS_LOCATION=westeurope \
AKS_RESOURCE_GROUP=ai-trial-rg \
AKS_CLUSTER_NAME=ai-trial-aks \
  ./scripts/azure_cli_deploy/aks_up.sh

# Teardown (set destructive flags as needed)
DELETE_AKS_CLUSTER=true DELETE_RESOURCE_GROUP=true \
  ./scripts/azure_cli_deploy/aks_down.sh
```

## Why Keep These?

- **Zero external dependencies**: works with just `az`, `kubectl`, `docker`, `kompose`
- **Single-script deployment**: useful for quick prototypes or CI environments without Terraform
- **Reference implementation**: shows exactly what Azure resources are provisioned

## Terraform-based deployment

For the Terraform-managed deployment, see:
- `scripts/aks/aks_up.sh` — workload deployment (reads infra values from Terraform outputs)
- `terraform/` — infrastructure-as-code for Azure (and Scaleway)
