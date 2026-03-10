locals {
  scaleway_tags = distinct(concat(var.scw_tags, ["env:${var.environment}", "managed-by:terraform", "cloud:scaleway"]))
}

provider "scaleway" {
  access_key = var.scw_access_key != "" ? var.scw_access_key : "SCWXXXXXXXXXXXXXXXXX"
  secret_key = var.scw_secret_key != "" ? var.scw_secret_key : "00000000-0000-0000-0000-000000000000"
  project_id = var.scw_project_id
  region     = var.scw_region
  zone       = var.scw_zone
}

module "scaleway_registry" {
  source     = "../modules/scaleway/registry"
  name       = var.scw_registry_name
  region     = var.scw_region
  project_id = var.scw_project_id
}

module "scaleway_kubernetes" {
  source             = "../modules/scaleway/kubernetes"
  cluster_name       = var.cluster_name
  region             = var.scw_region
  project_id         = var.scw_project_id
  kubernetes_version = var.scw_kubernetes_version
  node_type          = var.scw_node_type
  node_count         = var.node_count
  tags               = local.scaleway_tags
}

provider "kubernetes" {
  host                   = module.scaleway_kubernetes.kube_config_host
  cluster_ca_certificate = base64decode(module.scaleway_kubernetes.kube_config_cluster_ca_certificate)
  token                  = module.scaleway_kubernetes.kube_config_token
}

provider "helm" {
  kubernetes {
    host                   = module.scaleway_kubernetes.kube_config_host
    cluster_ca_certificate = base64decode(module.scaleway_kubernetes.kube_config_cluster_ca_certificate)
    token                  = module.scaleway_kubernetes.kube_config_token
  }
}

module "scaleway_networking" {
  source     = "../modules/scaleway/networking"
  region     = var.scw_region
  project_id = var.scw_project_id
}

module "scaleway_dns" {
  count              = var.create_dns_zone ? 1 : 0
  source             = "../modules/scaleway/dns"
  zone_name          = var.frontend_domain
  ingress_ip_address = module.scaleway_networking.ip_address
  cname_subdomains   = var.dns_cname_subdomains
}

module "scaleway_secrets" {
  count        = var.enable_secrets_manager ? 1 : 0
  source       = "../modules/scaleway/secrets"
  cluster_name = var.cluster_name
  region       = var.scw_region
  project_id   = var.scw_project_id
  tags         = local.scaleway_tags
}

module "scaleway_helm_releases" {
  count                       = var.enable_helm_releases ? 1 : 0
  source                      = "../modules/scaleway/helm_releases"
  ingress_nginx_chart_version = var.ingress_nginx_chart_version
  cert_manager_version        = var.cert_manager_version
  ingress_public_ip           = module.scaleway_networking.ip_address

  depends_on = [module.scaleway_kubernetes]
}
