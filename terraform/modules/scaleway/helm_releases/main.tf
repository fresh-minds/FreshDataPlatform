# Scaleway Helm releases for cluster-level infrastructure
# Equivalent to: modules/azure/helm_releases
#
# ingress-nginx and cert-manager are cloud-agnostic Helm charts.
# On Scaleway, the load balancer annotation differs (no azure-pip-name),
# but the chart names, repos, and versions are identical.

resource "helm_release" "ingress_nginx" {
  name             = "ingress-nginx"
  repository       = "https://kubernetes.github.io/ingress-nginx"
  chart            = "ingress-nginx"
  version          = var.ingress_nginx_chart_version
  namespace        = "ingress-nginx"
  create_namespace = true
  wait             = true
  timeout          = 600

  # On Scaleway, the LoadBalancer IP is reserved via scaleway_lb_ip and
  # passed via loadBalancerIP — no cloud-specific annotations required.
  set {
    name  = "controller.service.loadBalancerIP"
    value = var.ingress_public_ip
  }

  # Ensure single replica for small dev clusters; override in prod
  set {
    name  = "controller.replicaCount"
    value = tostring(var.ingress_replica_count)
  }
}

resource "helm_release" "cert_manager" {
  name             = "cert-manager"
  repository       = "https://charts.jetstack.io"
  chart            = "cert-manager"
  version          = var.cert_manager_version
  namespace        = "cert-manager"
  create_namespace = true
  wait             = true
  timeout          = 600

  set {
    name  = "crds.enabled"
    value = "true"
  }
}
