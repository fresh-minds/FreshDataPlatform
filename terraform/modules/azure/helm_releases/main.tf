resource "helm_release" "ingress_nginx" {
  name             = "ingress-nginx"
  repository       = "https://kubernetes.github.io/ingress-nginx"
  chart            = "ingress-nginx"
  version          = var.ingress_nginx_chart_version
  namespace        = "ingress-nginx"
  create_namespace = true
  wait             = true
  timeout          = 600

  set {
    name  = "controller.service.annotations.service\\.beta\\.kubernetes\\.io/azure-load-balancer-resource-group"
    value = var.node_resource_group
  }

  set {
    name  = "controller.service.annotations.service\\.beta\\.kubernetes\\.io/azure-pip-name"
    value = var.ingress_pip_name
  }

  set {
    name  = "controller.service.loadBalancerIP"
    value = var.ingress_public_ip
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
