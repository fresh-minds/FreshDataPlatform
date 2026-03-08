# Scaleway Load Balancer IP
# Equivalent to: modules/azure/networking (static public IP for ingress)
#
# On Scaleway, a Flexible IP (lb_ip) is reserved and later attached to the
# Load Balancer that Kapsule provisions for LoadBalancer-type Services.

resource "scaleway_lb_ip" "ingress" {
  # region is determined by the provider configuration (set at root via scw_region)
  project_id = var.project_id
}
