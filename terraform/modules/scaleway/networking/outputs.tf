output "id" {
  value = scaleway_lb_ip.ingress.id
}

output "ip_address" {
  value = scaleway_lb_ip.ingress.ip_address
}

# Scaleway doesn't use a named PIP concept; expose a static name for
# downstream script compatibility.
output "name" {
  value = "scw-ingress-lb-ip"
}
