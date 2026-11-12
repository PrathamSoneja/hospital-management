resource "helm_release" "argocd" {
  name             = "argocd"
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-cd"
  version          = var.argocd_chart_version
  namespace        = "argocd"
  create_namespace = true
  atomic           = true
  cleanup_on_fail  = true
  timeout          = 600

  values = [
    yamlencode({
      dex = {
        enabled = false
      }
      "redis-ha" = {
        enabled = false
      }
      controller = {
        replicas = 1
      }
      repoServer = {
        replicas = 1
      }
      applicationSet = {
        enabled = true
      }
      server = {
        service = {
          type = "ClusterIP"
        }
      }
      configs = {
        params = {
          "server.insecure" = false
        }
      }
    })
  ]
}
