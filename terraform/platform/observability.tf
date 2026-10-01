# Metrics/logs/traces aggregation -- the "Observability Aggregation" box
# in the tenancy-architecture reference page, one per cluster, filtered
# into a per-tenant Grafana folder view rather than duplicated per tenant.

resource "helm_release" "kube_prometheus_stack" {
  count = var.enable_observability ? 1 : 0
  name       = "kube-prometheus-stack"
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-prometheus-stack"
  namespace  = kubernetes_namespace_v1.observability.metadata[0].name

  values = [file("${path.module}/../../helm-values/kube-prometheus-stack-values.yaml")]
}

resource "helm_release" "loki" {
  count = var.enable_observability ? 1 : 0
  name       = "loki"
  repository = "https://grafana.github.io/helm-charts"
  chart      = "loki"
  namespace  = kubernetes_namespace_v1.observability.metadata[0].name

  values = [file("${path.module}/../../helm-values/loki-values.yaml")]
}

resource "helm_release" "tempo" {
  count = var.enable_observability ? 1 : 0
  name       = "tempo"
  repository = "https://grafana.github.io/helm-charts"
  chart      = "tempo"
  namespace  = kubernetes_namespace_v1.observability.metadata[0].name

  values = [file("${path.module}/../../helm-values/tempo-values.yaml")]
}
