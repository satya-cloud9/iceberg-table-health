# "Grafana (tenant folder)" from the tenancy-architecture reference page --
# one shared Grafana (deployed by terraform/platform/observability.tf), a
# folder per tenant via this labeled ConfigMap, which kube-prometheus-stack's
# sidecar (searchNamespace: ALL, see helm-values/kube-prometheus-stack-values.yaml)
# picks up from this tenant's own namespace. No per-tenant Grafana instance.

resource "kubernetes_config_map_v1" "tenant_dashboard" {
  metadata {
    name      = "${var.tenant_id}-overview-dashboard"
    namespace = kubernetes_namespace_v1.tenant.metadata[0].name
    labels = {
      grafana_dashboard = "1"
    }
    annotations = {
      "lakehouse.io/tenant" = var.tenant_id
    }
  }

  data = {
    "${var.tenant_id}-overview.json" = jsonencode({
      title = "${var.tenant_id} overview"
      tags  = ["lakehouse", var.tenant_id]
      panels = [
        {
          id    = 1
          title = "Pod CPU usage"
          type  = "timeseries"
          gridPos = { x = 0, y = 0, w = 12, h = 8 }
          targets = [
            {
              expr = "sum(rate(container_cpu_usage_seconds_total{namespace=\"${var.tenant_id}\"}[5m])) by (pod)"
            }
          ]
        },
        {
          id    = 2
          title = "Pod memory usage"
          type  = "timeseries"
          gridPos = { x = 12, y = 0, w = 12, h = 8 }
          targets = [
            {
              expr = "sum(container_memory_working_set_bytes{namespace=\"${var.tenant_id}\"}) by (pod)"
            }
          ]
        }
      ]
      schemaVersion = 39
    })
  }
}
