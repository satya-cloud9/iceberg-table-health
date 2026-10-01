#
# IMPORTANT: every other resource in this module that lives inside this
# namespace (catalog.tf, kestra.tf, shared-oltp.tf, tenant-pool-postgres.tf)
# must set its own `namespace = kubernetes_namespace_v1.platform.metadata[0].name`
# -- NEVER `namespace = var.platform_namespace` directly. Confirmed live:
# with those resources reading the plain variable instead of this
# resource's own attribute, Terraform's dependency graph has no edge
# between them and this namespace resource (a matching string value isn't
# a dependency), so nothing stops the provider from scheduling
# kubernetes_secret_v1/kubernetes_service_v1/etc. creation before this
# namespace actually exists. Hit exactly this on a from-scratch apply
# against a fresh kind cluster: `Error: namespaces "platform" not found`
# on half a dozen resources at once. Referencing the resource attribute
# instead of the variable gives Terraform the implicit dependency edge it
# needs, at no cost (the value is identical either way).
# Platform-level namespaces only. Tenant namespaces (tenant-<id>) are
# created by terraform/tenants, never here -- keeping that split is what
# makes "tenant provisioning" mean "apply the tenants module again", not
# "edit this file".

resource "kubernetes_namespace_v1" "platform" {
  metadata {
    name = var.platform_namespace
    labels = {
      purpose = "shared-platform-control-plane"
    }
  }
}

resource "kubernetes_namespace_v1" "observability" {
  metadata {
    name = var.observability_namespace
    labels = {
      purpose = "prometheus-grafana-loki-tempo"
    }
  }
}
