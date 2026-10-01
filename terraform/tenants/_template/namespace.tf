# The isolation boundary from the tenancy-architecture reference page's
# main diagram -- a namespace, a ResourceQuota, and a default-deny
# NetworkPolicy with explicit holes only for the platform and
# observability namespaces (the cross-boundary mechanisms: gRPC to the
# Kestra Controller, the Nessie catalog, the Shared OLTP Service, and
# Prometheus scraping in the other direction).

resource "kubernetes_namespace_v1" "tenant" {
  metadata {
    name = var.tenant_id
    labels = {
      "lakehouse.io/tenant"          = var.tenant_id
      "lakehouse.io/isolation-tier"  = var.isolation_tier
    }
  }
}

resource "kubernetes_resource_quota_v1" "tenant" {
  metadata {
    name      = "${var.tenant_id}-quota"
    namespace = kubernetes_namespace_v1.tenant.metadata[0].name
  }
  spec {
    hard = {
      "requests.cpu"    = var.isolation_tier == "pool" ? "4" : "8"
      "requests.memory" = var.isolation_tier == "pool" ? "8Gi" : "16Gi"
      "limits.cpu"      = var.isolation_tier == "pool" ? "8" : "16"
      "limits.memory"   = var.isolation_tier == "pool" ? "16Gi" : "32Gi"
    }
  }
}

# A ResourceQuota tracking requests/limits on cpu+memory means every
# container created in this namespace MUST declare all four fields itself
# -- Kubernetes enforces this at admission time, no exceptions. Nothing
# here supplied that for create_pool_schema's psql container (postgres.tf),
# which was rejected outright ("failed quota: must specify limits.cpu ...").
# A LimitRange is the standard pairing with a ResourceQuota -- it supplies
# default request/limit values to any container that doesn't set its own,
# fixing that without touching postgres.tf's own Job spec, and covering
# anything else added to this namespace later. (Originally also needed for
# MinIO's own chart-provided hook jobs, before object storage moved to the
# provider layer -- see CONTRACT.md's "object-storage outputs" section.)
resource "kubernetes_limit_range_v1" "tenant" {
  metadata {
    name      = "${var.tenant_id}-default-limits"
    namespace = kubernetes_namespace_v1.tenant.metadata[0].name
  }
  spec {
    limit {
      type = "Container"
      default = {
        cpu    = "500m"
        memory = "512Mi"
      }
      default_request = {
        cpu    = "100m"
        memory = "128Mi"
      }
    }
  }
}

# Ingress: default-deny, with explicit allows for same-namespace traffic,
# the platform/observability namespaces, this tenant's own dbt execution
# namespace (dbt-execution.tf, when enable_dbt_execution_namespace is
# true -- structural and automatic, not something a tenant's main.tf has
# to remember to grant), and whatever else a tenant's own main.tf opts
# into via additional_allowed_namespaces (see variables.tf -- the
# general-purpose mechanism for a genuinely one-off, audited grant that
# isn't already covered by a first-class variable of its own).
#
# Egress is intentionally left open here -- pinning it down to exactly
# DNS + the platform namespace + the shared object-storage endpoint
# (CONTRACT.md's object_storage_endpoint, now a provider-layer address
# rather than another namespace in this same cluster) is the more correct
# end state, but needs verifying against what Trino/Kestra actually call
# at runtime before locking it down without breaking them.
resource "kubernetes_network_policy_v1" "tenant_isolation" {
  metadata {
    name      = "${var.tenant_id}-isolation-boundary"
    namespace = kubernetes_namespace_v1.tenant.metadata[0].name
  }
  spec {
    pod_selector {}
    policy_types = ["Ingress"]

    ingress {
      from {
        namespace_selector {
          match_labels = {
            "kubernetes.io/metadata.name" = kubernetes_namespace_v1.tenant.metadata[0].name
          }
        }
      }
    }
    ingress {
      from {
        namespace_selector {
          match_labels = {
            "kubernetes.io/metadata.name" = var.platform_namespace
          }
        }
      }
    }
    ingress {
      from {
        namespace_selector {
          match_labels = {
            "kubernetes.io/metadata.name" = var.observability_namespace
          }
        }
      }
    }

    dynamic "ingress" {
      for_each = var.enable_dbt_execution_namespace ? [kubernetes_namespace_v1.dbt_exec[0].metadata[0].name] : []
      content {
        from {
          namespace_selector {
            match_labels = {
              "kubernetes.io/metadata.name" = ingress.value
            }
          }
        }
      }
    }

    dynamic "ingress" {
      for_each = var.additional_allowed_namespaces
      content {
        from {
          namespace_selector {
            match_labels = {
              "kubernetes.io/metadata.name" = ingress.value
            }
          }
        }
      }
    }
  }
}
