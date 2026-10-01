# Kestra, standalone for now -- see the tenancy-architecture reference
# page's "Reading the diagram" section: the Controller/Worker-Group split
# (Kestra 2.0) exists to let workers live in a different cluster/region
# from the scheduler. On a single bare-metal box that's solving a problem
# you don't have yet, so this stays standalone (server + worker together)
# until the day a second provider/cluster is actually added -- at which
# point this becomes a Controller here plus a Worker Group per cluster,
# a deployment-topology change, not a flow-authoring one.

resource "kubernetes_secret_v1" "kestra_postgres" {
  metadata {
    name      = "kestra-postgres"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  data = {
    POSTGRES_USER     = "kestra"
    POSTGRES_PASSWORD = "kestra" # local-only demo credential -- do not reuse anywhere real
    POSTGRES_DB       = "kestra"
  }
}

resource "kubernetes_persistent_volume_claim_v1" "kestra_postgres" {
  wait_until_bound = false

  metadata {
    name      = "kestra-postgres-data"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = var.storage_class_name
    resources {
      requests = { storage = "5Gi" }
    }
  }
}

resource "kubernetes_deployment_v1" "kestra_postgres" {
  metadata {
    name      = "kestra-postgres"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  spec {
    replicas = 1
    selector {
      match_labels = { app = "kestra-postgres" }
    }
    template {
      metadata {
        labels = { app = "kestra-postgres" }
      }
      spec {
        container {
          name  = "postgres"
          image = "postgres:16-alpine"
          env_from {
            secret_ref { name = kubernetes_secret_v1.kestra_postgres.metadata[0].name }
          }
          port { container_port = 5432 }
          resources {
            requests = { cpu = "250m", memory = "512Mi" }
            limits   = { cpu = "500m", memory = "1Gi" }
          }
          volume_mount {
            name       = "data"
            mount_path = "/var/lib/postgresql/data"
            sub_path   = "pgdata"
          }
          readiness_probe {
            exec {
              command = ["pg_isready", "-U", "kestra"]
            }
            initial_delay_seconds = 5
            period_seconds         = 5
          }
        }
        volume {
          name = "data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.kestra_postgres.metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "kestra_postgres" {
  metadata {
    name      = "kestra-postgres"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  spec {
    selector = { app = "kestra-postgres" }
    port {
      port        = 5432
      target_port = 5432
    }
  }
}
# GHCR pull credentials for the private kestra-lakehouse image -- see
# variables.tf's ghcr_username/ghcr_token doc comments and
# docs/ROADMAP.md's "Image distribution" section. Confirmed against
# `helm show values kestra/kestra --version 1.3.37`, which lists
# `imagePullSecrets: []` at the values root (a plain list of
# `{name: ...}` entries, the standard Kubernetes PodSpec shape) -- this
# Secret's name ("ghcr-pull-secret") is what
# helm-values/kestra-values.yaml's imagePullSecrets entry references.
# type = kubernetes.io/dockerconfigjson is the Kubernetes-standard shape
# for a registry credential; `data`'s value here is plain JSON on
# purpose -- the kubernetes provider base64-encodes every `data` entry
# itself before sending it to the API, so pre-encoding it here would
# double-encode it.
resource "kubernetes_secret_v1" "ghcr_pull" {
  metadata {
    name      = "ghcr-pull-secret"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  type = "kubernetes.io/dockerconfigjson"
  data = {
    ".dockerconfigjson" = jsonencode({
      auths = {
        "ghcr.io" = {
          username = var.ghcr_username
          password = var.ghcr_token
          auth     = base64encode("${var.ghcr_username}:${var.ghcr_token}")
        }
      }
    })
  }
}
resource "helm_release" "kestra" {
  name       = "kestra"
  repository = "https://helm.kestra.io"
  chart      = "kestra"
  namespace  = kubernetes_namespace_v1.platform.metadata[0].name
  version    = "1.3.37"

  values = [file("${path.module}/../../helm-values/kestra-values.yaml")]

  # Overrides kestra-values.yaml's local-only fallback repository with
  # the real GHCR path -- keeping the username out of the committed
  # values file, since it's environment-specific even if not sensitive.
  set {
    name  = "image.repository"
    value = "ghcr.io/${var.ghcr_username}/kestra-lakehouse"
  }

  depends_on = [
    kubernetes_deployment_v1.kestra_postgres,
    kubernetes_service_v1.kestra_postgres,
    kubernetes_secret_v1.ghcr_pull,
  ]
}

# The tenant layer's per-tenant dbt execution namespace (see
# terraform/tenants/_template/dbt-execution.tf) needs to grant this exact
# ServiceAccount -- the one Kestra's own worker actually runs as -- a
# RoleBinding in each tenant it's allowed to execute pipelines for. Rather
# than hardcoding "kestra" as a literal string in the tenant module (which
# is exactly the kind of unverified, chart-default-dependent assumption
# that already broke once this session, when an unpinned chart version
# silently changed how the whole release was structured), this data
# source reads the ServiceAccount the chart actually created and exports
# its real name as a platform output. If a future chart version changes
# the fullname convention, this fails loudly at `tofu plan`/`apply` time
# (resource not found) instead of silently producing a stale value.
data "kubernetes_service_account_v1" "kestra" {
  metadata {
    name      = "kestra"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }

  depends_on = [helm_release.kestra]
}
