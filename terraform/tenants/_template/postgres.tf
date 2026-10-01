# Pool tier: a schema inside the shared tenant-pool-postgres instance
# (terraform/platform/tenant-pool-postgres.tf) -- private to this tenant,
# but sharing compute with every other pool-tier tenant. Node-group/
# dedicated tier: this tenant's own Postgres instance, inside its own
# namespace. Never both -- see the tenancy-architecture reference page's
# "Isolation tiers" section for what else changes between these.

data "kubernetes_secret_v1" "pool_postgres_admin" {
  count = var.isolation_tier == "pool" ? 1 : 0
  metadata {
    name      = var.tenant_pool_postgres_admin_secret
    namespace = var.platform_namespace
  }
}

resource "kubernetes_job_v1" "create_pool_schema" {
  count = var.isolation_tier == "pool" ? 1 : 0
  metadata {
    name      = "${var.tenant_id}-create-pool-schema"
    namespace = kubernetes_namespace_v1.tenant.metadata[0].name
  }
  spec {
    template {
      metadata {
        labels = { app = "${var.tenant_id}-create-pool-schema" }
      }
      spec {
        restart_policy = "OnFailure"
        container {
          name  = "psql"
          image = "postgres:16-alpine"
          command = ["sh", "-c", <<-EOT
            psql "postgresql://$${PGUSER}:$${PGPASSWORD}@$${PGHOSTPORT}/postgres" \
              -c "CREATE SCHEMA IF NOT EXISTS tenant_${replace(var.tenant_id, "-", "_")};"
          EOT
          ]
          env {
            name  = "PGHOSTPORT"
            value = var.tenant_pool_postgres_host
          }
          env {
            name  = "PGUSER"
            value = data.kubernetes_secret_v1.pool_postgres_admin[0].data["POSTGRES_USER"]
          }
          env {
            name  = "PGPASSWORD"
            value = data.kubernetes_secret_v1.pool_postgres_admin[0].data["POSTGRES_PASSWORD"]
          }
        }
      }
    }
    backoff_limit = 3
  }
}

# --- node_group / dedicated tier: this tenant's own Postgres instance ---

resource "kubernetes_secret_v1" "tenant_postgres" {
  count = var.isolation_tier != "pool" ? 1 : 0
  metadata {
    name      = "${var.tenant_id}-postgres"
    namespace = kubernetes_namespace_v1.tenant.metadata[0].name
  }
  data = {
    POSTGRES_USER     = replace(var.tenant_id, "-", "_")
    POSTGRES_PASSWORD = "changeme" # local-only demo credential -- do not reuse anywhere real
    POSTGRES_DB       = replace(var.tenant_id, "-", "_")
  }
}

resource "kubernetes_persistent_volume_claim_v1" "tenant_postgres" {
  count = var.isolation_tier != "pool" ? 1 : 0
  metadata {
    name      = "${var.tenant_id}-postgres-data"
    namespace = kubernetes_namespace_v1.tenant.metadata[0].name
  }
  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = var.storage_class_name
    resources {
      requests = { storage = "10Gi" }
    }
  }
}

resource "kubernetes_deployment_v1" "tenant_postgres" {
  count = var.isolation_tier != "pool" ? 1 : 0
  metadata {
    name      = "${var.tenant_id}-postgres"
    namespace = kubernetes_namespace_v1.tenant.metadata[0].name
  }
  spec {
    replicas = 1
    selector {
      match_labels = { app = "${var.tenant_id}-postgres" }
    }
    template {
      metadata {
        labels = { app = "${var.tenant_id}-postgres" }
      }
      spec {
        container {
          name  = "postgres"
          image = "postgres:16-alpine"
          env_from {
            secret_ref { name = kubernetes_secret_v1.tenant_postgres[0].metadata[0].name }
          }
          port { container_port = 5432 }
          resources {
            requests = { cpu = "500m", memory = "1Gi" }
            limits   = { cpu = "1", memory = "2Gi" }
          }
          volume_mount {
            name       = "data"
            mount_path = "/var/lib/postgresql/data"
            sub_path   = "pgdata"
          }
        }
        volume {
          name = "data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.tenant_postgres[0].metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "tenant_postgres" {
  count = var.isolation_tier != "pool" ? 1 : 0
  metadata {
    name      = "${var.tenant_id}-postgres"
    namespace = kubernetes_namespace_v1.tenant.metadata[0].name
  }
  spec {
    selector = { app = "${var.tenant_id}-postgres" }
    port {
      port        = 5432
      target_port = 5432
    }
  }
}
