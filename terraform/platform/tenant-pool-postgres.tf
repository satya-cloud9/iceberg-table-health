# One shared Postgres instance backing every pool-tier tenant's *private*
# Postgres data -- NOT the Shared OLTP Service (shared-oltp.tf), which is
# for genuinely cross-tenant rows. This one just hosts a separate schema
# per pool-tier tenant, each accessed only by that tenant (see
# terraform/tenants/_template/postgres.tf, which creates the schema).
# node-group/dedicated-tier tenants skip this entirely and get their own
# Postgres instance inside their own namespace instead.

resource "kubernetes_secret_v1" "tenant_pool_postgres" {
  metadata {
    name      = "tenant-pool-postgres"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  data = {
    POSTGRES_USER     = "pool_admin"
    POSTGRES_PASSWORD = "pool-admin" # local-only demo credential -- do not reuse anywhere real
    POSTGRES_DB       = "postgres"
  }
}

resource "kubernetes_persistent_volume_claim_v1" "tenant_pool_postgres" {
  wait_until_bound = false
  metadata {
    name      = "tenant-pool-postgres-data"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = var.storage_class_name
    resources {
      requests = { storage = "10Gi" }
    }
  }
}

resource "kubernetes_deployment_v1" "tenant_pool_postgres" {
  metadata {
    name      = "tenant-pool-postgres"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  spec {
    replicas = 1
    selector {
      match_labels = { app = "tenant-pool-postgres" }
    }
    template {
      metadata {
        labels = { app = "tenant-pool-postgres" }
      }
      spec {
        container {
          name  = "postgres"
          image = "postgres:16-alpine"
          env_from {
            secret_ref { name = kubernetes_secret_v1.tenant_pool_postgres.metadata[0].name }
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
            claim_name = kubernetes_persistent_volume_claim_v1.tenant_pool_postgres.metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "tenant_pool_postgres" {
  metadata {
    name      = "tenant-pool-postgres"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  spec {
    selector = { app = "tenant-pool-postgres" }
    port {
      port        = 5432
      target_port = 5432
    }
  }
}
