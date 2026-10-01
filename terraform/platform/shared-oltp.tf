# The Shared OLTP Service from the tenancy-architecture reference page's
# "Sharing Postgres-resident data" section -- one Postgres instance,
# outside every tenant's isolation boundary, for the narrow slice of
# row-level data that's genuinely cross-tenant (a marketplace order, a
# shared workflow state), governed by Row-Level Security rather than by
# namespace/network isolation. NOT a home for any tenant's private tables
# -- those stay in that tenant's own Postgres, inside its workspace (see
# terraform/tenants).
#
# The RLS policies themselves are intentionally not applied by Terraform:
# they're per-table application logic, not infrastructure, and belong to
# whichever tenant/table first needs one. bootstrap.sql below is a worked
# example, not something this module runs automatically -- apply it with
# `kubectl exec -n platform deploy/shared-oltp -- psql -U shared_oltp -d
# shared_oltp -f -` once you have a real table to protect.

resource "kubernetes_secret_v1" "shared_oltp" {
  count = var.enable_shared_oltp ? 1 : 0
  metadata {
    name      = "shared-oltp"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  data = {
    POSTGRES_USER     = "shared_oltp"
    POSTGRES_PASSWORD = "shared-oltp" # local-only demo credential -- do not reuse anywhere real
    POSTGRES_DB       = "shared_oltp"
  }
}

resource "kubernetes_config_map_v1" "shared_oltp_bootstrap" {
  count = var.enable_shared_oltp ? 1 : 0
  metadata {
    name      = "shared-oltp-rls-bootstrap"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  data = {
    "bootstrap.sql" = <<-SQL
      -- Worked example: a table two tenants both need to see rows in,
      -- e.g. a marketplace order that touches a buyer tenant and a
      -- seller tenant. Every row carries the owning tenant plus who
      -- else it's been shared with; the policy expresses both the
      -- default-isolated case and the explicit-share case in one rule.
      CREATE TABLE IF NOT EXISTS shared_orders (
        id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        tenant_id     text NOT NULL,
        shared_with   text[] NOT NULL DEFAULT '{}',
        payload       jsonb NOT NULL,
        created_at    timestamptz NOT NULL DEFAULT now()
      );

      ALTER TABLE shared_orders ENABLE ROW LEVEL SECURITY;

      -- current_setting('app.tenant_id') is set per-connection by whatever
      -- issues the query (Trino's JDBC connector, or the app layer) --
      -- e.g. `SET app.tenant_id = 'tenant-a';` right after connecting.
      CREATE POLICY tenant_isolation_and_sharing ON shared_orders
        USING (
          tenant_id = current_setting('app.tenant_id', true)
          OR current_setting('app.tenant_id', true) = ANY (shared_with)
        );
    SQL
  }
}

resource "kubernetes_persistent_volume_claim_v1" "shared_oltp" {
  wait_until_bound = false
  count = var.enable_shared_oltp ? 1 : 0
  metadata {
    name      = "shared-oltp-data"
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

resource "kubernetes_deployment_v1" "shared_oltp" {
  count = var.enable_shared_oltp ? 1 : 0
  metadata {
    name      = "shared-oltp"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  spec {
    replicas = 1
    selector {
      match_labels = { app = "shared-oltp" }
    }
    template {
      metadata {
        labels = { app = "shared-oltp" }
      }
      spec {
        container {
          name  = "postgres"
          image = "postgres:16-alpine"
          env_from {
            secret_ref { name = kubernetes_secret_v1.shared_oltp[0].metadata[0].name }
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
          volume_mount {
            name       = "bootstrap"
            mount_path = "/bootstrap"
          }
        }
        volume {
          name = "data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.shared_oltp[0].metadata[0].name
          }
        }
        volume {
          name = "bootstrap"
          config_map {
            name = kubernetes_config_map_v1.shared_oltp_bootstrap[0].metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "shared_oltp" {
  count = var.enable_shared_oltp ? 1 : 0
  metadata {
    name      = "shared-oltp"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  spec {
    selector = { app = "shared-oltp" }
    port {
      port        = 5432
      target_port = 5432
    }
  }
}
