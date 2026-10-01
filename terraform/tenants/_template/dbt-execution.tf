# This tenant's dedicated dbt/Kubernetes-task-runner execution surface --
# see variables.tf's enable_dbt_execution_namespace doc comment for the
# full rationale, and the lakehouse-series article's "who owns the
# pipeline" paragraph for the design principle this exists to uphold:
# coordination (Kestra, in the platform namespace) stays centralized,
# execution doesn't. kestra-plugins/plugin-dbt-k8s-runner's
# KubernetesTaskRunner is the mechanism that makes that structurally
# true -- it creates a fresh, disposable Pod per task run, here, rather
# than running the task inside Kestra's own long-lived process.
#
# This namespace is deliberately separate from kubernetes_namespace_v1.tenant
# (namespace.tf) -- see that file's NetworkPolicy comment. The short version:
# the platform's worker ServiceAccount gets Pod-scoped RBAC ONLY here, never
# inside the tenant's own namespace where Trino/MinIO/Postgres actually run,
# so nothing about running a dbt task ever grants Kubernetes API-level power
# over the tenant's real infrastructure.

resource "kubernetes_namespace_v1" "dbt_exec" {
  count = var.enable_dbt_execution_namespace ? 1 : 0

  metadata {
    name = "${var.tenant_id}-dbt-exec"
    labels = {
      "lakehouse.io/tenant"  = var.tenant_id
      "lakehouse.io/purpose" = "dbt-task-execution"
    }
  }
}

# Sized for transient task Pods (KubernetesTaskRunner's own PodResources
# defaults in examples/02-dbt-run.yaml: 250m/1 cpu, 512Mi/1Gi memory per
# Pod), not for the tenant's own always-on services -- deliberately its
# own, smaller quota so a burst of concurrent pipeline runs can't compete
# unbounded with anything else in the cluster, and can't be sized based on
# what namespace.tf's tenant quota happens to be.
resource "kubernetes_resource_quota_v1" "dbt_exec" {
  count = var.enable_dbt_execution_namespace ? 1 : 0

  metadata {
    name      = "${var.tenant_id}-dbt-exec-quota"
    namespace = kubernetes_namespace_v1.dbt_exec[0].metadata[0].name
  }
  spec {
    hard = {
      "requests.cpu"    = "2"
      "requests.memory" = "4Gi"
      "limits.cpu"      = "4"
      "limits.memory"   = "8Gi"
    }
  }
}

resource "kubernetes_limit_range_v1" "dbt_exec" {
  count = var.enable_dbt_execution_namespace ? 1 : 0

  metadata {
    name      = "${var.tenant_id}-dbt-exec-default-limits"
    namespace = kubernetes_namespace_v1.dbt_exec[0].metadata[0].name
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

# The whole tenant-isolation property of the dbt Kubernetes task runner
# lives here, in RBAC, not in the plugin's Java code (see
# KubernetesTaskRunner.java's own class-level Javadoc). Cross-namespace
# subject reference: the Role+RoleBinding are created IN this tenant's
# execution namespace, but the RoleBinding's subject is a ServiceAccount
# that lives in the platform namespace -- that's what lets the platform
# worker act inside this one namespace without ever touching this
# tenant's real resources, and without needing anything cluster-wide.
resource "kubernetes_role_v1" "dbt_exec_runner" {
  count = var.enable_dbt_execution_namespace ? 1 : 0

  metadata {
    name      = "kestra-dbt-k8s-runner"
    namespace = kubernetes_namespace_v1.dbt_exec[0].metadata[0].name
  }
  rule {
    api_groups = [""]
    resources  = ["pods"]
    verbs      = ["create", "get", "list", "watch", "delete"]
  }
  rule {
    api_groups = [""]
    resources  = ["pods/log"]
    verbs      = ["get", "watch"]
  }
}

resource "kubernetes_role_binding_v1" "dbt_exec_runner" {
  count = var.enable_dbt_execution_namespace ? 1 : 0

  metadata {
    name      = "kestra-dbt-k8s-runner"
    namespace = kubernetes_namespace_v1.dbt_exec[0].metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = var.kestra_service_account_name
    namespace = var.platform_namespace
  }
  role_ref {
    kind      = "Role"
    name      = kubernetes_role_v1.dbt_exec_runner[0].metadata[0].name
    api_group = "rbac.authorization.k8s.io"
  }
}
