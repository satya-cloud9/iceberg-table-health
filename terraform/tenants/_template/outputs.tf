output "namespace" {
  value = kubernetes_namespace_v1.tenant.metadata[0].name
}

output "trino_service" {
  value = "trino.${kubernetes_namespace_v1.tenant.metadata[0].name}.svc.cluster.local"
}

# No minio_service output anymore -- object storage is shared, one
# instance per environment, owned by the provider layer now (see
# CONTRACT.md's "object-storage outputs" section), not a per-tenant
# in-cluster service.

output "dbt_execution_namespace" {
  description = "This tenant's dbt/Kubernetes-task-runner execution namespace name (dbt-execution.tf) -- null when enable_dbt_execution_namespace is false. Flow authors point KubernetesTaskRunner's own `namespace` field at this value."
  value       = var.enable_dbt_execution_namespace ? kubernetes_namespace_v1.dbt_exec[0].metadata[0].name : null
}
