output "platform_namespace" {
  value = kubernetes_namespace_v1.platform.metadata[0].name
}

output "observability_namespace" {
  value = kubernetes_namespace_v1.observability.metadata[0].name
}

output "catalog_uri" {
  description = "Nessie's Iceberg REST catalog endpoint -- what every tenant's Trino points its iceberg.rest-catalog.uri at."
  value       = "http://nessie.${var.platform_namespace}.svc.cluster.local:19120/iceberg"
}

output "shared_oltp_jdbc_url" {
  description = "The Shared OLTP Service's JDBC URL -- only for tables governed by Row-Level Security, see shared-oltp.tf. Never a tenant's private schema."
  value       = "jdbc:postgresql://shared-oltp.${var.platform_namespace}.svc.cluster.local:5432/shared_oltp"
}

output "tenant_pool_postgres_host" {
  description = "Host:port for the shared pool-tier Postgres instance -- consumed only by pool-tier tenants (see terraform/tenants/_template/postgres.tf), which get a schema here rather than a dedicated instance."
  value       = "tenant-pool-postgres.${var.platform_namespace}.svc.cluster.local:5432"
}

output "tenant_pool_postgres_admin_secret" {
  description = "Name of the Secret (in the platform namespace) holding admin credentials for the shared pool-tier Postgres instance."
  value       = kubernetes_secret_v1.tenant_pool_postgres.metadata[0].name
}

output "kestra_service_account_name" {
  description = "The real ServiceAccount name Kestra's own worker runs as, read back from the cluster (see kestra.tf's data source) rather than assumed from the chart's default naming convention. Consumed by terraform/tenants/_template/dbt-execution.tf's RoleBinding -- every tenant's dbt execution namespace grants exactly this identity, and only this identity, Pod-scoped access."
  value       = data.kubernetes_service_account_v1.kestra.metadata[0].name
}

