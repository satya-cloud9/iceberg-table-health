# Second tenant instantiation -- copied from terraform/tenants/tenant-a's
# main.tf per that file's own header comment ("A second tenant is another
# file like this one -- copy it, change tenant_id, done") and
# scripts/05-apply-tenant.sh's header comment. NOT a copy of
# terraform/tenants/_template itself, and nothing here should ever diverge
# from tenant-a's shape except tenant_id and the module's own local name --
# any other difference is either a bug in one of the two, or something
# that belongs in _template as a real per-tenant variable instead.
#
# Existing today specifically so docs/testing-platform-tenant.md has a
# second, independent tenant to test cross-tenant object-storage isolation
# against -- not because tenant-b has any other product reason to exist
# yet.

module "tenant_b" {
  source = "../_template"

  tenant_id                         = "tenant-b"
  isolation_tier                    = var.isolation_tier
  storage_class_name                = var.storage_class_name
  workload_identity_mechanism       = var.workload_identity_mechanism
  catalog_uri                       = var.catalog_uri
  object_storage_endpoint           = var.object_storage_endpoint
  object_storage_bucket             = var.object_storage_bucket
  object_storage_catalog_properties = var.object_storage_catalog_properties
  tenant_pool_postgres_host         = var.tenant_pool_postgres_host
  tenant_pool_postgres_admin_secret = var.tenant_pool_postgres_admin_secret
  platform_namespace                = var.platform_namespace
  observability_namespace           = var.observability_namespace

  # See tenant-a/main.tf's identical comment -- dbt-execution.tf
  # provisions tenant-b-dbt-exec the same declarative way, scoped to the
  # real Kestra worker identity, no manual RBAC step.
  kestra_service_account_name = var.kestra_service_account_name
}

