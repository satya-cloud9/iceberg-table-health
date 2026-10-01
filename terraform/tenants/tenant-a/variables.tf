# Same pass-through shape as terraform/platform/variables.tf -- fed by
# scripts/05-apply-tenant.sh's two -var-file flags, built from
# `tofu output -json` of whichever provider module and then
# terraform/platform were applied first (scripts/03/04-apply-*.sh).

variable "kubeconfig_path" {
  type = string
}

variable "storage_class_name" {
  type = string
}

variable "workload_identity_mechanism" {
  type = string
}

variable "catalog_uri" {
  type = string
}

variable "object_storage_endpoint" {
  type = string
}

variable "object_storage_catalog_properties" {
  description = "Provider layer's sixth contract output -- see terraform/providers/CONTRACT.md and terraform/tenants/_template/variables.tf's equivalent declaration. Replaces the object_storage_access_key_id/secret_access_key pass-through this file used to declare directly."
  type        = string
  sensitive   = true
}

variable "object_storage_bucket" {
  type    = string
  default = ""
}

variable "tenant_pool_postgres_host" {
  type    = string
  default = ""
}

variable "tenant_pool_postgres_admin_secret" {
  type    = string
  default = ""
}

variable "platform_namespace" {
  type    = string
  default = "platform"
}

variable "observability_namespace" {
  type    = string
  default = "observability"
}

variable "isolation_tier" {
  description = "Only \"pool\" is meaningful on a single-node cluster -- see terraform/tenants/_template/variables.tf."
  type        = string
  default     = "pool"
}

variable "kestra_service_account_name" {
  description = "From terraform/platform's kestra_service_account_name output -- passed through to the _template module's dbt-execution.tf RoleBinding."
  type        = string
}

