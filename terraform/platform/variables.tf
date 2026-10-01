# Every one of these (except project_name) is a pass-through of a provider
# module's contract output -- see terraform/providers/CONTRACT.md. This
# module never imports a provider module directly; scripts/03-apply-provider.sh
# captures `tofu output -json` from whichever provider you applied into
# terraform/generated/<provider>.tfvars.json, and scripts/04-apply-platform.sh
# feeds it in here via -var-file, which is what actually keeps this
# directory provider-agnostic in practice, not just in principle.

variable "kubeconfig_path" {
  type = string
}

variable "storage_class_name" {
  type = string
}

variable "workload_identity_mechanism" {
  type = string
  validation {
    condition     = contains(["irsa", "pod-identity", "workload-identity", "azuread-workload-identity", "static-secret"], var.workload_identity_mechanism)
    error_message = "Must be one of: irsa, pod-identity, workload-identity, azuread-workload-identity, static-secret -- see terraform/providers/CONTRACT.md."
  }
}

# NOT a CONTRACT.md variable -- Nessie is platform-wide, not tenant-scoped,
# so it doesn't fit CONTRACT.md's per-tenant service_account_annotations
# shape. Passed through from AWS's own
# nessie_service_account_annotations output (terraform/providers/aws/outputs.tf)
# when it exists; {} default so a provider that doesn't produce this key at
# all (GCP, Azure, bare metal, or AWS before this output existed) doesn't
# break this module's plan/apply -- same optional-with-fallback treatment
# service_account_annotations itself gets in terraform/tenants/_template.
variable "nessie_service_account_annotations" {
  type    = map(string)
  default = {}
}

# --- Object storage (CONTRACT.md's object-storage outputs) ---
# What lets Nessie boot without any tenant existing -- see catalog.tf.

variable "object_storage_endpoint" {
  type = string
}

variable "object_storage_bucket" {
  type = string
}

variable "object_storage_access_key_id" {
  type      = string
  sensitive = true
}

variable "object_storage_secret_access_key" {
  type      = string
  sensitive = true
}

variable "project_name" {
  type    = string
  default = "lakehouse"
}

variable "platform_namespace" {
  type    = string
  default = "platform"
}

variable "observability_namespace" {
  type    = string
  default = "observability"
}

# Both default to true (nothing changes for a normal/mini-PC-sized apply).
# Set either to false in a local terraform/platform/terraform.tfvars
# (gitignored, same convention as terraform/providers/baremetal's) to trim
# footprint on a smaller box -- not a hack to remember to revert later,
# just don't carry that override to bigger hardware. Neither is a real
# dependency of the core Kestra -> Trino -> Iceberg/Nessie pipeline the
# example flow exercises: observability is metrics/logs/traces, and the
# Shared OLTP Service only matters once you have genuinely cross-tenant
# Postgres-resident data to protect with RLS (see shared-oltp.tf). The
# tenant-pool Postgres (tenant-pool-postgres.tf) has no such toggle --
# a pool-tier tenant's schema-creation Job (terraform/tenants/_template/
# postgres.tf) depends on it directly, so it's not optional while any
# pool-tier tenant is applied.
variable "enable_observability" {
  description = "kube-prometheus-stack + Loki + Tempo. ~1.75Gi requests / ~3.5Gi limits when on."
  type        = bool
  default     = true
}

variable "enable_shared_oltp" {
  description = "The Shared OLTP Service (shared-oltp.tf). ~512Mi requests / ~1Gi limits when on."
  type        = bool
  default     = true
}

# --- GHCR credentials for the private kestra-lakehouse image ---
# kestra-lakehouse (kestra-plugins/plugin-dbt-k8s-runner/docker/Dockerfile)
# started as a purely local Docker tag, manually loaded into whichever
# cluster needed it with provider-specific tooling (`k3s ctr images
# import` on bare metal, `kind load docker-image` on the emulator
# doubles -- see that plugin's README, step 3/3b). Pushed to GHCR instead
# so every provider pulls the identical image over the network, no manual
# per-cluster load step. Private for now -- see docs/ROADMAP.md's "Image
# distribution" section for what gates flipping it to public.

variable "ghcr_username" {
  description = "GitHub username/org kestra-lakehouse was pushed under (ghcr.io/<this>/kestra-lakehouse). Not secret by itself -- GitHub usernames aren't -- but still environment-specific, so it belongs in a local terraform/platform/terraform.tfvars (gitignored, same convention as terraform/providers/baremetal's), not committed here."
  type        = string
}

variable "ghcr_token" {
  description = "GitHub PAT with read:packages scope -- used to build the imagePullSecret Kubernetes needs to pull the private kestra-lakehouse image. Set in terraform/platform/terraform.tfvars, never committed."
  type        = string
  sensitive   = true
}

