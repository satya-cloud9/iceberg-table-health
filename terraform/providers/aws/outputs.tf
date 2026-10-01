# See terraform/providers/CONTRACT.md -- the first four are the required
# contract; everything after is AWS-specific extra.

output "kubeconfig_path" {
  description = "Local path to the EKS cluster's kubeconfig (generated via `aws eks update-kubeconfig` against floci, with two emulator-only patches -- see cluster.tf's null_resource.kubeconfig header comment)."
  # abspath(): see the same note in providers/baremetal/outputs.tf -- this
  # value crosses into terraform/platform/tenants, which apply from a
  # different working directory, so a bare relative path breaks there.
  value      = abspath("${path.module}/generated/kubeconfig")
  depends_on = [null_resource.kubeconfig]
}

output "node_pool_refs" {
  description = "The one managed node group this module creates. Used to be three kind node names (a separate control-plane node among them) -- real EKS, and floci's emulation of it, doesn't expose the control plane as a node at all, so this is now a single real node-group identifier."
  value       = [aws_eks_node_group.workers.node_group_name]
}

output "workload_identity_mechanism" {
  # "static-secret" by default -- the same shared, bucket-wide
  # object_storage_* credential GCP/Azure/bare metal all fall back to
  # (see iam.tf and trino.tf's header comments). Otherwise, whichever real
  # mechanism var.aws_workload_identity_mechanism names -- "irsa" (default
  # as of 2026-09-24) or "pod-identity" -- built in workload-identity.tf.
  # See that file's header comment for why there are two, and which one is
  # actually usable against the current floci build.
  #
  # A "vault-broker" value used to live here too -- a Vault instance in
  # terraform/platform brokering between this provider's one bootstrap
  # AWS identity and every tenant's own pods. Removed from this branch;
  # see terraform/providers/CONTRACT.md's "Known gap" section.
  value = var.enable_native_workload_identity ? var.aws_workload_identity_mechanism : "static-secret"
}

# Option A's provider-agnostic output (see CONTRACT.md's "Native
# federation (Option A)" section). {} unless the "irsa" mechanism is
# active: EKS Pod Identity associations (workload-identity.tf) are made
# through the EKS API directly, keyed on namespace + ServiceAccount name,
# so they need nothing on the pod spec/ServiceAccount itself -- but
# classic IRSA's trust condition is keyed on exactly this annotation
# (eks.amazonaws.com/role-arn), the same shape GCP/Azure's Workload
# Identity designs already use this output for. trino.tf's
# lookup(var.service_account_annotations, var.tenant_id, {}) treats a
# missing key the same as an empty map, so omitting a tenant here (rather
# than {tenant_id: {}}) is equivalent, not a simplification that changes
# behavior.
output "service_account_annotations" {
  description = "{} unless the \"irsa\" mechanism is active (pod-identity and static-secret don't use ServiceAccount annotations at all). Populated with eks.amazonaws.com/role-arn per tenant when it is."
  value = local.irsa_enabled ? {
    for t in var.tenant_ids : t => {
      "eks.amazonaws.com/role-arn" = aws_iam_role.tenant_trino_irsa[t].arn
    }
  } : {}
}

# NOT a CONTRACT.md output -- Nessie is platform-wide, not tenant-scoped,
# so it doesn't fit service_account_annotations' per-tenant map shape (and
# CONTRACT.md's per-tenant output was never meant to cover it). Read
# directly by terraform/platform/catalog.tf's helm_release.nessie, the
# same way workload-identity.tf's Nessie Pod Identity association was
# always built and consumed unilaterally rather than through a
# CONTRACT.md-shaped output. {} unless the "irsa" mechanism is active.
output "nessie_service_account_annotations" {
  description = "eks.amazonaws.com/role-arn for Nessie's ServiceAccount when the \"irsa\" mechanism is active; {} otherwise."
  value = local.irsa_enabled ? {
    "eks.amazonaws.com/role-arn" = aws_iam_role.nessie_irsa[0].arn
  } : {}
}

output "storage_class_name" {
  # UNVERIFIED against floci specifically: floci's EKS emulation runs on
  # real k3s images (per floci's own docs, its version-to-image mapping is
  # literally rancher/k3s:vX.Y.Z-k3s1) -- k3s's own built-in default
  # StorageClass is named "local-path", not "standard". "standard" was
  # correct for the old `kind`-direct setup (kind names its bundled
  # local-path-provisioner StorageClass "standard" specifically, a
  # kind-only choice) but that's no longer what's running underneath this
  # module. Confirm with `kubectl get storageclass` against the real
  # cluster on first apply -- if it's actually still "standard" for some
  # floci-specific reason, this comment is wrong and should be corrected,
  # not the other way around.
  value = "local-path"
}

# --- AWS-specific extras ---

output "vpc_id" {
  value = aws_vpc.main.id
}

output "kms_key_id" {
  value = aws_kms_key.lakehouse.key_id
}

output "parity_bucket" {
  value = aws_s3_bucket.parity.bucket
}

# --- Object storage (CONTRACT.md's object-storage outputs) ---

output "object_storage_endpoint" {
  description = "See variables.tf's aws_emulator_pod_endpoint -- UNVERIFIED, confirm pod-reachability on first apply."
  value       = var.aws_emulator_pod_endpoint
}

output "object_storage_bucket" {
  value = aws_s3_bucket.parity.bucket
}

output "object_storage_access_key_id" {
  value     = aws_iam_access_key.object_storage.id
  sensitive = true
}

output "object_storage_secret_access_key" {
  value     = aws_iam_access_key.object_storage.secret
  sensitive = true
}

# Sixth contract output -- CONTRACT.md's "The storage-protocol fork, and
# the sixth output that keeps it out of _template" section. The
# protocol-specific half of _template/trino.tf's iceberg_catalog_properties
# (everything that says how to reach/authenticate to object storage) --
# NOT the five REST-catalog lines (connector.name, iceberg.catalog.type,
# the URI, the warehouse, the file format), which stay owned by _template
# itself because they're about Trino's relationship to Nessie, not to
# storage or to which provider produced this cluster.
#
# Built here rather than in _template because this is the one piece of
# that block that would otherwise need _template to branch on provider --
# every other provider's copy of this output is the identical S3-interop
# shape today, so nothing changes behaviorally by moving it here. It's
# what makes GCP's eventual native-GCS-connector work (see CONTRACT.md and
# docs/lakehouse-series article 3's closing sections) a one-file change to
# gcp/outputs.tf instead of a _template-level branch, once that work
# actually starts -- it hasn't yet.
#
# The s3.aws-*-key lines are omitted under the exact same condition
# _template/trino.tf's own object_storage_credentials_properties local
# used to check before this output existed (local.native_federation_enabled,
# i.e. "irsa" or "pod-identity" active) -- moved here verbatim, not
# changed. compact() drops the two credential lines entirely rather than
# leaving blank lines in the properties block when they're omitted.
output "object_storage_catalog_properties" {
  description = "Opaque Trino/Iceberg catalog-properties fragment (fs.s3.*/s3.* lines) for whichever storage connector this provider's cluster actually needs. _template/trino.tf splices this in next to its own five REST-catalog lines and never parses it -- see CONTRACT.md."
  # sensitive = true: this string embeds aws_iam_access_key.object_storage's
  # secret (when native federation is off) the same way
  # object_storage_secret_access_key above does -- OpenTofu correctly
  # refuses to let a root-module output carry sensitive data un-flagged
  # (confirmed live: "Output refers to sensitive values" on this exact
  # output before this line was added). Sensitivity propagates from here
  # through var.object_storage_catalog_properties in every downstream
  # module -- see _template/variables.tf's matching sensitive = true.
  sensitive = true
  value = join("\n", compact([
    "fs.s3.enabled=true",
    "s3.region=us-east-1",
    local.native_federation_enabled ? "" : "s3.aws-access-key=${aws_iam_access_key.object_storage.id}",
    local.native_federation_enabled ? "" : "s3.aws-secret-key=${aws_iam_access_key.object_storage.secret}",
    "s3.endpoint=${var.aws_emulator_pod_endpoint}",
    "s3.path-style-access=true",
  ]))
}

# --- eks_admin identity (scripts/03-apply-provider.sh's two-phase bootstrap) ---
#
# Not a CONTRACT.md output -- terraform/platform and terraform/tenants
# never see these. Read directly by scripts/03-apply-provider.sh via
# `tofu output -raw` during its preliminary -target apply pass, then
# written back into this same module as eks_admin.auto.tfvars.json so the
# real apply's provider.aws.eks_admin block (versions.tf) has them. See
# iam.tf's aws_iam_user.eks_admin header comment for why this identity
# exists at all.

output "eks_admin_access_key_id" {
  value     = aws_iam_access_key.eks_admin.id
  sensitive = true
}

output "eks_admin_secret_access_key" {
  value     = aws_iam_access_key.eks_admin.secret
  sensitive = true
}

