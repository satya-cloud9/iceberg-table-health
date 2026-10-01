# The Iceberg catalog config is generated here (not in helm-values/trino-values.yaml)
# because it needs this tenant's own warehouse name interpolated in --
# catalog.type=rest points at Nessie (terraform/platform), with
# iceberg.rest-catalog.warehouse set to this tenant's own name so Nessie
# can enforce per-tenant catalog ACLs (see the tenancy-architecture
# reference page's "Cross-tenant data sharing" section).
#
# Object storage connector properties (fs.s3.enabled, s3.region,
# s3.aws-*-key, s3.endpoint, s3.path-style-access): NOT built here anymore
# -- var.object_storage_catalog_properties, spliced verbatim into
# local.iceberg_catalog_properties below. That string is the provider
# layer's sixth contract output (CONTRACT.md's "The storage-protocol
# fork, and the sixth output that keeps it out of _template" section):
# each provider module builds it entirely out of its own knowledge of its
# own storage protocol, endpoint, and credential mechanism, and this file
# never parses or branches on what's inside it. Today every provider's
# copy is the identical S3-interop shape this file used to hardcode
# directly (the static, shared object_storage_access_key_id/secret_access_key
# credential every tenant uses on every provider EXCEPT AWS once Option A
# is on there) -- moving ownership changes nothing about current behavior,
# it's what lets a future provider's copy diverge (a native GCS connector,
# say) without this file ever needing an `if provider == "gcp"`.
#
# AWS's real per-tenant mechanism, when var.workload_identity_mechanism ==
# "pod-identity" or "irsa" (terraform/providers/aws/workload-identity.tf,
# gated behind that provider module's var.enable_native_workload_identity):
# either EKS Pod Identity or classic IRSA -- see that file's header comment
# for why there are two and which one is actually usable against the
# current floci build ("irsa" is the default as of 2026-09-24; Pod Identity
# is confirmed blocked by a floci-side bug and kept gated off).
#
# Pod Identity associations are made through the EKS API directly
# (namespace + ServiceAccount name -> IAM role), not through anything on
# the pod spec, so there's no annotation/admission-webhook step for this
# repo to be missing on that path -- the credentials properties below are
# simply omitted, and Trino's S3 client is expected to fall back to the AWS
# SDK's default credential chain, which is where Pod Identity's injected
# credentials (169.254.170.23/v1/credentials, per floci's own docs) would
# actually be picked up.
#
# IRSA works differently: no node-side relay, no injected 169.254 endpoint.
# Trino's pod needs the projected-service-account-token volume + AWS_ROLE_ARN/
# AWS_WEB_IDENTITY_TOKEN_FILE env vars, which real EKS's amazon-eks-pod-identity-
# webhook auto-injects from the ServiceAccount's eks.amazonaws.com/role-arn
# annotation (serviceAccount.annotations block below, sourced from
# service_account_annotations). Whether floci's k3s emulation runs an
# equivalent auto-injecting webhook is NOT yet confirmed -- if it doesn't,
# the AWS SDK's WebIdentityTokenFileCredentialsProvider will find nothing and
# this fallback needs hand-wiring into helm-values/trino-values.yaml instead.
# Verify with `kubectl exec <trino-pod> -- env | grep AWS` on first apply
# with "irsa" on, same discipline as everywhere else in this repo.
#
# Either way, the credentials-properties fallback below (omitted, relying on
# the AWS SDK's default chain) is standard for AWS-SDK-v2-based clients but
# NOT independently confirmed against Trino's specific S3 filesystem
# implementation. GCP and Azure's Workload Identity designs are also
# annotation-based (see each provider's own workload-identity.tf) -- the
# serviceAccount.annotations block below covers all three (AWS-irsa, GCP,
# Azure) with the same lookup().
#
# s3.endpoint itself is unrelated to which credential mechanism is
# active -- always the provider layer's shared bucket endpoint (see
# CONTRACT.md's "object-storage outputs" section), never this tenant's
# own MinIO.
#
# KNOWN GAP: this doesn't yet configure Trino's resource-groups.json for
# query-level CPU/memory quotas per tenant on a shared cluster -- the
# ResourceQuota in namespace.tf covers pod-level requests/limits, but true
# query-level resource groups (the pool tier's other isolation mechanism,
# per the tenancy-architecture page) are a real next step, not yet wired up.

locals {
  # fs.native-s3.enabled / s3.ssl.enabled are both stale against current
  # Trino (confirmed against v483, what `image.tag: latest` resolves to
  # today): the property was renamed to fs.s3.enabled, and s3.ssl.enabled
  # was removed outright -- SSL/TLS is inferred from s3.endpoint's own
  # scheme, not a separate flag.
  #
  # s3.region is NOT optional, even against MinIO -- confirmed live: without
  # it, the coordinator crashes on startup (exit code 100) while loading the
  # Iceberg connector, deep in Guice injector creation, with the real cause
  # buried several "Caused by:"s down: SdkClientException: Unable to load
  # region from any of the providers in the chain (env var, AWS profile,
  # EC2 instance metadata -- none of which mean anything against a MinIO
  # container on a homelab box). Trino's own S3 filesystem support has an
  # open upstream issue about this exact requirement (trinodb/trino#21795,
  # "Make region in S3 native file system optional") -- as of v483 it's
  # still mandatory. The value itself doesn't matter to MinIO (no real
  # region enforcement), so this just matches what catalog.tf already sets
  # for Nessie's own S3 client, for consistency. That match is now the
  # provider layer's job to keep, not this file's -- see below.

  # HAND-WIRED, not a guess -- see helm_release.trino's env/additionalVolumes/
  # additionalVolumeMounts below. Same empirically-confirmed gap as
  # terraform/platform/catalog.tf's identical local (2026-09-24): floci's
  # EKS emulation does not run the real EKS amazon-eks-pod-identity-webhook,
  # so the eks.amazonaws.com/role-arn ServiceAccount annotation below is
  # inert on its own -- nothing reads it to inject the projected-token
  # volume + AWS_ROLE_ARN/AWS_WEB_IDENTITY_TOKEN_FILE env vars a real EKS
  # control plane would provide automatically. This does that by hand
  # instead, using the role ARN Terraform already knows directly. Only for
  # "irsa" -- pod-identity's credential delivery doesn't use a projected
  # token at all (see this file's header comment), and GCP/Azure's own
  # Workload Identity designs are unaffected (this local is AWS-specific,
  # named accordingly).
  tenant_irsa_hand_wired = var.workload_identity_mechanism == "irsa"
  tenant_irsa_role_arn   = lookup(lookup(var.service_account_annotations, var.tenant_id, {}), "eks.amazonaws.com/role-arn", "")

  # The five universal lines: protocol-independent, about Trino's
  # relationship to Nessie's REST catalog, not about storage or which
  # provider produced this cluster -- these stay owned by this file
  # directly, same as always. var.object_storage_catalog_properties below
  # is the provider-emitted, protocol-specific half (fs.s3.*/s3.* today,
  # a future provider's fs.gs.*/etc. later) -- see this file's header
  # comment and CONTRACT.md's "The storage-protocol fork, and the sixth
  # output that keeps it out of _template" section. Spliced in verbatim,
  # never parsed.
  iceberg_catalog_properties = <<-PROPERTIES
    connector.name=iceberg
    iceberg.catalog.type=rest
    iceberg.rest-catalog.uri=${var.catalog_uri}
    iceberg.rest-catalog.warehouse=${var.tenant_id}
    iceberg.file-format=PARQUET
    ${var.object_storage_catalog_properties}
  PROPERTIES
}

resource "helm_release" "trino" {
  name       = "trino"
  repository = "https://trinodb.github.io/charts"
  chart      = "trino"
  namespace  = kubernetes_namespace_v1.tenant.metadata[0].name

  # Separate from the pod-level startupProbe tuning in trino-values.yaml --
  # this is Helm's own wait-for-ready timeout on the whole release
  # (provider default 300s), which was expiring before the coordinator/
  # worker's now-longer probe window even elapsed. Matched to roughly the
  # same ceiling so neither one gives up before the other.
  timeout = 600

  values = [
    file("${path.module}/../../../helm-values/trino-values.yaml"),
    yamlencode({
      additionalCatalogs = {
        iceberg = local.iceberg_catalog_properties
      }
      # Explicit and fixed, not left to the chart's own release-name-
      # derived default -- so a known name is always available to
      # reference rather than guessing the chart's naming convention.
      # Standard top-level Trino chart keys (trinodb/charts) -- not yet
      # independently confirmed against a live `helm show values
      # trino/trino`, same caveat this file's own comments already apply
      # elsewhere.
      serviceAccount = {
        create = true
        name   = "trino"
        # Option A: merges in this tenant's own federation annotation
        # (eks.amazonaws.com/role-arn, iam.gke.io/gcp-service-account, or
        # azure.workload.identity/client-id) when the provider layer
        # produced one -- see CONTRACT.md's "Native federation (Option A)"
        # section and each provider's own workload-identity.tf. lookup()'s
        # third argument is the fallback: {} whenever this tenant_id isn't
        # a key in the map at all (var.tenant_ids didn't include it on the
        # provider side) or the provider hasn't wired Option A yet. AWS's
        # own outputs.tf returns {} here when Pod Identity is active (it
        # doesn't use ServiceAccount annotations at all -- see this file's
        # header comment) but a real eks.amazonaws.com/role-arn entry per
        # tenant when "irsa" is active. Harmless to always merge in either
        # way: an extra annotation on a ServiceAccount that isn't otherwise
        # using it does nothing.
        annotations = lookup(var.service_account_annotations, var.tenant_id, {})
      }
      # Hand-wired IRSA credential delivery -- see locals.tenant_irsa_hand_wired's
      # comment above for why this exists at all (floci doesn't run the
      # webhook that would normally do this from the annotation above).
      # Keys confirmed against the chart's real shape via `helm show values
      # trino/trino` (2026-09-24), not guessed: `env` is top-level and
      # shared by every pod the chart creates, but volumes are per-role
      # (coordinator.additionalVolumes / worker.additionalVolumeMounts,
      # NOT a single top-level additionalVolumes) -- both need it, since
      # both the coordinator and workers talk to S3 for Iceberg reads/
      # writes, not just the coordinator. [] / {} when not applicable so
      # this is a no-op on every other mechanism/provider.
      # AWS_ENDPOINT_URL_STS -- same fix as terraform/platform/catalog.tf's
      # identical block, see its comment for the full story (confirmed
      # 2026-09-24): the credential provider's internal STS client has its
      # own endpoint resolution independent of whatever S3 endpoint is
      # configured elsewhere, and without this override it calls real AWS
      # STS instead of floci -- which correctly rejects a token signed by
      # floci's fake local OIDC issuer. Confirmed by replaying the identical
      # (correctly-rejected-by-real-AWS) token directly against floci's own
      # STS endpoint by hand, which succeeded immediately.
      env = local.tenant_irsa_hand_wired ? [
        { name = "AWS_ROLE_ARN", value = local.tenant_irsa_role_arn },
        { name = "AWS_WEB_IDENTITY_TOKEN_FILE", value = "/var/run/secrets/eks.amazonaws.com/serviceaccount/token" },
        { name = "AWS_ENDPOINT_URL_STS", value = var.object_storage_endpoint },
        { name = "AWS_REGION", value = "us-east-1" },
        { name = "AWS_DEFAULT_REGION", value = "us-east-1" },
      ] : []
      coordinator = {
        additionalVolumes = local.tenant_irsa_hand_wired ? [
          {
            name = "aws-iam-token"
            projected = {
              sources = [
                {
                  serviceAccountToken = {
                    audience          = "sts.amazonaws.com"
                    expirationSeconds = 86400
                    path              = "token"
                  }
                }
              ]
            }
          }
        ] : []
        additionalVolumeMounts = local.tenant_irsa_hand_wired ? [
          {
            name      = "aws-iam-token"
            mountPath = "/var/run/secrets/eks.amazonaws.com/serviceaccount"
            readOnly  = true
          }
        ] : []
      }
      worker = {
        additionalVolumes = local.tenant_irsa_hand_wired ? [
          {
            name = "aws-iam-token"
            projected = {
              sources = [
                {
                  serviceAccountToken = {
                    audience          = "sts.amazonaws.com"
                    expirationSeconds = 86400
                    path              = "token"
                  }
                }
              ]
            }
          }
        ] : []
        additionalVolumeMounts = local.tenant_irsa_hand_wired ? [
          {
            name      = "aws-iam-token"
            mountPath = "/var/run/secrets/eks.amazonaws.com/serviceaccount"
            readOnly  = true
          }
        ] : []
      }
    }),
  ]
}

