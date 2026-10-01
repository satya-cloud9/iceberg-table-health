# Nessie as the shared Iceberg REST catalog -- the multi-provider-correct
# choice over Glue (see the "Cross-tenant data sharing" and topology
# sections of the tenancy-architecture reference page): one service, one
# per cluster, speaking the standard Iceberg REST API regardless of which
# provider produced this cluster. Backed by its own small Postgres so
# catalog metadata survives a pod restart -- Nessie's in-memory default
# store does not.
#
# CHART SCHEMA CAVEAT (same as helm-values/kestra-values.yaml): written
# from Nessie's documented Helm values shape
# (https://charts.projectnessie.org), not verified against a live `helm
# install`. Run `helm show values nessie/nessie` first and reconcile any
# drifted keys before applying -- `versionStoreType`/`postgres.jdbcUrl` are
# the ones most likely to have moved.

resource "kubernetes_secret_v1" "nessie_postgres" {
  metadata {
    name      = "nessie-postgres"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  data = {
    POSTGRES_USER     = "nessie"
    POSTGRES_PASSWORD = "nessie" # local-only demo credential -- do not reuse anywhere real
    POSTGRES_DB       = "nessie"
  }
}

# Only created when Nessie is actually going to use it. Sourced from the
# provider layer (CONTRACT.md's object-storage outputs), not a literal
# "minioadmin" -- this is the same credential every tenant's Trino also
# uses at its own default (see terraform/tenants/_template/trino.tf),
# since there's one shared bucket and, outside AWS's own Pod Identity
# path, per-tenant credential scoping isn't wired up yet.
#
# count, not unconditional: when var.workload_identity_mechanism is
# "pod-identity" or "irsa" (AWS, Option A on, either mechanism), Nessie
# authenticates via its own native-federation identity instead
# (terraform/providers/aws/workload-identity.tf's aws_iam_role.nessie_pod_identity
# or aws_iam_role.nessie_irsa, depending on which is active) -- see the
# helm_release.nessie block below for the authType branch this secret's
# existence feeds.
resource "kubernetes_secret_v1" "nessie_object_storage_creds" {
  count = local.nessie_uses_native_federation ? 0 : 1

  metadata {
    name      = "nessie-object-storage-creds"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  data = {
    awsAccessKeyId     = var.object_storage_access_key_id
    awsSecretAccessKey = var.object_storage_secret_access_key
  }
}

resource "kubernetes_persistent_volume_claim_v1" "nessie_postgres" {
  # local-path uses WaitForFirstConsumer binding -- it deliberately delays
  # binding the PVC until a pod that mounts it is scheduled. Terraform's
  # default wait_until_bound = true blocks THIS resource's own apply step
  # waiting for Bound, but the consuming Deployment below never gets
  # created until this step finishes -- a real deadlock, not specific to
  # this box (confirmed against known kubernetes provider issues with
  # local-path/WaitForFirstConsumer storage classes).
  wait_until_bound = false

  metadata {
    name      = "nessie-postgres-data"
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

resource "kubernetes_deployment_v1" "nessie_postgres" {
  metadata {
    name      = "nessie-postgres"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  spec {
    replicas = 1
    selector {
      match_labels = { app = "nessie-postgres" }
    }
    template {
      metadata {
        labels = { app = "nessie-postgres" }
      }
      spec {
        container {
          name  = "postgres"
          image = "postgres:16-alpine"
          env_from {
            secret_ref { name = kubernetes_secret_v1.nessie_postgres.metadata[0].name }
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
        }
        volume {
          name = "data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.nessie_postgres.metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "nessie_postgres" {
  metadata {
    name      = "nessie-postgres"
    namespace = kubernetes_namespace_v1.platform.metadata[0].name
  }
  spec {
    selector = { app = "nessie-postgres" }
    port {
      port        = 5432
      target_port = 5432
    }
  }
}

locals {
  # Covers both native-federation mechanisms AWS can produce
  # (workload-identity.tf's "pod-identity" and "irsa") -- either way,
  # Nessie gets no static credential and authenticates itself instead. See
  # the two resources/blocks below this feeds for what actually differs
  # between the two: pod-identity needs nothing on the ServiceAccount
  # itself, irsa needs the eks.amazonaws.com/role-arn annotation
  # (nessie_service_account_annotations, sourced from AWS's own output of
  # the same name).
  nessie_uses_native_federation = contains(["pod-identity", "irsa"], var.workload_identity_mechanism)

  # HAND-WIRED, not a guess -- see helm_release.nessie's extraEnv/
  # extraVolumes/extraVolumeMounts below. Confirmed empirically against a
  # real apply (2026-09-24) that floci's EKS emulation does NOT run
  # anything equivalent to the real EKS amazon-eks-pod-identity-webhook:
  # `kubectl get mutatingwebhookconfigurations` showed nothing for it, and
  # the pod had no /var/run/secrets/eks.amazonaws.com/serviceaccount/ token
  # mounted at all. On real EKS that webhook is what reads the
  # eks.amazonaws.com/role-arn ServiceAccount annotation and auto-injects
  # the projected-token volume + AWS_ROLE_ARN/AWS_WEB_IDENTITY_TOKEN_FILE
  # env vars -- since floci doesn't do that, this does it by hand instead,
  # using the role ARN Terraform already knows directly (not dependent on
  # the annotation being read by anything). Only for "irsa" specifically --
  # pod-identity's credential delivery (when/if floci's relay bug is fixed)
  # works through the kubelet-injected 169.254.170.23 endpoint instead, not
  # a projected token, so it doesn't need this.
  nessie_irsa_hand_wired = var.workload_identity_mechanism == "irsa"
  nessie_irsa_role_arn   = lookup(var.nessie_service_account_annotations, "eks.amazonaws.com/role-arn", "")
}

resource "helm_release" "nessie" {
  name       = "nessie"
  repository = "https://charts.projectnessie.org"
  chart      = "nessie"
  # Pinned -- the repo index's "latest" (0.108.5) 404s on its own release
  # asset upstream (broken/retracted release, not something wrong here).
  # 0.108.4 is a known-good fallback: its image already pulled and ran
  # successfully in earlier testing.
  version   = "0.108.4"
  namespace = kubernetes_namespace_v1.platform.metadata[0].name

  values = [
    yamlencode({
      # Fixed, explicit name (not the chart's generated-from-fullname
      # default) so terraform/providers/aws/workload-identity.tf's Nessie
      # Pod Identity association -- built in a separate apply, unilaterally,
      # per CONTRACT.md's "Known gap" section -- has a known ServiceAccount
      # name to bind to regardless of whether Option A is actually on.
      # Harmless when it isn't: an unused ServiceAccount name changes
      # nothing about how Nessie runs.
      serviceAccount = {
        create = true
        name   = "nessie"
        # {} unless AWS's "irsa" mechanism is active -- see
        # variables.tf's nessie_service_account_annotations comment.
        # Harmless to always merge in either way, same reasoning
        # tenants/_template/trino.tf's own annotations line already
        # documents: an extra annotation on a ServiceAccount that isn't
        # otherwise using it does nothing.
        annotations = var.nessie_service_account_annotations
      }
      # Hand-wired IRSA credential delivery -- see locals.nessie_irsa_hand_wired's
      # comment above for why this exists at all (floci doesn't run the
      # webhook that would normally do this from the annotation above).
      # Keys confirmed against the chart's real shape via `helm show values
      # nessie/nessie --version 0.108.4` (2026-09-24), not guessed --
      # extraEnv/extraVolumes/extraVolumeMounts are exactly the documented
      # top-level keys, [] when not applicable so this is a no-op on every
      # other mechanism/provider.
      # AWS_ENDPOINT_URL_STS is the fix for a real, empirically-confirmed bug
      # (2026-09-24): storage.s3.defaultOptions.endpoint above only points
      # Nessie's S3 client at floci -- WebIdentityTokenFileCredentialsProvider's
      # own internal STS client is a SEPARATE client with its own independent
      # endpoint resolution, unaffected by that S3-specific setting. Left
      # unset, it resolves the real AWS STS endpoint, which correctly rejects
      # a token signed by floci's own fake local OIDC issuer for a role in
      # the fake 000000000000 test account -- confirmed directly: replaying
      # the exact same rejected token via `aws sts assume-role-with-web-identity`
      # against floci's endpoint from the host succeeded immediately and
      # returned real credentials, proving the token itself was always valid
      # and the only problem was which STS this SDK's credential provider was
      # calling. AWS_REGION/AWS_DEFAULT_REGION included too since STS's own
      # region resolution is independent of the S3 client's region setting
      # below and has no other source in this pod otherwise.
      extraEnv = local.nessie_irsa_hand_wired ? [
        { name = "AWS_ROLE_ARN", value = local.nessie_irsa_role_arn },
        { name = "AWS_WEB_IDENTITY_TOKEN_FILE", value = "/var/run/secrets/eks.amazonaws.com/serviceaccount/token" },
        { name = "AWS_ENDPOINT_URL_STS", value = var.object_storage_endpoint },
        { name = "AWS_REGION", value = "us-east-1" },
        { name = "AWS_DEFAULT_REGION", value = "us-east-1" },
      ] : []
      extraVolumes = local.nessie_irsa_hand_wired ? [
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
      extraVolumeMounts = local.nessie_irsa_hand_wired ? [
        {
          name      = "aws-iam-token"
          mountPath = "/var/run/secrets/eks.amazonaws.com/serviceaccount"
          readOnly  = true
        }
      ] : []
      # CORRECTED against the chart's real current shape (projectnessie/nessie
      # helm/nessie/README.md) -- the old `postgres.jdbcUrl/username/password`
      # keys above were never a real key in this chart, so they were silently
      # ignored. That's why it failed on `secret "datasource-creds" not
      # found`: with no jdbc.secret block supplied, the chart falls back to
      # its own hardcoded default, which expects a secret literally named
      # "datasource-creds" -- something nothing here ever created. Real
      # shape is versionStoreType: JDBC2 ("JDBC" is a deprecated alias) plus
      # a jdbc.secret block naming an *existing* secret and the KEY NAMES
      # inside it holding the username/password (not literal values) -- so
      # this points at the nessie-postgres secret already created above.
      versionStoreType = "JDBC2"
      jdbc = {
        jdbcUrl = "jdbc:postgresql://nessie-postgres.${var.platform_namespace}.svc.cluster.local:5432/nessie"
        secret = {
          name     = kubernetes_secret_v1.nessie_postgres.metadata[0].name
          username = "POSTGRES_USER"
          password = "POSTGRES_PASSWORD"
        }
      }
      service = {
        type = "ClusterIP"
        port = 19120
      }
      # Every other component in this repo pins resources -- this was the
      # one exception, meaning it ran BestEffort QoS (no CPU/memory floor,
      # first evicted under node pressure) with a footprint invisible to
      # any capacity planning. Sized as a small Quarkus/JVM REST service --
      # lighter than Kestra's Standalone process, similar order of
      # magnitude to Trino's coordinator. Adjust once you've actually
      # observed it running.
      resources = {
        requests = { cpu = "500m", memory = "768Mi" }
        limits   = { cpu = "1", memory = "1536Mi" }
      }

      # catalog.enabled defaults to FALSE in this chart -- confirmed via
      # `helm show values nessie/nessie --version 0.108.4`, not guessed --
      # and the Iceberg REST endpoint requires at least one warehouse and
      # its backing object-store location configured before ANY request
      # succeeds. This is what Trino's coordinator was actually stuck on at
      # startup (inside StaticCatalogManager.loadInitialCatalogs): not a
      # crash, just an endpoint with nothing behind it.
      #
      # Storage now comes from the provider layer (CONTRACT.md's
      # object-storage outputs), not a specific tenant's own MinIO -- see
      # that file's "object-storage outputs" section for the full story of
      # why this changed. The practical effect: `defaultWarehouse` below
      # is genuinely tenant-agnostic, backed by a prefix in the shared
      # bucket that exists from the moment the provider layer applies, so
      # Nessie's readiness probe no longer depends on any tenant existing.
      #
      # What THIS does NOT yet fix: Nessie's warehouse-to-tenant mapping
      # is still static server config -- there's no live API to register a
      # new tenant's warehouse without restarting Nessie (confirmed against
      # Nessie's own docs, not assumed). So the "tenant-a" entry below is
      # still a literal, hand-maintained reference to one specific tenant,
      # and a real second tenant means adding a line here and re-applying
      # platform -- a smaller, config-time coupling than the boot-time one
      # this change removes, not a full elimination of platform knowing
      # tenant names. Fully dynamic registration (tenant's own apply
      # updates this list and rolls Nessie, with no manual edit here) is
      # real follow-up work, not yet built.
      catalog = {
        enabled = true
        iceberg = {
          defaultWarehouse = "default"
          warehouses = [
            {
              name     = "default"
              location = "s3://${var.object_storage_bucket}/_platform/"
            },
            {
              name     = "tenant-a"
              location = "s3://${var.object_storage_bucket}/tenant-a/"
            }
          ]
        }
        storage = {
          s3 = {
            defaultOptions = {
              # Nessie's server-side S3 client (AWS SDK v2) requires SOME
              # region value to be resolvable, even against MinIO which
              # ignores it entirely -- confirmed via the actual health-check
              # error: "Unable to load region from any of the providers in
              # the chain" (env var, AWS profile, EC2 metadata service, all
              # empty in this pod). A placeholder is sufficient.
              region          = "us-east-1"
              endpoint        = var.object_storage_endpoint
              pathStyleAccess = true
              # APPLICATION_GLOBAL ("use the default AWS credentials
              # provider chain", confirmed against the chart's own
              # documented authType values -- projectnessie/nessie
              # helm/nessie/values.yaml) when Nessie's own native-federation
              # identity (workload-identity.tf's pod-identity or irsa role,
              # whichever is active) is what's supposed to be supplying
              # credentials instead of a static key -- the AWS SDK's
              # default chain covers both (ContainerCredentialsProvider for
              # pod-identity, WebIdentityTokenFileCredentialsProvider for
              # irsa). Not independently confirmed that Nessie's own S3
              # client actually picks up either one's injected credentials
              # via this chain end to end -- same unverified-until-apply-
              # tested status as Trino's equivalent fallback, see trino.tf's
              # header comment.
              authType = local.nessie_uses_native_federation ? "APPLICATION_GLOBAL" : "STATIC"
              accessKeySecret = local.nessie_uses_native_federation ? null : {
                name               = kubernetes_secret_v1.nessie_object_storage_creds[0].metadata[0].name
                awsAccessKeyId     = "awsAccessKeyId"
                awsSecretAccessKey = "awsSecretAccessKey"
              }
            }
          }
        }
      }
    })
  ]

  depends_on = [
    kubernetes_deployment_v1.nessie_postgres,
    kubernetes_service_v1.nessie_postgres,
  ]
}

