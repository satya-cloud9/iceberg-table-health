# Option A: native federation for object storage access, via one of two
# real AWS mechanisms, chosen by var.aws_workload_identity_mechanism and
# gated overall by var.enable_native_workload_identity (default false;
# nothing in this file runs, and workload_identity_mechanism stays
# "static-secret", until you turn that on). See
# terraform/providers/CONTRACT.md's "Native federation (Option A)" section
# for the current status across all four providers.
#
# --- Why this file has two mechanisms, not one ---
#
# "pod-identity" -- EKS Pod Identity (aws_eks_addon + one
# aws_eks_pod_identity_association per tenant, plus one for Nessie). This
# was the only mechanism here until 2026-09-24. Its Terraform/IAM-side
# wiring is CONFIRMED CORRECT -- verified via `tofu state show` against a
# real apply: the addon installs, and the association binds to exactly the
# right namespace + ServiceAccount + role. It is nonetheless CONFIRMED
# BLOCKED, on the data-plane side, not this HCL: floci's own credential-
# delivery relay (the socat-based proxy meant to serve
# 169.254.170.23, added in floci-io/floci#4124, merged 2026-09-22 -- one
# day before this was tested) fails to install, logging "Could not install
# Pod Identity relay dependencies... No supported package manager found
# for IMDS proxy dependencies." No local workaround exists: neither the
# emulated node nor floci's own controller container (itself a stripped
# RHEL 9 image with no package manager anywhere on its own PATH) has
# anywhere to install `socat` from. That failure is cluster-wide -- every
# pod on the cluster trying to use Pod Identity hits the same missing
# relay, Nessie and every tenant's Trino alike. Kept here, gated off by
# default now, ready to flip back on once floci ships a fix. Full writeup:
# scratch/lakehouse-series-03-the-credential-that-never-touches-terraform.md.
#
# "irsa" -- classic OIDC federation (aws_iam_openid_connect_provider plus
# ServiceAccount-annotation-based trust conditions on
# sts:AssumeRoleWithWebIdentity). DEFAULT as of 2026-09-24, specifically
# because its data plane doesn't touch the broken component above at all: a
# pod's credentials come from a projected ServiceAccount token file (a
# stock kubelet/API-server feature, nothing cloud-specific, nothing floci
# has to implement) plus a direct sts:AssumeRoleWithWebIdentity call the
# AWS SDK inside the pod makes for itself -- no node-side relay, nothing
# floci needs to install anywhere. This is the SAME classic-IRSA shape this
# file used to build before Pod Identity replaced it, rebuilt now because
# the thing that blocked it then -- `kind` having no publicly-reachable
# OIDC discovery document -- is gone: cluster.tf creates the cluster
# through floci's own EKS API, which returns a real
# identity.oidc.issuer (floci-io/floci#3783, merged 2026-09-18, confirmed
# empirically against a `nightly` image).
#
# Genuinely unverified, same discipline as everywhere else in this repo:
# (1) whether floci's STS emulation actually validates
# AssumeRoleWithWebIdentity's signature/audience/subject-claim checks
# correctly, not just accepts the call; and (2) whether real EKS's
# `amazon-eks-pod-identity-webhook` -- a built-in control-plane component
# on real EKS, not an addon, responsible for auto-injecting the projected
# token volume plus AWS_ROLE_ARN/AWS_WEB_IDENTITY_TOKEN_FILE into a pod
# whose ServiceAccount carries the eks.amazonaws.com/role-arn annotation --
# is something floci's emulation runs at all. Unconfirmed either way as of
# this writing. First empirical check once this applies: `kubectl get pods
# -n kube-system` (does anything resembling that webhook show up?) and
# `kubectl exec <nessie-or-trino-pod> -- env | grep AWS` (did anything get
# injected?) -- same diagnostic pattern that root-caused the Pod Identity
# relay failure. If nothing injects it, platform/catalog.tf's
# helm_release.nessie and tenants/_template/trino.tf's helm_release.trino
# will need the projected-volume + env vars wired into their Helm values
# by hand instead -- not yet done here, since guessing at unverified
# chart-specific volume/env keys is exactly the class of silent-no-op bug
# this repo's own comments keep warning about elsewhere (see catalog.tf's
# own "CHART SCHEMA CAVEAT" for a real example of that happening before).

locals {
  native_federation_enabled = var.enable_native_workload_identity
  pod_identity_enabled      = local.native_federation_enabled && var.aws_workload_identity_mechanism == "pod-identity"
  irsa_enabled              = local.native_federation_enabled && var.aws_workload_identity_mechanism == "irsa"
}

# ---------------------------------------------------------------------------
# Shared data-access policy document -- identical S3 prefix-scoping logic
# regardless of which mechanism actually delivers the credential to a pod.
# Built once per tenant whenever EITHER mechanism is on; attached to
# whichever role (pod-identity or irsa) is actually active below.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "tenant_data_access" {
  for_each = local.native_federation_enabled ? toset(var.tenant_ids) : toset([])

  statement {
    sid    = "TenantPrefixOnly"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = ["${aws_s3_bucket.parity.arn}/${each.key}/*"]
  }

  statement {
    sid       = "ListOwnPrefixOnly"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.parity.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${each.key}/*"]
    }
  }

  statement {
    sid    = "KmsForWarehouse"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey",
    ]
    resources = [aws_kms_key.lakehouse.arn]
  }
}

# ---------------------------------------------------------------------------
# "pod-identity" mechanism -- gated off by default now
# (var.aws_workload_identity_mechanism defaults to "irsa"). Kept intact
# rather than deleted: see this file's header comment for why.
# ---------------------------------------------------------------------------

resource "aws_eks_addon" "pod_identity_agent" {
  count = local.pod_identity_enabled ? 1 : 0

  cluster_name = aws_eks_cluster.this.name
  addon_name   = "eks-pod-identity-agent"

  depends_on = [aws_eks_node_group.workers]
}

data "aws_iam_policy_document" "tenant_pod_identity_trust" {
  for_each = local.pod_identity_enabled ? toset(var.tenant_ids) : toset([])

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "tenant_trino_pod_identity" {
  for_each = local.pod_identity_enabled ? toset(var.tenant_ids) : toset([])

  name               = "${var.project_name}-pod-identity-${each.key}-trino"
  assume_role_policy = data.aws_iam_policy_document.tenant_pod_identity_trust[each.key].json
}

resource "aws_iam_role_policy" "tenant_trino_pod_identity_data_access" {
  for_each = local.pod_identity_enabled ? toset(var.tenant_ids) : toset([])

  name   = "${var.project_name}-pod-identity-${each.key}-data-access"
  role   = aws_iam_role.tenant_trino_pod_identity[each.key].name
  policy = data.aws_iam_policy_document.tenant_data_access[each.key].json
}

resource "aws_eks_pod_identity_association" "tenant_trino" {
  for_each = local.pod_identity_enabled ? toset(var.tenant_ids) : toset([])

  cluster_name    = aws_eks_cluster.this.name
  namespace       = "tenant-${each.key}"
  service_account = "trino"
  role_arn        = aws_iam_role.tenant_trino_pod_identity[each.key].arn

  depends_on = [aws_eks_addon.pod_identity_agent]
}

data "aws_iam_policy_document" "nessie_pod_identity_trust" {
  count = local.pod_identity_enabled ? 1 : 0

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "nessie_pod_identity" {
  count = local.pod_identity_enabled ? 1 : 0

  name               = "${var.project_name}-pod-identity-nessie"
  assume_role_policy = data.aws_iam_policy_document.nessie_pod_identity_trust[0].json
}

resource "aws_iam_role_policy_attachment" "nessie_pod_identity_data_access" {
  count = local.pod_identity_enabled ? 1 : 0

  role       = aws_iam_role.nessie_pod_identity[0].name
  policy_arn = aws_iam_policy.lakehouse_data_access.arn
}

resource "aws_eks_pod_identity_association" "nessie" {
  count = local.pod_identity_enabled ? 1 : 0

  cluster_name    = aws_eks_cluster.this.name
  namespace       = var.platform_namespace
  service_account = "nessie"
  role_arn        = aws_iam_role.nessie_pod_identity[0].arn

  depends_on = [aws_eks_addon.pod_identity_agent]
}

# ---------------------------------------------------------------------------
# "irsa" mechanism -- default as of 2026-09-24. See this file's header
# comment for what's confirmed vs. still open about it.
# ---------------------------------------------------------------------------

# The thumbprint IAM needs to trust the issuer's TLS chain -- fetched live
# from the issuer URL itself, the same bootstrap pattern every reference
# IRSA module (e.g. terraform-aws-modules/eks) uses. UNVERIFIED against
# floci specifically, beyond the issuer STRING itself: this data source
# makes a real outbound HTTPS connection to
# aws_eks_cluster.this.identity[0].oidc[0].issuer and needs a real TLS
# certificate chain back. floci-io/floci#3783 confirms DescribeCluster
# returns a real issuer URL; whether that URL is actually reachable and
# TLS-serving is a separate, unconfirmed claim -- this is the first thing
# that will fail loudly (not silently) if it isn't.
data "tls_certificate" "eks_oidc" {
  count = local.irsa_enabled ? 1 : 0
  url   = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  count = local.irsa_enabled ? 1 : 0

  url             = aws_eks_cluster.this.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.eks_oidc[0].certificates[0].sha1_fingerprint]
}

# The OIDC provider's own trust-policy condition keys are of the form
# "<issuer-host-and-path>:sub" -- keyed on the bare host+path, never the
# "https://" scheme, per AWS's own documented IRSA trust-policy shape.
locals {
  oidc_provider_no_scheme = local.irsa_enabled ? replace(aws_eks_cluster.this.identity[0].oidc[0].issuer, "https://", "") : ""
}

data "aws_iam_policy_document" "tenant_trino_irsa_trust" {
  for_each = local.irsa_enabled ? toset(var.tenant_ids) : toset([])

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.eks[0].arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_no_scheme}:sub"
      values   = ["system:serviceaccount:tenant-${each.key}:trino"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_no_scheme}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "tenant_trino_irsa" {
  for_each = local.irsa_enabled ? toset(var.tenant_ids) : toset([])

  name               = "${var.project_name}-irsa-${each.key}-trino"
  assume_role_policy = data.aws_iam_policy_document.tenant_trino_irsa_trust[each.key].json
}

resource "aws_iam_role_policy" "tenant_trino_irsa_data_access" {
  for_each = local.irsa_enabled ? toset(var.tenant_ids) : toset([])

  name   = "${var.project_name}-irsa-${each.key}-data-access"
  role   = aws_iam_role.tenant_trino_irsa[each.key].name
  policy = data.aws_iam_policy_document.tenant_data_access[each.key].json
}

data "aws_iam_policy_document" "nessie_irsa_trust" {
  count = local.irsa_enabled ? 1 : 0

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.eks[0].arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_no_scheme}:sub"
      values   = ["system:serviceaccount:${var.platform_namespace}:nessie"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_no_scheme}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "nessie_irsa" {
  count = local.irsa_enabled ? 1 : 0

  name               = "${var.project_name}-irsa-nessie"
  assume_role_policy = data.aws_iam_policy_document.nessie_irsa_trust[0].json
}

resource "aws_iam_role_policy_attachment" "nessie_irsa_data_access" {
  count = local.irsa_enabled ? 1 : 0

  role       = aws_iam_role.nessie_irsa[0].name
  policy_arn = aws_iam_policy.lakehouse_data_access.arn
}

