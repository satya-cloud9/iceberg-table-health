# --- EKS cluster / node group service roles ---
#
# Real EKS requires both of these to exist before CreateCluster/
# CreateNodegroup succeed; floci was empirically lenient about this in the
# probe that confirmed OIDC support (it accepted a role ARN that didn't
# exist at all) but these are built as the real thing anyway, with the
# real managed-policy attachments, since versions.tf's whole migration
# story for this directory is "point at real AWS later, nothing else
# should need to change."

data "aws_iam_policy_document" "eks_cluster_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eks_cluster" {
  name               = "${var.project_name}-eks-cluster"
  assume_role_policy = data.aws_iam_policy_document.eks_cluster_assume.json
}

resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  role       = aws_iam_role.eks_cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

data "aws_iam_policy_document" "eks_node_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eks_node" {
  name               = "${var.project_name}-eks-node"
  assume_role_policy = data.aws_iam_policy_document.eks_node_assume.json
}

resource "aws_iam_role_policy_attachment" "eks_node_worker_policy" {
  role       = aws_iam_role.eks_node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "eks_node_cni_policy" {
  role       = aws_iam_role.eks_node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "eks_node_ecr_policy" {
  role       = aws_iam_role.eks_node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

# Base data-access policy document, attached to whichever identity needs
# bucket-wide S3/KMS reach -- today that's just the static
# object_storage_* credential below (Nessie needs bucket-wide reach
# across every tenant's prefix, not one tenant's narrow slice).
#
# A per-tenant IAM role per component (trino_trust, aws_iam_role.trino/
# kestra), each with a placeholder trust policy naming only "some EKS
# service" and nothing more (no tenant name was knowable yet at
# provider-apply time), and later a Vault-broker design routing identity
# through one shared bootstrap IAM user/role, both used to live in this
# file. Both removed from this branch -- workload-identity.tf now carries
# AWS's real per-tenant mechanism (Option A, native federation via EKS
# Pod Identity -- aws_eks_pod_identity_association, not classic
# OIDC-federated IRSA), gated behind var.enable_native_workload_identity
# so it stays a no-op until you opt in. See
# terraform/providers/CONTRACT.md's "Native federation (Option A)"
# section for the current status.

data "aws_iam_policy_document" "lakehouse_data_access" {
  statement {
    sid    = "S3Warehouse"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.parity.arn,
      "${aws_s3_bucket.parity.arn}/*",
    ]
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

resource "aws_iam_policy" "lakehouse_data_access" {
  name   = "${var.project_name}-data-access"
  policy = data.aws_iam_policy_document.lakehouse_data_access.json
}

# --- Object storage static credential (CONTRACT.md's object-storage outputs) ---
#
# Nessie (terraform/platform/catalog.tf) authenticates with this. It
# needs bucket-wide reach across every tenant's prefix rather than one
# tenant's narrow slice, so it stays on the shared static credential even
# once Trino has a real per-tenant mechanism (workload-identity.tf).
# Trino itself uses this same credential too, until
# var.enable_native_workload_identity is on and a real OIDC issuer is
# wired up (see trino.tf's header comment and workload-identity.tf).
# Same kind of static credential bare metal's static-secret mechanism
# already uses, just as an IAM user/access-key pair here instead of a
# Kubernetes Secret's literal value.

resource "aws_iam_user" "object_storage" {
  name = "${var.project_name}-object-storage"
}

resource "aws_iam_user_policy_attachment" "object_storage_data_access" {
  user       = aws_iam_user.object_storage.name
  policy_arn = aws_iam_policy.lakehouse_data_access.arn
}

resource "aws_iam_access_key" "object_storage" {
  user = aws_iam_user.object_storage.name
}

# --- Real (non-test/test) IAM identity for EKS cluster creation + kubectl auth ---
#
# ROOT CAUSE of the persistent 401 Unauthorized from kubectl/the Kubernetes
# and Helm providers, confirmed against floci's own docs and source, not
# assumed: floci-io/floci#2912 ("fix(eks): validate IAM authentication
# tokens", merged 2026-09-02, present in the `nightly` image
# docker-compose.floci.yml now pins) hardened the EKS token-authentication
# webhook -- it now actually validates the SigV4 signature on `aws eks
# get-token` bearer tokens (previously it accepted ANY k8s-aws-v1.-prefixed
# token unconditionally, which is why test/test worked in the earlier,
# pre-#2912-image probe that first confirmed OIDC support). Per floci's own
# EKS docs (floci.io/floci/services/eks/): "The public local-development
# pairs test/test and floci/floci are deliberately rejected because the
# webhook grants cluster-admin access." That's an unconditional, identity-
# level rejection -- it doesn't matter what RBAC/access-config says, those
# two specific credential pairs never pass this webhook.
#
# Every identity in this directory, including the one that calls
# CreateCluster (which is what bootstrap_cluster_creator_admin_permissions
# in cluster.tf maps to cluster-admin), was the shared provider "aws"
# block's test/test credentials -- so the bootstrap grant itself was
# pointed at an identity the webhook always rejects, independent of every
# access_config/access-entry theory tried before this. The fix only needs
# to be as wide as the problem: floci's docs also confirm "Non-worker IAM
# users and ordinary STS sessions retain the existing cluster-admin
# compatibility behavior" -- i.e. any REAL IAM identity (not the hardcoded
# test/test pair) is sufficient, no access entries required, which is
# exactly what bootstrap_cluster_creator_admin_permissions already assumes.
#
# So: one dedicated real IAM user + access key, used ONLY for (a) the
# provider alias that creates aws_eks_cluster.this (so the recorded
# "cluster creator" is this identity, not test/test -- see versions.tf's
# aws.eks_admin provider block) and (b) the kubeconfig exec-plugin
# environment (cluster.tf's null_resource.kubeconfig). Everything else in
# this directory keeps using the default test/test provider -- floci's EKS
# token webhook is the only place this rejection has been confirmed, so
# there's no reason to widen this past the one thing it actually fixes.
resource "aws_iam_user" "eks_admin" {
  name = "${var.project_name}-eks-admin"
}

resource "aws_iam_user_policy_attachment" "eks_admin_cluster_policy" {
  # Not load-bearing against floci today (bootstrap_cluster_creator_admin_permissions
  # is what actually grants cluster access, per the header comment above) --
  # attached anyway so this identity is the real, least-surprising shape if
  # this directory ever points at real AWS (see versions.tf's own header
  # comment on that migration path).
  user       = aws_iam_user.eks_admin.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_iam_access_key" "eks_admin" {
  user = aws_iam_user.eks_admin.name
}

