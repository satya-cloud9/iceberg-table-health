terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # >= 5.55 for aws_eks_pod_identity_association (workload-identity.tf) --
      # that resource landed in the AWS provider around when EKS Pod Identity
      # itself went GA (late 2023 / early 2024). Not independently confirmed
      # against this exact version boundary -- if `tofu init` can't find the
      # resource type, bump the lower bound and note the version that actually
      # worked here.
      version = ">= 5.55.0, < 6.0.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
    # For the "irsa" workload-identity mechanism only (workload-identity.tf,
    # data.tls_certificate.eks_oidc) -- fetches the EKS OIDC issuer's real
    # TLS certificate chain to compute the thumbprint
    # aws_iam_openid_connect_provider needs. Unused, and this requirement
    # harmless to have declared, when aws_workload_identity_mechanism stays
    # "pod-identity" or native federation is off entirely.
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }
}

# All AWS-shaped resources in this directory are applied against floci, and
# are a contract-test double, not a real cluster provisioner -- see
# terraform/providers/CONTRACT.md and docs/architecture.md.
#
# This used to also apply to the cluster itself: `kind` directly, not
# floci's own EKS emulation, on the reasoning that floci's EKS coverage was
# "just another layer of indirection over the same kind cluster" and
# couldn't prove real IRSA/Workload-Identity token exchange anyway. That
# reasoning no longer holds -- floci's EKS emulation (a real k3s node per
# cluster) gained genuine OIDC-signed service-account tokens and EKS Pod
# Identity support (floci-io/floci#3783, merged 2026-09-18, confirmed
# empirically against a `nightly` image: DescribeCluster returns a real
# identity.oidc.issuer). cluster.tf now creates the cluster through floci's
# EKS API (aws_eks_cluster + aws_eks_node_group) instead of `kind` directly,
# which is what makes workload-identity.tf's Pod Identity associations
# possible without a manual JWKS-hosting workaround. Real-AWS credentials
# are never read (floci accepts any non-empty access/secret key). To point
# this at real AWS later: delete the `endpoints` block below and supply
# real credentials via the usual AWS provider mechanisms -- cluster.tf and
# workload-identity.tf's resources are already the real EKS/IAM shapes, not
# floci-specific stand-ins, so nothing else in this directory should need
# to change.
provider "aws" {
  region                      = var.aws_region
  access_key                  = "test"
  secret_key                  = "test"
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3         = var.aws_emulator_endpoint
    iam        = var.aws_emulator_endpoint
    sts        = var.aws_emulator_endpoint
    kms        = var.aws_emulator_endpoint
    ec2        = var.aws_emulator_endpoint
    eks        = var.aws_emulator_endpoint
    cloudwatch = var.aws_emulator_endpoint
    logs       = var.aws_emulator_endpoint
  }
}

# Second, aliased provider instance -- same endpoint, but a real
# (non-test/test) identity from iam.tf's aws_iam_access_key.eks_admin.
# aws_eks_cluster.this (cluster.tf) is the only resource that uses it. See
# iam.tf's aws_iam_user.eks_admin header comment for why this exists
# (floci-io/floci#2912 rejects test/test specifically for EKS's token-auth
# webhook, and bootstrap_cluster_creator_admin_permissions needs the
# cluster-creator identity to be one the webhook will actually accept).
#
# Chicken-and-egg note: this can't reference
# aws_iam_access_key.eks_admin's own attributes directly -- Terraform
# requires a provider's configuration to be fully known before any
# resource in its scope is planned, and a resource attribute from the same
# apply isn't known that early. var.eks_admin_access_key_id/secret are
# populated from a generated eks_admin.auto.tfvars.json file that
# scripts/03-apply-provider.sh writes via a preliminary
# `tofu apply -target=aws_iam_access_key.eks_admin` pass before the real
# apply runs -- see that script's header comment for the two-phase
# sequence. Both variables default to "" so `tofu validate`/a first
# `tofu plan` don't hard-fail before that pass has ever run; an apply of
# aws_eks_cluster.this with empty credentials will fail loudly at floci
# rather than silently, which is the right failure mode here.
provider "aws" {
  alias                       = "eks_admin"
  region                      = var.aws_region
  access_key                  = var.eks_admin_access_key_id
  secret_key                  = var.eks_admin_secret_access_key
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    eks = var.aws_emulator_endpoint
  }
}

