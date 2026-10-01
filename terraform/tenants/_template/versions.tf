terraform {
  required_version = ">= 1.5.0"

  required_providers {
    kubernetes = {
      source = "hashicorp/kubernetes"
      # < 2.38.0 -- see terraform/platform/versions.tf's header comment
      # on this same constraint for the full story
      # (hashicorp/terraform-provider-kubernetes#2779, an open,
      # unresolved "Unexpected Identity Change" bug on resources with
      # wait-for-rollout logic). Keep this in sync with that file.
      version = ">= 2.30.0, < 2.38.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.14"
    }
  }
}

# No provider blocks here on purpose -- this is a child module, not a
# root. It inherits kubernetes/helm's provider configuration from
# whichever root module calls it (see ../tenant-a/main.tf). Every tenant
# always needs both, identically, on every provider -- there's nothing to
# make conditional.
#
# A hashicorp/vault provider and its own vault.tf resources (per-tenant
# Vault policy, Kubernetes-auth role, AWS secrets-engine role, and the
# credential-minting data source trino.tf consumed) used to live here,
# implementing the Vault-broker mechanism for AWS. Deliberately removed
# from this branch -- see terraform/providers/CONTRACT.md's "Known gap"
# section for the current, vault-free status of native-federation
# binding on every provider.

