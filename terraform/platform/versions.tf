terraform {
  required_version = ">= 1.5.0"

  required_providers {
    kubernetes = {
      source = "hashicorp/kubernetes"
      # < 2.38.0, not just "~> 2.30" -- confirmed against this repo's own
      # apply output, not assumed: `~> 2.30` (a two-segment constraint)
      # only pins the MAJOR version, so 2.38.x satisfies it fine. 2.38.0
      # (July 2025, PR #2751) added a "resource identity" feature with a
      # real, open, unresolved bug
      # (hashicorp/terraform-provider-kubernetes#2779): resources with
      # wait-for-rollout logic (kubernetes_deployment_v1,
      # kubernetes_stateful_set_v1, kubernetes_secret_v1,
      # kubernetes_config_map_v1 -- exactly catalog.tf/kestra.tf/
      # shared-oltp.tf/tenant-pool-postgres.tf's Deployments) save a null
      # identity during create, then the very next read returns the real
      # one, and the provider treats any identity mismatch as fatal
      # ("Unexpected Identity Change"). Two fix PRs (#2841, #2859) are
      # open but unmerged as of this writing -- re-widen once one ships.
      version = ">= 2.30.0, < 2.38.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.14"
    }
  }
}

# Configured entirely from the provider contract (see
# terraform/providers/CONTRACT.md) -- this file has no idea which of
# providers/{baremetal,aws,gcp,azure} produced var.kubeconfig_path, and
# that's the point. Feed it whichever provider's kubeconfig_path output
# you applied (scripts/03-apply-provider.sh captures this into
# terraform/generated/<provider>.tfvars.json for you).

provider "kubernetes" {
  config_path = var.kubeconfig_path
}

provider "helm" {
  kubernetes {
    config_path = var.kubeconfig_path
  }
}

