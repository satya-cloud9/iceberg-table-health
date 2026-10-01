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

provider "kubernetes" {
  config_path = var.kubeconfig_path
}

provider "helm" {
  kubernetes {
    config_path = var.kubeconfig_path
  }
}

