variable "aws_region" {
  description = "Region used for all resources (floci accepts any valid AWS region name)."
  type        = string
  default     = "us-east-1"
}

variable "aws_emulator_endpoint" {
  description = "floci (AWS emulator) edge endpoint. Default assumes floci running on the same host via docker compose (PROVIDER=aws scripts/02-start-emulator.sh)."
  type        = string
  default     = "http://localhost:4566"
}

variable "project_name" {
  description = "Prefix applied to all resource names."
  type        = string
  default     = "lakehouse"
}

variable "vpc_cidr" {
  type    = string
  default = "10.42.0.0/16"
}

variable "storage_bucket_name" {
  description = "S3 bucket name. Originally a parity-only resource (validates the HCL against AWS's real API shape); now the real, shared object store every tenant's Iceberg data lives in -- see s3.tf and CONTRACT.md's object-storage outputs."
  type        = string
  default     = "lakehouse-aws-parity"
}

variable "cluster_name" {
  description = "Name for the EKS cluster (floci) and the kubeconfig context. cluster.tf used to build this from `kind` directly -- see versions.tf's header comment for why it's floci's own EKS API now."
  type        = string
  default     = "lakehouse-aws"
}

variable "kubernetes_version" {
  description = "EKS version passed to aws_eks_cluster. floci's docs list supported-version-to-k3s-image mappings starting at 1.28; 1.29 is a mid-range pick, not verified as optimal against floci specifically."
  type        = string
  default     = "1.29"
}

variable "node_group_desired_size" {
  description = "aws_eks_node_group's scaling_config.desired_size. 2 loosely matches the old kind setup's 2 worker nodes (kind additionally modeled the control plane as its own node; real EKS -- and floci's emulation of it -- doesn't expose the control plane as a node at all, so node_pool_refs now reflects one managed node group, not three named nodes)."
  type        = number
  default     = 2
}

variable "platform_namespace" {
  description = "Kubernetes namespace terraform/platform creates Nessie into -- needed here only for workload-identity.tf's Nessie Pod Identity association (namespace + ServiceAccount name is the naming convention both sides agree to independently, per CONTRACT.md's \"Known gap\" section). Keep this in sync with terraform/platform/variables.tf's own platform_namespace default if you ever change one."
  type        = string
  default     = "platform"
}

variable "tenant_ids" {
  description = "Every tenant this provider module should build a per-tenant native-federation identity for (Option A -- see workload-identity.tf). Empty by default: nothing here creates per-tenant IAM objects until you list tenants explicitly. Keep in sync with terraform/tenants/*'s own directory names (excluding _template) -- scripts/03-apply-provider.sh discovers this list for you now, see its own header comment."
  type        = list(string)
  default     = []
}

variable "enable_native_workload_identity" {
  description = "Opt-in switch for Option A (see workload-identity.tf and terraform/providers/CONTRACT.md's \"Native federation (Option A)\" section). false (default): workload_identity_mechanism stays \"static-secret\", nothing about AWS's current behavior changes. true: workload_identity_mechanism flips to whichever mechanism var.aws_workload_identity_mechanism names, built in workload-identity.tf."
  type        = bool
  default     = false
}

variable "aws_workload_identity_mechanism" {
  description = "Which native-federation mechanism AWS uses when var.enable_native_workload_identity is true (ignored otherwise -- see workload-identity.tf's header comment for the full story on both). \"irsa\" (default as of 2026-09-24): classic OIDC federation via aws_iam_openid_connect_provider -- no node-side relay, so it doesn't touch the component confirmed broken in floci. \"pod-identity\": EKS Pod Identity -- Terraform/IAM-side wiring confirmed correct, but confirmed BLOCKED against the current floci build (its credential-delivery relay fails to install: \"No supported package manager found for IMDS proxy dependencies\"). Kept available, gated off by default, for when floci ships a fix."
  type        = string
  default     = "irsa"

  validation {
    condition     = contains(["irsa", "pod-identity"], var.aws_workload_identity_mechanism)
    error_message = "aws_workload_identity_mechanism must be \"irsa\" or \"pod-identity\"."
  }
}

variable "eks_admin_access_key_id" {
  description = "Access key ID for the real (non-test/test) IAM identity that creates the EKS cluster and authenticates kubectl against it -- see iam.tf's aws_iam_user.eks_admin header comment for why this has to be a real identity, not the shared test/test pair. Populated by scripts/03-apply-provider.sh's preliminary `tofu apply -target=aws_iam_access_key.eks_admin` pass, written to eks_admin.auto.tfvars.json (gitignored, regenerated every run -- same treatment as tenant_ids.auto.tfvars.json). Empty default so a first `tofu validate`/`tofu plan`, before that pass has ever run, doesn't hard-fail."
  type        = string
  default     = ""
}

variable "eks_admin_secret_access_key" {
  description = "Secret access key paired with var.eks_admin_access_key_id. Same sourcing as above."
  type        = string
  sensitive   = true
  default     = ""
}

variable "aws_emulator_pod_endpoint" {
  description = <<-EOT
    UNVERIFIED -- check this on first apply rather than trusting the
    default. var.aws_emulator_endpoint (http://localhost:4566) is what
    THIS Terraform process uses to create the S3 bucket, running on the
    host. Pods running inside the kind cluster are separate Docker
    containers, typically on a different Docker network than floci's
    compose network, so "localhost" from inside a pod does not reach the
    host's floci container the way it does from the host's own shell.
    172.17.0.1 is Linux Docker's default bridge gateway IP, which often
    (not always -- depends on your Docker network config) lets a
    container reach a service published on the host. Confirm with a
    quick pod-level curl against this value before trusting Nessie/Trino
    to reach it; if it's wrong, floci and kind may need to share an
    explicit Docker network instead.
  EOT
  type        = string
  default     = "http://172.17.0.1:4566"
}

