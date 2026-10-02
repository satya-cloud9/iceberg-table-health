# Shared settings for Glue-Lite scripts. Sourced, not run.
# Every value can be overridden from the environment.

GL_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$GL_ROOT/../.." && pwd)"

# Floci from the host (tofu, aws cli) vs. from inside pods. 172.17.0.1 is the
# Docker bridge gateway; it's what the base repo's aws provider uses for pods
# (terraform/providers/aws/variables.tf: aws_emulator_pod_endpoint).
FLOCI_ENDPOINT="${FLOCI_ENDPOINT:-http://localhost:4566}"
AWS_ENDPOINT="${AWS_ENDPOINT:-http://172.17.0.1:4566}"
AWS_REGION="${AWS_REGION:-us-east-1}"

WAREHOUSE_BUCKET="${WAREHOUSE_BUCKET:-glue-lite-warehouse}"
WAREHOUSE="${WAREHOUSE:-s3://$WAREHOUSE_BUCKET/warehouse}"

IMAGE="${IMAGE:-glue-lite-spark:local}"
SPARK_OPERATOR_VERSION="${SPARK_OPERATOR_VERSION:-2.5.2}"

# Use the project's kubeconfig (written by `make provider-apply`), not
# ~/.kube/config, so this never touches another cluster by accident.
if [ -z "${KUBECONFIG:-}" ] && [ -f "$REPO_ROOT/terraform/generated/aws.tfvars.json" ]; then
  KUBECONFIG="$(jq -r '.kubeconfig_path' "$REPO_ROOT/terraform/generated/aws.tfvars.json")"
  case "$KUBECONFIG" in /*) ;; *) KUBECONFIG="$REPO_ROOT/$KUBECONFIG" ;; esac
fi
export KUBECONFIG AWS_ENDPOINT AWS_REGION WAREHOUSE IMAGE
