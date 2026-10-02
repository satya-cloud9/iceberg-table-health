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

# Always use this project's kubeconfig (written by `make provider-apply`).
# An inherited KUBECONFIG is ignored on purpose: a stale export in the shell
# profile pointed these scripts at another project's cluster. To force a
# different one, set GL_KUBECONFIG explicitly.
CONTRACT_JSON="$REPO_ROOT/terraform/generated/aws.tfvars.json"
if [ -n "${GL_KUBECONFIG:-}" ]; then
  KUBECONFIG="$GL_KUBECONFIG"
elif [ -f "$CONTRACT_JSON" ]; then
  KUBECONFIG="$(jq -r '.kubeconfig_path' "$CONTRACT_JSON")"
  case "$KUBECONFIG" in /*) ;; *) KUBECONFIG="$REPO_ROOT/$KUBECONFIG" ;; esac
else
  echo "No $CONTRACT_JSON -- run 'make gl-cluster' first." >&2
  return 1 2>/dev/null || exit 1
fi
if [ ! -f "$KUBECONFIG" ]; then
  echo "Kubeconfig $KUBECONFIG doesn't exist -- re-run 'make gl-cluster'." >&2
  return 1 2>/dev/null || exit 1
fi
export KUBECONFIG AWS_ENDPOINT AWS_REGION WAREHOUSE IMAGE
