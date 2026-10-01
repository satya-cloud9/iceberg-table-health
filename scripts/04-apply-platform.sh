#!/usr/bin/env bash
# Phase 4: apply terraform/platform (the shared control-plane layer --
# ingress/auth, Nessie, Kestra, the Shared OLTP Service, observability),
# fed entirely from whichever provider's contract outputs 03 captured.
# Replaces the old 04-create-kind-cluster.sh + most of 05-deploy-stack.sh
# now that Terraform owns cluster creation (in the provider module) and
# the Helm installs (here and in 05-apply-tenant.sh) directly.
#
# Usage: PROVIDER=baremetal|aws|gcp|azure bash scripts/04-apply-platform.sh
set -euo pipefail

cd "$(dirname "$0")/.."

PROVIDER="${PROVIDER:-aws}"
VARS_FILE="terraform/generated/${PROVIDER}.tfvars.json"

if [ ! -f "$VARS_FILE" ]; then
  echo "$VARS_FILE doesn't exist -- run 'PROVIDER=$PROVIDER bash scripts/03-apply-provider.sh' first." >&2
  exit 1
fi

# Re-run 03-apply-provider.sh's sanity checks here too, not just at the
# end of that script -- cheap, and it catches the cluster having gone
# stale in the gap between running 03 and 04 (a floci container restart,
# a kubeconfig that's aged out, etc.), which would otherwise surface as a
# much less direct error deep inside this module's Kubernetes/Helm
# provider calls below. See 03b-verify-provider.sh's own header comment
# for the incident this whole check exists because of.
PROVIDER="$PROVIDER" bash "$(dirname "$0")/03b-verify-provider.sh"

echo "=== tofu init (terraform/platform) ==="
(cd terraform/platform && tofu init -upgrade)

echo "=== tofu validate (terraform/platform) ==="
(cd terraform/platform && tofu validate)

echo "=== tofu apply (terraform/platform) ==="
echo "(warnings about undeclared variables from ${PROVIDER}.tfvars.json are expected --"
echo " that file carries every provider output, not just the ones this module reads.)"
(cd terraform/platform && tofu apply -auto-approve -var-file="../generated/${PROVIDER}.tfvars.json")

echo ""
echo "=== Capturing platform outputs ==="
(cd terraform/platform && tofu output -json) > terraform/generated/platform-outputs.json
jq 'map_values(.value)' terraform/generated/platform-outputs.json > terraform/generated/platform.tfvars.json

echo "Wrote terraform/generated/platform.tfvars.json:"
cat terraform/generated/platform.tfvars.json

echo ""
echo "Next: PROVIDER=$PROVIDER bash scripts/05-apply-tenant.sh"
