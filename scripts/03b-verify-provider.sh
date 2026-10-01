#!/usr/bin/env bash
# Phase 3b: sanity-check what 03-apply-provider.sh produced, BEFORE
# terraform/platform or terraform/tenants ever touch it. Runs
# automatically at the end of 03-apply-provider.sh; also safe to re-run
# on its own (e.g. right before 04-apply-platform.sh, if you want to
# double-check nothing's gone stale since 03 last ran).
#
# Why this exists: every real incident chasing AWS's 401s down to floci's
# EKS token webhook rejecting test/test, and then the provider crash that
# followed, would have been caught HERE instead of three-to-five
# resources into a later apply with a much less direct error message.
# Specifically:
#   - `tofu output` after a `-target=...` apply silently prints "No
#     outputs found" ON STDOUT in this tofu build -- so a naive
#     `$(tofu output -raw ...)` captures that warning banner itself as
#     the "value" with no non-zero exit code to catch. That garbage got
#     written into a generated *.tfvars.json file and then signed onto a
#     live AWS API call, crashing the AWS provider outright
#     ("Plugin did not respond" / index-out-of-range panic deep in
#     aws-sdk-go-base's HTTP debug logging).
#   - Before that: aws_eks_cluster.this and the kubeconfig's exec-plugin
#     both defaulted to the shared test/test credentials, which
#     floci-io/floci#2912 deliberately rejects for EKS token-webhook
#     auth -- surfacing as kubectl 401 Unauthorized deep inside
#     terraform/platform's Kubernetes/Helm provider calls, several
#     `tofu apply` invocations away from the actual cause.
# None of these were floci bugs or Terraform bugs -- they were captured
# command output silently standing in for a real value, several steps
# upstream of where the failure actually surfaced. This script's whole
# job is closing that gap: catch a bad value where it was produced, not
# where it finally explodes.
#
# Usage: PROVIDER=baremetal|aws|gcp|azure bash scripts/03b-verify-provider.sh
set -euo pipefail

cd "$(dirname "$0")/.."

PROVIDER="${PROVIDER:-aws}"
CONTRACT_JSON="terraform/generated/${PROVIDER}.tfvars.json"

echo "=== Sanity-checking ${CONTRACT_JSON} ==="

if [ ! -s "$CONTRACT_JSON" ]; then
  echo "ERROR: $CONTRACT_JSON is missing or empty -- run" >&2
  echo "'PROVIDER=$PROVIDER bash scripts/03-apply-provider.sh' first." >&2
  exit 1
fi

# 1. Valid JSON at all. A captured warning/traceback/stack-trace usually
# isn't -- this alone would have caught the incident above immediately,
# rather than three resources into a later, unrelated apply.
if ! jq empty "$CONTRACT_JSON" 2>/dev/null; then
  echo "ERROR: $CONTRACT_JSON isn't valid JSON. Someone's command output" >&2
  echo "(a warning, an error, a stack trace) almost certainly got captured" >&2
  echo "into a value upstream instead of the real thing. Contents:" >&2
  cat "$CONTRACT_JSON" >&2
  exit 1
fi

# 2. No empty-string values, and nothing that looks like a captured CLI
# warning -- ANSI escape codes, or literal phrases these scripts have
# actually produced by accident before (kept as a specific, evidence-based
# blocklist, not a guess at what else might go wrong).
BAD_KEYS=$(jq -r '
  to_entries[]
  | select(.value | type == "string")
  | select(
      (.value == "") or
      (.value | test("\u001b\\[")) or
      (.value | test("No outputs found")) or
      (.value | test("(?i)warning:")) or
      (.value | test("panic:"))
    )
  | .key
' "$CONTRACT_JSON")
if [ -n "$BAD_KEYS" ]; then
  echo "ERROR: $CONTRACT_JSON has empty or garbage-looking values for:" >&2
  echo "$BAD_KEYS" >&2
  echo "(raw file follows)" >&2
  cat "$CONTRACT_JSON" >&2
  exit 1
fi

# 3. The CONTRACT.md-required fields every provider module must produce
# (terraform/providers/CONTRACT.md) are actually present -- catches a
# provider module whose outputs.tf regressed, independent of whether any
# individual value happens to look well-formed.
for key in kubeconfig_path node_pool_refs workload_identity_mechanism service_account_annotations; do
  if ! jq -e "has(\"$key\")" "$CONTRACT_JSON" > /dev/null; then
    echo "ERROR: $CONTRACT_JSON is missing CONTRACT.md's required '$key' output." >&2
    exit 1
  fi
done

echo "OK: $CONTRACT_JSON is well-formed and has every CONTRACT.md field."

# 4. AWS-specific: the kubeconfig's exec-plugin identity isn't the
# test/test pair floci-io/floci#2912 deliberately rejects, and isn't
# empty. Cheap and doesn't need network -- runs before the live kubectl
# probe below so a bad identity gets a specific, actionable message
# instead of a generic "kubectl couldn't connect."
KUBECONFIG_PATH=$(jq -r '.kubeconfig_path' "$CONTRACT_JSON")
if [ ! -f "$KUBECONFIG_PATH" ]; then
  echo "ERROR: kubeconfig_path ($KUBECONFIG_PATH) doesn't exist on disk." >&2
  exit 1
fi

if [ "$PROVIDER" = "aws" ]; then
  EXEC_ACCESS_KEY=$(kubectl config view --kubeconfig "$KUBECONFIG_PATH" --raw \
    -o jsonpath='{.users[0].user.exec.env[?(@.name=="AWS_ACCESS_KEY_ID")].value}' 2>/dev/null || true)
  if [ -z "$EXEC_ACCESS_KEY" ] || [ "$EXEC_ACCESS_KEY" = "test" ]; then
    echo "ERROR: the kubeconfig's exec-plugin AWS_ACCESS_KEY_ID is" >&2
    echo "'${EXEC_ACCESS_KEY:-<empty>}'. floci-io/floci#2912 deliberately" >&2
    echo "rejects the test/test pair for EKS token-webhook auth (see" >&2
    echo "terraform/providers/aws/iam.tf's aws_iam_user.eks_admin header" >&2
    echo "comment) -- an empty value means the eks_admin identity never made" >&2
    echo "it into cluster.tf's null_resource.kubeconfig provisioner. Re-run" >&2
    echo "'PROVIDER=aws bash scripts/03-apply-provider.sh' from a clean slate." >&2
    exit 1
  fi
  echo "OK: kubeconfig exec-plugin identity is real (not test/test, not empty)."
fi

# 5. The decisive check: does the whole chain actually authenticate?
# Everything above is static inspection; this is the one live probe that
# would have caught the 401-Unauthorized incident directly, at the
# source, instead of inside terraform/platform's Kubernetes/Helm provider
# calls several steps later.
echo "--- kubectl get --raw /healthz against $KUBECONFIG_PATH ---"
if ! kubectl --kubeconfig "$KUBECONFIG_PATH" get --raw /healthz; then
  echo "" >&2
  echo "ERROR: kubectl can't authenticate against the cluster $PROVIDER just" >&2
  echo "created. Don't proceed to 04-apply-platform.sh -- it'll fail the same" >&2
  echo "way, just with a less direct error message three resources in." >&2
  echo "Common causes:" >&2
  echo "  - AWS: see the exec-plugin identity check above if it passed but" >&2
  echo "    this still fails -- the identity may be real but lack cluster-" >&2
  echo "    admin (e.g. it wasn't the identity that called CreateCluster)." >&2
  echo "  - A stale kubeconfig left over from a floci container restart --" >&2
  echo "    try a clean-slate reset (docker compose down -v, then wipe" >&2
  echo "    .terraform/, terraform.tfstate*, and generated/ under this" >&2
  echo "    provider module before re-running 03-apply-provider.sh)." >&2
  exit 1
fi
echo ""
echo "OK: cluster is reachable and authenticated."
echo ""
echo "=== All sanity checks passed for PROVIDER=$PROVIDER ==="
