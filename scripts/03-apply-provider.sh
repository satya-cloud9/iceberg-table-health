#!/usr/bin/env bash
# Phase 3: apply exactly one terraform/providers/<name> module, then
# capture its four contract outputs (see terraform/providers/CONTRACT.md)
# into terraform/generated/<name>.tfvars.json for the platform/tenant
# stages to consume. This is what actually keeps terraform/platform and
# terraform/tenants provider-agnostic in practice -- they only ever see
# these four values (plus whatever provider-specific extras the identity
# wiring needs), never the provider module itself.
#
# Usage: PROVIDER=baremetal|aws|gcp|azure bash scripts/03-apply-provider.sh
set -euo pipefail

cd "$(dirname "$0")/.."

PROVIDER="${PROVIDER:-aws}"
PROVIDER_DIR="terraform/providers/${PROVIDER}"

if [ ! -d "$PROVIDER_DIR" ]; then
  echo "No such provider module: $PROVIDER_DIR" >&2
  echo "Expected one of: baremetal, aws, gcp, azure" >&2
  exit 1
fi

if [ "$PROVIDER" = "baremetal" ] && [ ! -f "$PROVIDER_DIR/terraform.tfvars" ]; then
  echo "terraform/providers/baremetal/terraform.tfvars doesn't exist yet (it's"
  echo "gitignored -- machine-specific, not source)."
  echo "Copy the committed template and fill in your own values:"
  echo "  cp $PROVIDER_DIR/terraform.tfvars.example $PROVIDER_DIR/terraform.tfvars"
  echo "See terraform/providers/baremetal/variables.tf for every option."
  exit 1
fi

# Option A (see terraform/providers/CONTRACT.md's "Known gap" section and
# each provider's own workload-identity.tf) needs to know every tenant
# that exists BEFORE this provider module applies -- that's the one real
# cost of native federation over the shared static credential: onboarding a
# tenant now touches the provider layer too, not just that tenant's own
# apply (see workload-identity.tf's own header comments on why -- the
# trust binding has to be built unilaterally, inside the provider's own
# phase, using nothing it'd otherwise have to read from platform/tenant).
#
# Discovered here, not hand-maintained: every terraform/tenants/<name>
# directory except _template (which is a module source, never applied on
# its own -- see that directory's own README/header comments) is a real
# tenant. Written as PROVIDER_DIR/tenant_ids.auto.tfvars.json, a plain
# Terraform auto-loaded var-file (the .auto.tfvars.json suffix is what
# makes tofu pick it up with no -var-file flag needed) -- gitignored,
# regenerated fresh on every run of this script, the same "generated, not
# committed" treatment terraform/generated/*.tfvars.json already gets.
# for_each in workload-identity.tf keys off this list directly; an empty
# list (no terraform/tenants/* directories yet) makes every resource
# there for_each = {}, a clean no-op, not an error.
echo "=== Discovering tenants for Option A (workload-identity.tf) ==="
TENANT_IDS_JSON="[]"
if [ -d "terraform/tenants" ]; then
  TENANT_IDS_JSON=$(
    find terraform/tenants -mindepth 1 -maxdepth 1 -type d -printf '%f\n' \
      | grep -v '^_template$' \
      | sort \
      | jq -R . \
      | jq -s -c .
  )
fi
echo "Tenants found: $TENANT_IDS_JSON"
echo "{\"tenant_ids\": $TENANT_IDS_JSON}" > "$PROVIDER_DIR/tenant_ids.auto.tfvars.json"

echo "=== tofu init ($PROVIDER_DIR) ==="
(cd "$PROVIDER_DIR" && tofu init -upgrade)

echo "=== tofu validate ($PROVIDER_DIR) ==="
(cd "$PROVIDER_DIR" && tofu validate)

# AWS-only: pre-provision a real (non-test/test) IAM identity before the
# main apply touches aws_eks_cluster.this. Root cause, confirmed against
# floci's own docs/source (floci-io/floci#2912): floci's EKS
# token-authentication webhook deliberately rejects the test/test
# credential pair, so bootstrap_cluster_creator_admin_permissions
# (terraform/providers/aws/cluster.tf) has to be granted to a real
# identity or kubectl gets 401 Unauthorized against every cluster this
# module creates -- see terraform/providers/aws/iam.tf's
# aws_iam_user.eks_admin header comment for the full story.
#
# This has to be a separate, earlier apply, not just a resource ordered
# via depends_on: Terraform requires provider configuration (versions.tf's
# provider "aws" "eks_admin" block) to be fully known before any resource
# in that provider's scope is planned, and an access key generated in the
# same apply isn't known that early. So: apply just the access key here,
# read it back out, write it as an auto.tfvars.json var-file, then the
# real apply below picks it up like any other input variable -- no
# resource-depends-on-provider antipattern involved.
if [ "$PROVIDER" = "aws" ]; then
  echo "=== Pre-provisioning the real (non-test) EKS admin identity ($PROVIDER_DIR) ==="
  (cd "$PROVIDER_DIR" && tofu apply -auto-approve -target=aws_iam_access_key.eks_admin)

  # NOT `tofu output` -- confirmed empirically, not assumed: `-target`
  # prunes the entire outputs graph unless the outputs themselves are
  # ALSO explicitly targeted, so `tofu output` right after a
  # -target=aws_iam_access_key.eks_admin apply reports "No outputs found"
  # even though that resource is right there in state. Worse, that
  # warning text lands on stdout in this tofu build, so a naive
  # `$(tofu output -raw ...)` silently captures the warning banner itself
  # as the "value" -- which is exactly how eks_admin.auto.tfvars.json
  # ended up holding ANSI warning text instead of a real access key,
  # which then made aws_eks_cluster.this sign its CreateCluster call with
  # garbage and crash the AWS provider outright (see the guard below for
  # what that looks like when it happens). Reading the resource's own
  # attributes straight out of state instead has no such caveat -- no
  # output evaluation involved at all.
  EKS_ADMIN_ACCESS_KEY_ID=$(cd "$PROVIDER_DIR" && tofu show -json | jq -r '.values.root_module.resources[] | select(.address == "aws_iam_access_key.eks_admin") | .values.id')
  EKS_ADMIN_SECRET_ACCESS_KEY=$(cd "$PROVIDER_DIR" && tofu show -json | jq -r '.values.root_module.resources[] | select(.address == "aws_iam_access_key.eks_admin") | .values.secret')

  # Fail loudly here, not three resources later as an opaque "Plugin did
  # not respond" crash inside the AWS provider's own HTTP debug-logging
  # code (confirmed: that's what an empty access_key/secret_key produces
  # when aws_eks_cluster.this signs its CreateCluster call with them --
  # aws-sdk-go-base's authorizationHeaderAttribute panics on the resulting
  # malformed Authorization header, "index out of range [1] with length
  # 1"). If you hit that crash, it means this check didn't run -- you
  # likely called `tofu apply` directly in $PROVIDER_DIR instead of
  # through this script.
  if [ -z "$EKS_ADMIN_ACCESS_KEY_ID" ] || [ "$EKS_ADMIN_ACCESS_KEY_ID" = "null" ] \
    || [ -z "$EKS_ADMIN_SECRET_ACCESS_KEY" ] || [ "$EKS_ADMIN_SECRET_ACCESS_KEY" = "null" ]; then
    echo "ERROR: couldn't read aws_iam_access_key.eks_admin's id/secret out of" >&2
    echo "'tofu show -json' after the targeted apply above (empty or jq's" >&2
    echo "literal \"null\" -- the latter means the resource wasn't found at" >&2
    echo "that jq path, e.g. address/module changed). aws_eks_cluster.this" >&2
    echo "would sign its CreateCluster call with garbage credentials otherwise," >&2
    echo "which crashes the AWS provider outright rather than failing cleanly." >&2
    echo "Check 'tofu state show aws_iam_access_key.eks_admin' in $PROVIDER_DIR." >&2
    exit 1
  fi

  jq -n \
    --arg id "$EKS_ADMIN_ACCESS_KEY_ID" \
    --arg secret "$EKS_ADMIN_SECRET_ACCESS_KEY" \
    '{eks_admin_access_key_id: $id, eks_admin_secret_access_key: $secret}' \
    > "$PROVIDER_DIR/eks_admin.auto.tfvars.json"
fi

echo "=== tofu apply ($PROVIDER_DIR) ==="
(cd "$PROVIDER_DIR" && tofu apply -auto-approve)

echo ""
echo "=== Capturing the provider contract outputs ==="
mkdir -p terraform/generated
(cd "$PROVIDER_DIR" && tofu output -json) > "terraform/generated/${PROVIDER}-outputs.json"

# The four required contract outputs, plus every provider-specific extra
# (trino_gsa_email, etc.) -- terraform/platform and
# terraform/tenants only declare the variables they actually use, so
# passing extras through as a var-file is harmless. This is a plain
# var-file, not *.auto.tfvars.json -- it lives outside terraform/platform
# and terraform/tenants/<name>, so it has to be passed explicitly with
# -var-file (04/05 do this) rather than relying on Terraform's
# same-directory auto-loading.
jq 'map_values(.value)' "terraform/generated/${PROVIDER}-outputs.json" > "terraform/generated/${PROVIDER}.tfvars.json"

echo "Wrote terraform/generated/${PROVIDER}.tfvars.json:"
cat "terraform/generated/${PROVIDER}.tfvars.json"

echo ""
PROVIDER="$PROVIDER" bash scripts/03b-verify-provider.sh

echo ""
echo "Next: bash scripts/04-apply-platform.sh -- PROVIDER=${PROVIDER}"
