#!/usr/bin/env bash
# Phase 5: apply one terraform/tenants/<name> workspace -- Trino, MinIO, a
# per-tenant Grafana folder, and either a pool-tier schema or a dedicated
# Postgres instance, per that tenant's isolation_tier. Fed from both the
# provider's contract outputs (03) and the platform's outputs (04).
#
# Adding a second tenant means a new terraform/tenants/<name>/ directory
# (copy tenant-a's, change tenant_id) and TENANT=<name> here -- never
# editing terraform/tenants/_template.
#
# Usage: PROVIDER=baremetal|aws|gcp|azure TENANT=tenant-a bash scripts/05-apply-tenant.sh
set -euo pipefail

cd "$(dirname "$0")/.."

PROVIDER="${PROVIDER:-aws}"
TENANT="${TENANT:-tenant-a}"
PROVIDER_VARS="terraform/generated/${PROVIDER}.tfvars.json"
PLATFORM_VARS="terraform/generated/platform.tfvars.json"
TENANT_DIR="terraform/tenants/${TENANT}"

for f in "$PROVIDER_VARS" "$PLATFORM_VARS"; do
  if [ ! -f "$f" ]; then
    echo "$f doesn't exist -- run scripts/03-apply-provider.sh and scripts/04-apply-platform.sh first." >&2
    exit 1
  fi
done

if [ ! -d "$TENANT_DIR" ]; then
  echo "No such tenant workspace: $TENANT_DIR" >&2
  exit 1
fi

echo "=== tofu init ($TENANT_DIR) ==="
(cd "$TENANT_DIR" && tofu init -upgrade)

echo "=== tofu validate ($TENANT_DIR) ==="
(cd "$TENANT_DIR" && tofu validate)

VAR_FILE_ARGS=(
  -var-file="../../generated/${PROVIDER}.tfvars.json"
  -var-file="../../generated/platform.tfvars.json"
)

echo "=== tofu apply ($TENANT_DIR) ==="
(cd "$TENANT_DIR" && tofu apply -auto-approve "${VAR_FILE_ARGS[@]}")

echo ""
echo "=== Outputs ==="
(cd "$TENANT_DIR" && tofu output)

echo ""
echo "Next: make flows, then make status"

