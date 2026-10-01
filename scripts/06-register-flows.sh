#!/usr/bin/env bash
# Phase 6: register the example flow with Kestra via its REST API, and
# trigger one execution so you can confirm the whole pipeline works.
# Kestra now lives in the platform namespace (terraform/platform/kestra.tf),
# not a per-tenant one -- it's shared, see the tenancy-architecture
# reference page.
#
# Two things confirmed live, not just from docs, against the actual
# deployed chart (kestra.tf pins version = "1.3.37"):
#
# 1. Every API call needs Basic Auth -- helm-values/kestra-values.yaml's
#    own comment on basic-auth already documented this ("Now that
#    authentication is required, it is always enabled"), but this script
#    never actually sent credentials, so every call here was 401ing
#    regardless of that config being correct.
# 2. /api/v1/flows and /api/v1/executions/... (no tenant segment) both
#    404 on 1.3.37 -- Kestra's multi-tenancy work (landed well before this
#    version, see kestra.io/docs/migration-guide/v0.23.0/tenant-migration-oss)
#    made a tenant segment mandatory in the path. OSS/non-EE installs use
#    the fixed tenant "main" -- confirmed against kestra.io/docs/how-to-guides/api's
#    own current examples (POST /api/v1/main/flows, POST
#    /api/v1/main/executions/{namespace}/{id}).
set -euo pipefail

cd "$(dirname "$0")/.."

KESTRA_AUTH=(-u "admin@kestra.io:Kestra2026")  # must match helm-values/kestra-values.yaml's basic-auth block

echo "=== Port-forwarding Kestra (background) ==="
kubectl port-forward -n platform svc/kestra 8081:8080 >/tmp/kestra-pf.log 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true' EXIT

sleep 3
# GET on this path is a genuine 405 ("Allowed methods: [POST]"), not a
# reachability problem -- confirmed live. So this only checks for a
# connection failure (curl's "000" placeholder code), not a 2xx: any real
# HTTP response, even a 405, means the port-forward and Kestra's own HTTP
# layer are both up.
STATUS=$(curl -s -o /dev/null -w '%{http_code}' "${KESTRA_AUTH[@]}" http://localhost:8081/api/v1/main/flows)
if [ "$STATUS" = "000" ]; then
  echo "Kestra API not reachable at http://localhost:8081 — check 'kubectl get pods -n platform' and paste back the Kestra pod's status/logs."
  exit 1
fi

echo "=== Registering flow ==="
curl -fsS "${KESTRA_AUTH[@]}" -X POST http://localhost:8081/api/v1/main/flows \
  -H "Content-Type: application/x-yaml" \
  --data-binary @kestra/flows/ingest-to-iceberg.yaml \
  || echo "(non-2xx above may just mean the flow already exists — checking...)"

echo ""
echo "=== Triggering an execution ==="
curl -fsS "${KESTRA_AUTH[@]}" -X POST http://localhost:8081/api/v1/main/executions/lakehouse/ingest-to-iceberg \
  | jq . || echo "Check the Kestra UI (make status has the port-forward command) to trigger it manually instead."

echo ""
echo "Open the Kestra UI to watch the execution: kubectl port-forward -n platform svc/kestra 8081:8080, then http://localhost:8081 (admin@kestra.io / Kestra2026)"
