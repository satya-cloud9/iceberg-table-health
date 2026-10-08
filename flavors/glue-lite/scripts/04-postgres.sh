#!/usr/bin/env bash
# Postgres for the advisor's state and logs on the homelab (make gl-pg-up).
# Creates the advisor-pg Secret once (a random password, kept on re-runs) in
# advisor-db and spark-jobs, applies k8s/postgres.yaml and waits until the pod
# is ready. Safe to re-run: the data volume and the password stay.
#
# Jobs use it when a backend is postgres: config state.backend / logs.backend,
# or for one run: make gl-scan STATE_BACKEND=postgres LOGS_BACKEND=postgres
set -euo pipefail
source "$(dirname "$0")/env.sh"

kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: advisor-db
EOF

if kubectl -n advisor-db get secret advisor-pg >/dev/null 2>&1; then
  PASSWORD="$(kubectl -n advisor-db get secret advisor-pg -o jsonpath='{.data.password}' | base64 -d)"
  echo "=== advisor-pg Secret exists (password kept) ==="
else
  PASSWORD="$(head -c 24 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 24)"
  echo "=== advisor-pg Secret created ==="
fi
for ns in advisor-db spark-jobs; do
  kubectl -n "$ns" create secret generic advisor-pg --from-literal=password="$PASSWORD" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
done

echo "=== Postgres (advisor-db/advisor-pg) ==="
kubectl apply -f "$GL_ROOT/k8s/postgres.yaml"
kubectl -n advisor-db rollout status deployment/advisor-pg --timeout=5m
kubectl -n advisor-db exec deploy/advisor-pg -- psql -U advisor -d advisor -tAc "SELECT 'ready: ' || version()"
echo ""
echo "Use it for one run:   make gl-scan STATE_BACKEND=postgres LOGS_BACKEND=postgres"
echo "or for every run:     state.backend / logs.backend = postgres in jobs/config/health.json (then make gl-image)"
echo "Query it:             make gl-pg-sql Q=\"SELECT scan_id, count(*) FROM advisor.table_metrics GROUP BY 1\""
