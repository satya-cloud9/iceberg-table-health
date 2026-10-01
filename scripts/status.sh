#!/usr/bin/env bash
# Prints what's running (emulator, cluster, per-stage Terraform outputs)
# and the port-forward command for each UI, for whichever PROVIDER/TENANT
# you're pointed at.
#
# Usage: PROVIDER=baremetal|aws|gcp|azure TENANT=tenant-a bash scripts/status.sh
set -uo pipefail

cd "$(dirname "$0")/.."

PROVIDER="${PROVIDER:-aws}"
TENANT="${TENANT:-tenant-a}"

echo "=== Emulator (PROVIDER=$PROVIDER) ==="
case "$PROVIDER" in
  baremetal)
    echo "baremetal: no emulator -- talking directly to the mini PC over SSH."
    ;;
  aws)
    curl -fs http://localhost:4566/_localstack/health >/dev/null 2>&1 \
      && echo "floci: healthy (http://localhost:4566)" \
      || echo "floci: not reachable (run 'PROVIDER=aws make emulator-up')"
    ;;
  gcp)
    curl -fs http://localhost:4588/_localstack/health >/dev/null 2>&1 \
      && echo "floci-gcp: healthy (http://localhost:4588)" \
      || echo "floci-gcp: not reachable (run 'PROVIDER=gcp make emulator-up')"
    ;;
  azure)
    curl -fs http://localhost:4577/_localstack/health >/dev/null 2>&1 \
      && echo "floci-az: healthy (http://localhost:4577)" \
      || echo "floci-az: not reachable (run 'PROVIDER=azure make emulator-up')"
    ;;
  *)
    echo "Unknown PROVIDER='$PROVIDER'" >&2
    ;;
esac

echo ""
echo "=== Terraform stages ==="
for stage in "terraform/providers/${PROVIDER}" "terraform/platform" "terraform/tenants/${TENANT}"; do
  if [ -d "$stage" ] && [ -d "$stage/.terraform" ]; then
    echo "-- $stage --"
    (cd "$stage" && tofu output 2>/dev/null) || echo "(no outputs yet -- not applied)"
  else
    echo "-- $stage -- not initialized (tofu init hasn't been run here yet)"
  fi
  echo ""
done

echo "=== Pods ==="
kubectl get pods -A 2>/dev/null || echo "(no cluster reachable -- check KUBECONFIG, or the kubeconfig_path this provider module wrote out)"

# Object storage moved out of the tenant layer entirely (see CONTRACT.md's
# object-storage outputs) -- it's shared, provider-level infrastructure now,
# not a per-tenant Kubernetes Service, so there's no "svc/minio-console" to
# port-forward anymore (that Service, and the per-tenant MinIO it belonged
# to, were both deleted). Only bare metal has an actual browser console to
# link to (storage.tf's own Docker container, published via
# object_storage_console_endpoint) -- a real cloud provider's object storage
# has its own cloud console, unrelated to anything this repo outputs.
MINIO_CONSOLE_LINE="  (no browser console for PROVIDER=${PROVIDER} -- use that provider's own cloud console instead)"
if [ -d "terraform/providers/${PROVIDER}" ] && [ -d "terraform/providers/${PROVIDER}/.terraform" ]; then
  MINIO_CONSOLE_URL=$(cd "terraform/providers/${PROVIDER}" && tofu output -raw object_storage_console_endpoint 2>/dev/null || true)
  if [ -n "$MINIO_CONSOLE_URL" ]; then
    MINIO_CONSOLE_LINE="  $MINIO_CONSOLE_URL  (minioadmin / minioadmin, unless overridden -- not a Kubernetes Service, no kubectl port-forward needed; SSH-tunnel it instead if you're not on the box itself, e.g. ssh -L 9001:localhost:9001 <ssh_user>@<host>, then open http://localhost:9001 locally)"
  fi
fi

cat <<EOF

=== UI access (run these in separate terminals, then open the URL locally or via an SSH -L tunnel) ===

Trino UI (tenant: ${TENANT}):
  kubectl port-forward -n ${TENANT} svc/trino 8080:8080
  -> http://localhost:8080

Kestra UI (shared, platform namespace):
  kubectl port-forward -n platform svc/kestra 8081:8080
  -> http://localhost:8081  (admin@kestra.io / Kestra2026, unless overridden -- see helm-values/kestra-values.yaml)

Grafana (shared, observability namespace -- ${TENANT}'s dashboards are
auto-discovered from its own folder via the sidecar, see helm-values/kube-prometheus-stack-values.yaml):
  kubectl port-forward -n observability svc/kube-prometheus-stack-grafana 3000:80
  -> http://localhost:3000  (admin / admin)

Object storage console (shared across all tenants -- provider-level, see CONTRACT.md):
${MINIO_CONSOLE_LINE}

If you're SSHing in from your laptop, tunnel these through the same SSH
session instead of running a browser on the box, e.g.:
  ssh -L 8080:localhost:8080 -L 3000:localhost:3000 <user>@<box>
then run the port-forward commands above on the box itself.
EOF
