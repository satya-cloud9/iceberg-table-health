#!/usr/bin/env bash
# GL0: build the Glue-Lite Spark image and load it straight into the emulated
# EKS nodes' containerd (no registry on Floci). On real EKS (GL4) this becomes
# a push to ECR instead.
set -euo pipefail
source "$(dirname "$0")/env.sh"

echo "=== Building $IMAGE ==="
docker build -f "$GL_ROOT/docker/spark.Dockerfile" -t "$IMAGE" "$REPO_ROOT"

# Floci runs each EKS node as a Docker container named after the node
# (k3s inside, which ships `ctr`). UNVERIFIED against every Floci build:
# if a node name isn't a container here, this stops with what to check.
TAR="$(mktemp --suffix=.tar)"
trap 'rm -f "$TAR"' EXIT
docker save "$IMAGE" -o "$TAR"

for node in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
  if ! docker inspect "$node" >/dev/null 2>&1; then
    echo "Node '$node' is not a local Docker container." >&2
    echo "Run 'docker ps' to find the container behind it, then:" >&2
    echo "  docker exec -i <container> ctr -n k8s.io images import - < image.tar" >&2
    exit 1
  fi
  echo "=== Importing into node $node ==="
  docker exec -i "$node" ctr -n k8s.io images import - < "$TAR" \
    || docker exec -i "$node" k3s ctr -n k8s.io images import - < "$TAR"
done

echo ""
echo "Next: bash flavors/glue-lite/scripts/run-sql-job.sh smoke"
