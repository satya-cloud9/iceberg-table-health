#!/usr/bin/env bash
# Phase 2: start Floci (S3, Glue, DynamoDB, emulated EKS) and wait until healthy.
#
# Usage: bash scripts/02-start-emulator.sh
set -euo pipefail

cd "$(dirname "$0")/.."

PROVIDER="${PROVIDER:-aws}"
COMPOSE_FILE="docker-compose.floci.yml"
CONTAINER="lakehouse-floci"
PORT=4566
HEALTH_PATH="/_localstack/health"

echo "=== Starting $CONTAINER ==="
docker compose -f "$COMPOSE_FILE" up -d

echo "Waiting for $CONTAINER to be healthy..."
ATTEMPTS=0
MAX_ATTEMPTS=30
until curl -fs "http://localhost:${PORT}${HEALTH_PATH}" >/dev/null 2>&1; do
  ATTEMPTS=$((ATTEMPTS + 1))
  if [ "$ATTEMPTS" -ge "$MAX_ATTEMPTS" ]; then
    echo "$CONTAINER did not become healthy after $((MAX_ATTEMPTS * 3))s."
    echo "Run 'docker compose -f $COMPOSE_FILE logs' and paste the output back."
    exit 1
  fi
  sleep 3
done

echo "$CONTAINER is up. Service status:"
curl -s "http://localhost:${PORT}${HEALTH_PATH}" | jq . || curl -s "http://localhost:${PORT}${HEALTH_PATH}"

echo ""
echo "Next: bash scripts/03-apply-provider.sh"
