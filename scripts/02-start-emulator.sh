#!/usr/bin/env bash
# Phase 2: start the control-plane emulator for whichever cloud provider
# you're testing against (skipped entirely for bare metal -- there's no
# cloud API to emulate there). Replaces the old 02-start-floci.sh now that
# this repo supports more than one provider.
#
# Usage: PROVIDER=aws|gcp|azure|baremetal bash scripts/02-start-emulator.sh
set -euo pipefail

cd "$(dirname "$0")/.."

PROVIDER="${PROVIDER:-aws}"

case "$PROVIDER" in
  baremetal)
    echo "PROVIDER=baremetal -- nothing to emulate, there's no cloud API here. Skipping."
    exit 0
    ;;
  aws)
    COMPOSE_FILE="docker-compose.floci.yml"
    CONTAINER="lakehouse-floci"
    PORT=4566
    HEALTH_PATH="/_localstack/health"
    ;;
  gcp)
    COMPOSE_FILE="docker-compose.floci-gcp.yml"
    CONTAINER="lakehouse-floci-gcp"
    PORT=4588
    HEALTH_PATH="/health"
    ;;
  azure)
    COMPOSE_FILE="docker-compose.floci-az.yml"
    CONTAINER="lakehouse-floci-az"
    PORT=4577
    HEALTH_PATH="/_localstack/health"
    ;;
  *)
    echo "Unknown PROVIDER='$PROVIDER' -- expected one of: baremetal, aws, gcp, azure" >&2
    exit 1
    ;;
esac

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
    if [ "$PROVIDER" != "aws" ]; then
      echo "(If this is failing specifically on the health check path, see the"
      echo " caveat comment in $COMPOSE_FILE -- floci-gcp/floci-az may not share"
      echo " floci's /_localstack/health convention.)"
    fi
    exit 1
  fi
  sleep 3
done

echo "$CONTAINER is up. Service status:"
curl -s "http://localhost:${PORT}${HEALTH_PATH}" | jq . || curl -s "http://localhost:${PORT}${HEALTH_PATH}"

echo ""
echo "Next: PROVIDER=$PROVIDER bash scripts/03-apply-provider.sh"
