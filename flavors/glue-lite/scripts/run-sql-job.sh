#!/usr/bin/env bash
# Run one SQL file from jobs/sql/ as a SparkApplication, wait for it, print
# the driver log. Usage:
#   bash flavors/glue-lite/scripts/run-sql-job.sh smoke
#   bash flavors/glue-lite/scripts/run-sql-job.sh <name>   # jobs/sql/<name>.sql
set -euo pipefail
source "$(dirname "$0")/env.sh"

command -v envsubst >/dev/null || { echo "envsubst missing: sudo apt-get install -y gettext-base" >&2; exit 1; }

NAME="${1:?usage: run-sql-job.sh <sql-name>}"
[ -f "$GL_ROOT/jobs/sql/$NAME.sql" ] || { echo "No jobs/sql/$NAME.sql" >&2; exit 1; }

export JOB_NAME="${NAME//_/-}-$(date +%Y%m%d-%H%M%S)"
export SQL_FILE="/opt/jobs/sql/$NAME.sql"

echo "=== Submitting $JOB_NAME ($SQL_FILE) ==="
envsubst '${JOB_NAME} ${SQL_FILE} ${IMAGE} ${AWS_ENDPOINT} ${WAREHOUSE} ${AWS_REGION}' \
  < "$GL_ROOT/k8s/sparkapp-sql.tmpl.yaml" | kubectl apply -f -

echo "Waiting for it to finish (Ctrl-C stops waiting, not the job)..."
STATE=""
for _ in $(seq 1 120); do
  STATE="$(kubectl -n spark-jobs get sparkapplication "$JOB_NAME" \
            -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)"
  case "$STATE" in
    COMPLETED|FAILED|SUBMISSION_FAILED|FAILING) break ;;
  esac
  sleep 5
done
echo "State: ${STATE:-unknown}"

echo ""
echo "=== Driver log (job output) ==="
kubectl -n spark-jobs logs "${JOB_NAME}-driver" 2>/dev/null \
  | grep -v -E '^[0-9]{2}/[0-9]{2}/[0-9]{2} [0-9:]+ (INFO|WARN)' || true

if [ "$STATE" != "COMPLETED" ]; then
  echo ""
  echo "Not COMPLETED. Useful next looks:" >&2
  echo "  kubectl -n spark-jobs describe sparkapplication $JOB_NAME" >&2
  echo "  kubectl -n spark-jobs logs ${JOB_NAME}-driver" >&2
  exit 1
fi
