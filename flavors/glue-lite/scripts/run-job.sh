#!/usr/bin/env bash
# Run one Glue-Lite job as a SparkApplication, wait for it, print the driver
# output. Two forms:
#
#   run-job.sh sql <name>                # jobs/sql/<name>.sql via run_sql.py
#   run-job.sh py <script.py> [args...]  # jobs/<script.py> with its own args
#
# e.g.  run-job.sh sql smoke
#       run-job.sh py generate_small_files.py --recreate --commits 30
#
# JOB_TIMEOUT_MIN (default 30) caps how long this waits; the job itself keeps
# running if the wait gives up.
# Sizing (override per run): DRIVER_MEMORY (1g), EXECUTOR_MEMORY (2g),
# EXECUTOR_OVERHEAD (1g), EXECUTOR_INSTANCES (1), JOB_TTL_SECONDS (86400:
# how long a finished application and its driver pod are kept). Executor pod = memory +
# overhead; the spark-jobs quota is 8Gi of requests in total.
set -euo pipefail
source "$(dirname "$0")/env.sh"

command -v envsubst >/dev/null || { echo "envsubst missing: sudo apt-get install -y gettext-base" >&2; exit 1; }

KIND="${1:?usage: run-job.sh sql <name> | run-job.sh py <script.py> [args...]}"
shift
case "$KIND" in
  sql)
    NAME="${1:?usage: run-job.sh sql <name>}"
    [ -f "$GL_ROOT/jobs/sql/$NAME.sql" ] || { echo "No jobs/sql/$NAME.sql" >&2; exit 1; }
    export MAIN_FILE="/opt/jobs/run_sql.py"
    ARGS=("/opt/jobs/sql/$NAME.sql")
    ;;
  py)
    SCRIPT="${1:?usage: run-job.sh py <script.py> [args...]}"
    shift
    [ -f "$GL_ROOT/jobs/$SCRIPT" ] || { echo "No jobs/$SCRIPT" >&2; exit 1; }
    NAME="${SCRIPT%.py}"
    export MAIN_FILE="/opt/jobs/$SCRIPT"
    ARGS=("$@")
    ;;
  *)
    echo "Unknown kind '$KIND' -- use 'sql' or 'py'." >&2; exit 1 ;;
esac

# Kubernetes names: lowercase, '-' not '_', short enough with the timestamp.
SLUG="$(echo "${NAME//_/-}" | tr '[:upper:]' '[:lower:]' | cut -c1-30)"
export JOB_NAME="${SLUG}-$(date +%Y%m%d-%H%M%S)"

# Render args as YAML list lines; JSON-quote each so values with spaces,
# commas or colons stay intact.
JOB_ARGS=""
for arg in "${ARGS[@]}"; do
  JOB_ARGS+="    - $(printf '%s' "$arg" | jq -Rs .)"$'\n'
done
[ -n "$JOB_ARGS" ] || JOB_ARGS="    []"$'\n'
export JOB_ARGS="${JOB_ARGS%$'\n'}"

export DRIVER_MEMORY="${DRIVER_MEMORY:-1g}"
export EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-2g}"
export EXECUTOR_OVERHEAD="${EXECUTOR_OVERHEAD:-1g}"
export EXECUTOR_INSTANCES="${EXECUTOR_INSTANCES:-1}"
export JOB_TTL_SECONDS="${JOB_TTL_SECONDS:-86400}"   # finished jobs are cleaned up after a day

# Backends for this run (STATE_BACKEND / LOGS_BACKEND = iceberg | postgres; unset:
# the config decides) and the Postgres password when make gl-pg-up has run.
PG_CONF=""
if [ -n "${STATE_BACKEND:-}" ]; then
  PG_CONF+="    spark.kubernetes.driverEnv.GL_STATE_BACKEND: \"${STATE_BACKEND}\""$'\n'
fi
if [ -n "${LOGS_BACKEND:-}" ]; then
  PG_CONF+="    spark.kubernetes.driverEnv.GL_LOGS_BACKEND: \"${LOGS_BACKEND}\""$'\n'
fi
if kubectl -n spark-jobs get secret advisor-pg >/dev/null 2>&1; then
  PG_CONF+="    spark.kubernetes.driver.secretKeyRef.GL_PG_PASSWORD: advisor-pg:password"$'\n'
elif [[ "${STATE_BACKEND:-} ${LOGS_BACKEND:-}" == *postgres* ]]; then
  echo "A backend is postgres but there is no advisor-pg Secret: run make gl-pg-up first." >&2
  exit 1
fi
export PG_CONF="${PG_CONF%$'\n'}"

echo "=== Submitting $JOB_NAME ($MAIN_FILE ${ARGS[*]:-}) [executor ${EXECUTOR_INSTANCES} x ${EXECUTOR_MEMORY}+${EXECUTOR_OVERHEAD}] ==="
envsubst '${JOB_NAME} ${MAIN_FILE} ${JOB_ARGS} ${IMAGE} ${AWS_ENDPOINT} ${WAREHOUSE} ${AWS_REGION} ${DRIVER_MEMORY} ${EXECUTOR_MEMORY} ${EXECUTOR_OVERHEAD} ${EXECUTOR_INSTANCES} ${JOB_TTL_SECONDS} ${PG_CONF}' \
  < "$GL_ROOT/k8s/sparkapp.tmpl.yaml" | kubectl apply -f -

TIMEOUT_MIN="${JOB_TIMEOUT_MIN:-30}"
echo "Waiting up to ${TIMEOUT_MIN} min (Ctrl-C stops waiting, not the job)..."
STATE=""
for _ in $(seq 1 $((TIMEOUT_MIN * 12))); do
  STATE="$(kubectl -n spark-jobs get sparkapplication "$JOB_NAME" \
            -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)"
  case "$STATE" in
    COMPLETED|FAILED|SUBMISSION_FAILED) break ;;
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
