#!/usr/bin/env bash
# Run two (or more) profile groups at the same time, each as its own Spark job,
# then show which run scanned each table (make gl-coverage).
#
#   run-groups-parallel.sh group_a group_b [-- extra gl_scan args, e.g. --full]
#
# The spark-jobs quota is 8Gi of requests; one default-sized job is ~4.4Gi, so
# parallel runs use smaller pods (override DRIVER_MEMORY / EXECUTOR_MEMORY /
# EXECUTOR_OVERHEAD to change). Logs: /tmp/gl-group-<group>.log
set -euo pipefail
cd "$(dirname "$0")/../../.."
GROUPS_=(); EXTRA=()
while [ $# -gt 0 ]; do
  if [ "$1" = "--" ]; then shift; EXTRA=("$@"); break; fi
  GROUPS_+=("$1"); shift
done
[ ${#GROUPS_[@]} -ge 2 ] || { echo "usage: run-groups-parallel.sh <group> <group> [...] [-- gl_scan args]" >&2; exit 1; }

export DRIVER_MEMORY="${DRIVER_MEMORY:-1g}" EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-1g}" EXECUTOR_OVERHEAD="${EXECUTOR_OVERHEAD:-512m}"
pids=()
for g in "${GROUPS_[@]}"; do
  echo "=== starting group $g (log /tmp/gl-group-$g.log) ==="
  bash flavors/glue-lite/scripts/run-job.sh py gl_scan.py --group "$g" "${EXTRA[@]}" > "/tmp/gl-group-$g.log" 2>&1 &
  pids+=($!)
  sleep 3          # distinct SparkApplication names (they carry a timestamp to the second)
done
rc=0
for i in "${!pids[@]}"; do
  if wait "${pids[$i]}"; then echo "=== group ${GROUPS_[$i]}: job finished ==="; else echo "=== group ${GROUPS_[$i]}: job FAILED ==="; rc=1; fi
done
for g in "${GROUPS_[@]}"; do
  echo; echo "--- $g ---"
  grep -E "^=== Group|housekeeping|tables in|=== Timing|skip  |PASS|FAIL|STALE|retry" "/tmp/gl-group-$g.log" || true
done
echo
make -s gl-coverage RUNS=${#GROUPS_[@]}
exit $rc
