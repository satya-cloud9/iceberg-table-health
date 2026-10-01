#!/usr/bin/env bash
# One-off helper, not part of the numbered pipeline: force-clears
# namespaces stuck in Terminating (a lingering finalizer on some object
# inside, or on the namespace object itself) so scripts/04-apply-platform.sh
# can recreate them cleanly. Not committed to the numbered flow because
# this is a "something's stuck, get unstuck" tool, not a normal pipeline
# step -- run it by hand when a `kubectl delete namespace` hangs or a
# subsequent apply says "object is being deleted".
#
# Usage: KUBECONFIG_PATH=terraform/providers/aws/generated/kubeconfig \
#   bash scripts/unstick-namespaces.sh platform observability
set -euo pipefail

KUBECONFIG_PATH="${KUBECONFIG_PATH:-terraform/providers/aws/generated/kubeconfig}"
NAMESPACES=("$@")
if [ ${#NAMESPACES[@]} -eq 0 ]; then
  echo "Usage: KUBECONFIG_PATH=... bash scripts/unstick-namespaces.sh <ns> [<ns> ...]" >&2
  exit 1
fi

KC=(kubectl --kubeconfig "$KUBECONFIG_PATH")

for NS in "${NAMESPACES[@]}"; do
  echo "=== $NS ==="
  PHASE=$("${KC[@]}" get namespace "$NS" -o jsonpath='{.status.phase}' 2>/dev/null || echo "<gone>")
  echo "phase: $PHASE"
  if [ "$PHASE" != "Terminating" ]; then
    echo "not stuck, skipping"
    continue
  fi

  echo "--- conditions ---"
  "${KC[@]}" get namespace "$NS" -o json | jq '.status.conditions'

  echo "--- clearing finalizers on any namespaced resource still inside $NS ---"
  for TYPE in $("${KC[@]}" api-resources --verbs=list --namespaced -o name 2>/dev/null); do
    NAMES=$("${KC[@]}" get "$TYPE" -n "$NS" -o json 2>/dev/null \
      | jq -r '.items[]? | select((.metadata.finalizers // []) | length > 0) | .metadata.name' || true)
    for NAME in $NAMES; do
      echo "clearing finalizers: $TYPE/$NAME"
      "${KC[@]}" patch "$TYPE" "$NAME" -n "$NS" -p '{"metadata":{"finalizers":[]}}' --type=merge || true
    done
  done

  echo "--- force-finalizing the namespace object itself ---"
  "${KC[@]}" get namespace "$NS" -o json \
    | jq '.spec.finalizers = []' \
    | "${KC[@]}" replace --raw "/api/v1/namespaces/$NS/finalize" -f - || true

  echo "--- final state ---"
  "${KC[@]}" get namespace "$NS" 2>&1 || echo "$NS is gone"
done
