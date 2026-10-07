#!/usr/bin/env bash
# Does Floci's DynamoDB enforce the conditions the run coordinator uses?
# Runs from the host with the same aws-cli container as dynamodb-check.sh, on a
# scratch table deleted at the end, then lists what the advisor's real
# coordination table holds.
#
#   A  one OR condition (attribute_not_exists(pk) OR expires_at < :now OR run_id = :me):
#      a second run must be refused while the first run's claim is live
#   B  the coordinator's three single-clause puts, in turn:
#      B1 first run: attribute_not_exists(pk) succeeds
#      B2 second run: all three refused while the claim is live
#      B3 first run again: run_id = :me succeeds (renewal)
#      B4 claim expired: expires_at < :now lets the second run take it
#   C  release by a non-holder is refused (update with run_id = :me)
#
# Usage: bash flavors/glue-lite/spike/claim-check.sh
set -uo pipefail
source "$(dirname "$0")/../scripts/env.sh"
TABLE="advisor_claim_check_$(date +%s)"
PASS=0; FAIL=0
aws() {
  docker run --rm --network host \
    -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_REGION="$AWS_REGION" \
    amazon/aws-cli --endpoint-url "$FLOCI_ENDPOINT" --output json "$@"
}
ok()  { echo "PASS  $1"; PASS=$((PASS + 1)); }
bad() { echo "FAIL  $1"; [ -n "${2:-}" ] && echo "      ${2:0:300}"; FAIL=$((FAIL + 1)); }
item() {  # item <pk> <run_id> <expires_at>
  printf '{"pk":{"S":"%s"},"sk":{"S":"claim#scan"},"run_id":{"S":"%s"},"expires_at":{"N":"%s"}}' "$1" "$2" "$3"
}
refused() { echo "$1" | grep -q "ConditionalCheckFailed"; }

aws dynamodb create-table --table-name "$TABLE" --billing-mode PAY_PER_REQUEST \
  --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE >/dev/null 2>&1
aws dynamodb wait table-exists --table-name "$TABLE" >/dev/null 2>&1
NOW=$(date +%s); LIVE=$((NOW + 600)); OLD=$((NOW - 60))
OR_COND="attribute_not_exists(pk) OR expires_at < :now OR run_id = :me"

# A: the OR form
aws dynamodb put-item --table-name "$TABLE" --item "$(item a run-1 $LIVE)" --condition-expression "$OR_COND" \
  --expression-attribute-values "{\":now\":{\"N\":\"$NOW\"},\":me\":{\"S\":\"run-1\"}}" >/dev/null 2>&1
out=$(aws dynamodb put-item --table-name "$TABLE" --item "$(item a run-2 $LIVE)" --condition-expression "$OR_COND" \
  --expression-attribute-values "{\":now\":{\"N\":\"$NOW\"},\":me\":{\"S\":\"run-2\"}}" 2>&1)
if refused "$out"; then ok "A  OR condition: second run refused"
else bad "A  OR condition: second run was NOT refused (Floci lets the OR form through)" "$out"; fi

# B: single clauses in turn
put() {  # put <run> <expires> <cond> [values-json]
  if [ -n "${4:-}" ]; then aws dynamodb put-item --table-name "$TABLE" --item "$(item b "$1" "$2")" \
       --condition-expression "$3" --expression-attribute-values "$4" 2>&1
  else aws dynamodb put-item --table-name "$TABLE" --item "$(item b "$1" "$2")" --condition-expression "$3" 2>&1; fi
}
claim() {  # claim <run> <expires> -> prints granted|refused
  local r=$1 e=$2
  refused "$(put "$r" "$e" "attribute_not_exists(pk)")" || { echo granted; return; }
  refused "$(put "$r" "$e" "expires_at < :now" "{\":now\":{\"N\":\"$(date +%s)\"}}")" || { echo granted; return; }
  refused "$(put "$r" "$e" "run_id = :me" "{\":me\":{\"S\":\"$r\"}}")" || { echo granted; return; }
  echo refused
}
[ "$(claim run-1 $LIVE)" = granted ] && ok "B1 first run claims" || bad "B1 first run claims"
[ "$(claim run-2 $LIVE)" = refused ] && ok "B2 second run refused while live" || bad "B2 second run NOT refused"
[ "$(claim run-1 $LIVE)" = granted ] && ok "B3 holder renews (run_id = :me)" || bad "B3 holder could not renew"
aws dynamodb put-item --table-name "$TABLE" --item "$(item b run-1 $OLD)" >/dev/null 2>&1
[ "$(claim run-2 $LIVE)" = granted ] && ok "B4 expired claim taken over" || bad "B4 expired claim not taken over"

# C: release by a non-holder
out=$(aws dynamodb update-item --table-name "$TABLE" --key '{"pk":{"S":"b"},"sk":{"S":"claim#scan"}}' \
  --update-expression "SET expires_at = :z" --condition-expression "run_id = :me" \
  --expression-attribute-values '{":z":{"N":"0"},":me":{"S":"run-1"}}' 2>&1)
refused "$out" && ok "C  release by a non-holder refused" || bad "C  non-holder released the claim" "$out"

aws dynamodb delete-table --table-name "$TABLE" >/dev/null 2>&1
echo "=== $PASS passed, $FAIL failed ==="
echo; echo "=== advisor-coordination items (what the last runs wrote) ==="
aws dynamodb scan --table-name advisor-coordination \
  --projection-expression "pk, sk, run_id, expires_at" --output text 2>&1 | head -40
[ "$FAIL" -eq 0 ]
