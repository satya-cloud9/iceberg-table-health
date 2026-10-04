#!/usr/bin/env bash
# Phase 0 go/no-go: does Floci's DynamoDB support everything the advisor's
# state store needs? Runs from the host against Floci, with the same
# aws-cli container the other Glue-Lite scripts use, on a scratch table that
# is deleted at the end.
#
#   1  create a table (pk + sk strings, on-demand) and wait for it
#   2  put / get one item
#   3  conditional put: a claim succeeds once, a second claim is refused
#   4  conditional update: an expired claim can be taken over, a live one can't
#   5  atomic counter: UpdateItem ADD, twice
#   6  range query: begins_with and BETWEEN on the sort key
#   7  paging: 30 items, Limit 10, LastEvaluatedKey
#   8  transaction that succeeds: 3 puts + 1 condition check, all written
#   9  transaction that fails: a false condition writes nothing (atomic)
#  10  transaction of 100 items (DynamoDB's per-transaction limit)
#
# Usage: bash flavors/glue-lite/spike/dynamodb-check.sh
# Exit code 0 = all pass. Paste the output back either way.
set -uo pipefail
source "$(dirname "$0")/../scripts/env.sh"

TABLE="advisor_state_check_$(date +%s)"
PASS=0; FAIL=0

aws() {
  docker run --rm --network host \
    -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_REGION="$AWS_REGION" \
    amazon/aws-cli --endpoint-url "$FLOCI_ENDPOINT" --output json "$@"
}
ok()   { echo "PASS  $1"; PASS=$((PASS + 1)); }
bad()  { echo "FAIL  $1"; [ -n "${2:-}" ] && echo "      ${2:0:300}"; FAIL=$((FAIL + 1)); }
key()  { printf '{"pk":{"S":"%s"},"sk":{"S":"%s"}}' "$1" "$2"; }

echo "=== Floci DynamoDB check: table $TABLE at $FLOCI_ENDPOINT ==="

# 1 create + wait
if out=$(aws dynamodb create-table --table-name "$TABLE" \
      --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
      --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
      --billing-mode PAY_PER_REQUEST 2>&1) \
   && aws dynamodb wait table-exists --table-name "$TABLE" >/dev/null 2>&1; then
  ok "1 create table"
else
  bad "1 create table" "$out"; echo "Can't continue without a table."; exit 1
fi

# 2 put / get
aws dynamodb put-item --table-name "$TABLE" \
  --item '{"pk":{"S":"demo#t1"},"sk":{"S":"watermark"},"last_snapshot_id":{"N":"42"}}' >/dev/null 2>&1
got=$(aws dynamodb get-item --table-name "$TABLE" --key "$(key demo#t1 watermark)" 2>&1)
if echo "$got" | grep -q '"42"'; then ok "2 put / get"; else bad "2 put / get" "$got"; fi

# 3 conditional put (claim)
CLAIM='{"pk":{"S":"demo#t1"},"sk":{"S":"claim"},"run_id":{"S":"run-a"},"expires_at":{"N":"9999999999"}}'
if aws dynamodb put-item --table-name "$TABLE" --item "$CLAIM" \
     --condition-expression "attribute_not_exists(pk)" >/dev/null 2>&1; then
  out=$(aws dynamodb put-item --table-name "$TABLE" --item "${CLAIM/run-a/run-b}" \
          --condition-expression "attribute_not_exists(pk)" 2>&1)
  if echo "$out" | grep -q "ConditionalCheckFailed"; then ok "3 conditional put (second claim refused)"
  else bad "3 conditional put: second claim was NOT refused" "$out"; fi
else
  bad "3 conditional put: first claim failed"
fi

# 4 conditional update (take over only an expired claim)
NOW=$(date +%s)
out=$(aws dynamodb update-item --table-name "$TABLE" --key "$(key demo#t1 claim)" \
        --update-expression "SET run_id = :r, expires_at = :e" \
        --condition-expression "expires_at < :now" \
        --expression-attribute-values "{\":r\":{\"S\":\"run-c\"},\":e\":{\"N\":\"$((NOW + 600))\"},\":now\":{\"N\":\"$NOW\"}}" 2>&1)
live_refused=$(echo "$out" | grep -c "ConditionalCheckFailed")
aws dynamodb put-item --table-name "$TABLE" \
  --item "{\"pk\":{\"S\":\"demo#t2\"},\"sk\":{\"S\":\"claim\"},\"run_id\":{\"S\":\"run-a\"},\"expires_at\":{\"N\":\"$((NOW - 60))\"}}" >/dev/null 2>&1
out2=$(aws dynamodb update-item --table-name "$TABLE" --key "$(key demo#t2 claim)" \
        --update-expression "SET run_id = :r, expires_at = :e" \
        --condition-expression "expires_at < :now" \
        --expression-attribute-values "{\":r\":{\"S\":\"run-c\"},\":e\":{\"N\":\"$((NOW + 600))\"},\":now\":{\"N\":\"$NOW\"}}" 2>&1)
if [ "$live_refused" = "1" ] && ! echo "$out2" | grep -q "Error\|error"; then
  ok "4 conditional update (live claim kept, expired claim taken over)"
else
  bad "4 conditional update" "live: $out | expired: $out2"
fi

# 5 atomic counter
for _ in 1 2; do
  aws dynamodb update-item --table-name "$TABLE" --key "$(key demo#t1 hist#lateness_h#2026-10-04#24)" \
    --update-expression "ADD n :one" --expression-attribute-values '{":one":{"N":"1"}}' >/dev/null 2>&1
done
got=$(aws dynamodb get-item --table-name "$TABLE" --key "$(key demo#t1 hist#lateness_h#2026-10-04#24)" 2>&1)
if echo "$got" | grep -q '"2"'; then ok "5 atomic counter (ADD twice = 2)"; else bad "5 atomic counter" "$got"; fi

# 6 range query
for d in 01 02 03 04; do
  aws dynamodb put-item --table-name "$TABLE" \
    --item "{\"pk\":{\"S\":\"demo#t3\"},\"sk\":{\"S\":\"commit#2026-10-$d\"},\"files\":{\"N\":\"1\"}}" >/dev/null 2>&1
done
aws dynamodb put-item --table-name "$TABLE" --item '{"pk":{"S":"demo#t3"},"sk":{"S":"state"}}' >/dev/null 2>&1
c1=$(aws dynamodb query --table-name "$TABLE" --key-condition-expression "pk = :p AND begins_with(sk, :b)" \
       --expression-attribute-values '{":p":{"S":"demo#t3"},":b":{"S":"commit#"}}' --select COUNT 2>&1 | grep -o '"Count": [0-9]*')
c2=$(aws dynamodb query --table-name "$TABLE" --key-condition-expression "pk = :p AND sk BETWEEN :a AND :z" \
       --expression-attribute-values '{":p":{"S":"demo#t3"},":a":{"S":"commit#2026-10-02"},":z":{"S":"commit#2026-10-03"}}' --select COUNT 2>&1 | grep -o '"Count": [0-9]*')
if [ "$c1" = '"Count": 4' ] && [ "$c2" = '"Count": 2' ]; then ok "6 range query (begins_with 4, BETWEEN 2)"
else bad "6 range query" "begins_with: $c1, between: $c2 (want 4 and 2)"; fi

# 7 paging
for i in $(seq -w 1 30); do
  aws dynamodb put-item --table-name "$TABLE" \
    --item "{\"pk\":{\"S\":\"demo#t4\"},\"sk\":{\"S\":\"part#$i\"}}" >/dev/null 2>&1
done
page=$(aws dynamodb query --table-name "$TABLE" --key-condition-expression "pk = :p" \
         --expression-attribute-values '{":p":{"S":"demo#t4"}}' --max-items 10 2>&1)
if echo "$page" | grep -q '"NextToken"\|"LastEvaluatedKey"' && [ "$(echo "$page" | grep -c '"sk"')" = "10" ]; then
  ok "7 paging (10 of 30, with a continuation key)"
else
  bad "7 paging" "$(echo "$page" | tr -d '\n' | cut -c1-300)"
fi

# 8 transaction that succeeds
TX_OK=$(cat <<EOF
[
 {"Put": {"TableName": "$TABLE", "Item": {"pk":{"S":"demo#t5"},"sk":{"S":"commit#1"}}}},
 {"Put": {"TableName": "$TABLE", "Item": {"pk":{"S":"demo#t5"},"sk":{"S":"commit#2"}}}},
 {"Put": {"TableName": "$TABLE", "Item": {"pk":{"S":"demo#t5"},"sk":{"S":"watermark"},"last":{"N":"2"}}}},
 {"ConditionCheck": {"TableName": "$TABLE", "Key": {"pk":{"S":"demo#t1"},"sk":{"S":"claim"}},
   "ConditionExpression": "run_id = :r", "ExpressionAttributeValues": {":r":{"S":"run-a"}}}}
]
EOF
)
out=$(aws dynamodb transact-write-items --transact-items "$TX_OK" 2>&1)
n=$(aws dynamodb query --table-name "$TABLE" --key-condition-expression "pk = :p" \
      --expression-attribute-values '{":p":{"S":"demo#t5"}}' --select COUNT 2>&1 | grep -o '"Count": [0-9]*')
if [ "$n" = '"Count": 3' ]; then ok "8 transaction (3 puts + condition check, all written)"
else bad "8 transaction" "$out | rows: $n (want 3)"; fi

# 9 transaction that fails: nothing written
TX_BAD=$(cat <<EOF
[
 {"Put": {"TableName": "$TABLE", "Item": {"pk":{"S":"demo#t6"},"sk":{"S":"commit#1"}}}},
 {"Put": {"TableName": "$TABLE", "Item": {"pk":{"S":"demo#t6"},"sk":{"S":"watermark"}}}},
 {"ConditionCheck": {"TableName": "$TABLE", "Key": {"pk":{"S":"demo#t1"},"sk":{"S":"claim"}},
   "ConditionExpression": "run_id = :r", "ExpressionAttributeValues": {":r":{"S":"someone-else"}}}}
]
EOF
)
out=$(aws dynamodb transact-write-items --transact-items "$TX_BAD" 2>&1)
n=$(aws dynamodb query --table-name "$TABLE" --key-condition-expression "pk = :p" \
      --expression-attribute-values '{":p":{"S":"demo#t6"}}' --select COUNT 2>&1 | grep -o '"Count": [0-9]*')
if echo "$out" | grep -q "TransactionCanceled\|ConditionalCheckFailed" && [ "$n" = '"Count": 0' ]; then
  ok "9 failed transaction wrote nothing (atomic)"
else
  bad "9 failed transaction" "$out | rows: $n (want 0)"
fi

# 10 transaction of 100 items
TX_100="[$(for i in $(seq 1 100); do
  printf '{"Put":{"TableName":"%s","Item":{"pk":{"S":"demo#t7"},"sk":{"S":"commit#%03d"}}}}' "$TABLE" "$i"
  [ "$i" -lt 100 ] && printf ','
done)]"
out=$(aws dynamodb transact-write-items --transact-items "$TX_100" 2>&1)
n=$(aws dynamodb query --table-name "$TABLE" --key-condition-expression "pk = :p" \
      --expression-attribute-values '{":p":{"S":"demo#t7"}}' --select COUNT 2>&1 | grep -o '"Count": [0-9]*')
if [ "$n" = '"Count": 100' ]; then ok "10 transaction of 100 items"
else bad "10 transaction of 100 items" "$(echo "$out" | tr -d '\n' | cut -c1-300) | rows: $n"; fi

aws dynamodb delete-table --table-name "$TABLE" >/dev/null 2>&1 || true
echo ""
echo "=== $PASS passed, $FAIL failed (scratch table deleted) ==="
[ "$FAIL" = "0" ]
