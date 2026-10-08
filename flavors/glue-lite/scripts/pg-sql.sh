#!/usr/bin/env bash
# psql against the homelab Postgres (make gl-pg-sql Q="..."). Q comes from the
# environment so quotes in the query pass through untouched.
set -euo pipefail
source "$(dirname "$0")/env.sh"
Q="${Q:-SELECT relname AS table_name, n_live_tup AS rows FROM pg_stat_user_tables WHERE schemaname = 'advisor' ORDER BY relname}"
kubectl -n advisor-db exec -i deploy/advisor-pg -- psql -U advisor -d advisor -P pager=off -v ON_ERROR_STOP=1 -c "$Q"
