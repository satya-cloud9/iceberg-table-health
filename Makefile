SHELL := /bin/bash

# This repo is the Glue-Lite flavor only: Floci (S3, Glue) + Floci-emulated
# EKS + Spark Operator. The provider is pinned to the emulated AWS module.
override PROVIDER := aws
export PROVIDER

.PHONY: preflight install emulator-up provider-apply teardown destroy

preflight:
	bash scripts/00-preflight.sh

install:
	bash scripts/01-install-deps.sh

emulator-up:
	bash scripts/02-start-emulator.sh

provider-apply:
	bash scripts/03-apply-provider.sh

# Stops Floci and the emulated cluster; leaves Terraform state alone.
teardown:
	bash scripts/99-teardown.sh

# Runs `tofu destroy` on the provider module, then stops Floci.
destroy:
	DESTROY=1 bash scripts/99-teardown.sh

# --- Glue-Lite flavor (flavors/glue-lite) --------------------------------
# Kestra, Spark Operator, Glue, S3, Iceberg on the emulated EKS cluster --
# no platform/tenant layers. Tear down with `make destroy` as usual.
.PHONY: gl-spike gl-cluster gl-spark-operator gl-image gl-smoke gl-up gl-generate \
        gl-health gl-bench gl-compact gl-report gl-demo gl-test-tables gl-metrics gl-symptoms gl-scorecard gl-scan gl-plan gl-step gl-sql gl-clean gl-status gl-group gl-groups-parallel gl-coverage gl-ops-check gl-ops-maintain gl-ops-upkeep-log gl-deferred-check gl-freed-files gl-pg-up gl-pg-migrate gl-pg-sql gl-pg-status

# Effective state / logs backends: STATE_BACKEND / LOGS_BACKEND, else the config
# (read by the gl-freed-files, gl-coverage and gl-ops-upkeep-log queries).
GL_STATE := $(or $(STATE_BACKEND),$(shell jq -r '.state.backend // "iceberg"' flavors/glue-lite/jobs/config/health.json 2>/dev/null))
GL_LOGS := $(or $(LOGS_BACKEND),$(shell jq -r '.logs.backend // "iceberg"' flavors/glue-lite/jobs/config/health.json 2>/dev/null))

gl-spike:
	bash flavors/glue-lite/spike/run-glue-spike.sh

gl-cluster: emulator-up provider-apply

gl-spark-operator:
	bash flavors/glue-lite/scripts/02-install-spark-operator.sh

gl-image:
	bash flavors/glue-lite/scripts/03-build-load-image.sh

gl-smoke:
	bash flavors/glue-lite/scripts/run-job.sh sql smoke

gl-up: gl-cluster gl-spark-operator gl-pg-up gl-image gl-smoke

# GL1: build glue.demo.events with healthy and fragmented day partitions.
# Override sizes, e.g.  make gl-generate GEN_ARGS="--commits 60 --files-per-commit 20"
GEN_ARGS ?= --recreate
gl-generate:
	bash flavors/glue-lite/scripts/run-job.sh py generate_small_files.py $(GEN_ARGS)

# GL2: measure, compact, benchmark, report.
#   make gl-health                       per-partition health -> glue.ops.table_health
#   make gl-bench BENCH_LABEL=before     read timings -> glue.ops.read_benchmarks
#   make gl-compact                      rewrite_data_files on COMPACT_ARGS days
#   make gl-report                       before/after tables from glue.ops.*
#   make gl-demo                         all of the above, in order
HEALTH_ARGS ?=
COMPACT_ARGS ?= --days 2026-09-03,2026-09-05 --maintenance
BENCH_LABEL ?= before
BENCH_ARGS ?=
gl-health:
	bash flavors/glue-lite/scripts/run-job.sh py table_health.py $(HEALTH_ARGS)

gl-bench:
	bash flavors/glue-lite/scripts/run-job.sh py read_benchmark.py --label $(BENCH_LABEL) $(BENCH_ARGS)

gl-compact:
	bash flavors/glue-lite/scripts/run-job.sh py compact.py $(COMPACT_ARGS)

gl-report:
	bash flavors/glue-lite/scripts/run-job.sh sql report

gl-demo:
	$(MAKE) gl-health
	$(MAKE) gl-bench BENCH_LABEL=before
	$(MAKE) gl-compact
	$(MAKE) gl-health
	$(MAKE) gl-bench BENCH_LABEL=after
	$(MAKE) gl-report

# GL2.5: test tables and metric inventory.
#   make gl-test-tables              build S0-S6 test tables (~10 min; s3 waits out its hot window)
#   make gl-metrics                  run the metric inventory over every table in glue.demo
#   make gl-symptoms                 apply the symptom rules to the latest scan -> glue.ops.symptoms
#   make gl-scorecard                check the latest scan against config/expectations.json
#   make gl-scan                     metrics + symptoms + scorecard in one job (s3 measured first)
#   make gl-scan FRESH_S3=1          rebuild s3 first so its hot partition is checkable (~4 min more)
#   make gl-scan SCAN_ARGS=--full    measure every table, even unchanged ones
#   make gl-scan TRACE=s20_delete_sprawl   scan only that table, printing every step (TRACE lines);
#                                    TRACE=a,b for several; add SCAN_ARGS=--full to force the full-measure path
#   make gl-scan SCAN_ARGS="--tables s19_partition_churn"   scan only some tables (no trace)
#   make gl-plan T=s0                print the fix plan for s0 from the latest scan (dry run)
#   make gl-plan T=s0 APPLY=1        run the plan's auto steps -> glue.ops.actions
#   make gl-step STEP=s12-mor        scripted approval step for a scenario (see scenario_step.py)
#   make gl-plan T=s14 APPROVE=MIXED_SPEC   run the plan's ASK statements for those symptoms
#   make gl-sql Q="SELECT ... ; ALTER ..."  run ad-hoc Spark SQL against the catalog
#   make gl-bench BENCH_LABEL=before BENCH_ARGS="--table glue.demo.s0_small_appends"
#   make gl-test-tables TT_ARGS="--only s0,s4"
TT_ARGS ?=
TRACE ?=
METRIC_ARGS ?=
SYM_ARGS ?=
SCAN_ARGS ?=
FRESH_S3 ?=
T ?=
APPLY ?=
APPROVE ?=
STEP ?=
Q ?=
gl-test-tables:
	JOB_TIMEOUT_MIN=45 bash flavors/glue-lite/scripts/run-job.sh py build_test_tables.py $(TT_ARGS)

gl-metrics:
	bash flavors/glue-lite/scripts/run-job.sh py scan_metrics.py $(METRIC_ARGS)

gl-symptoms:
	bash flavors/glue-lite/scripts/run-job.sh py detect_symptoms.py $(SYM_ARGS)

gl-scorecard:
	bash flavors/glue-lite/scripts/run-job.sh py scorecard.py

gl-scan:
ifeq ($(FRESH_S3),1)
	JOB_TIMEOUT_MIN=15 bash flavors/glue-lite/scripts/run-job.sh py build_test_tables.py --only s3
endif
	bash flavors/glue-lite/scripts/run-job.sh py gl_scan.py $(SCAN_ARGS) $(if $(TRACE),--trace $(TRACE))

gl-sql:
	bash flavors/glue-lite/scripts/run-job.sh py run_sql.py -e "$(Q)"

# Delete finished SparkApplications (and their driver pods) now, rather than
# waiting for their timeToLiveSeconds.
gl-clean:
	bash -c 'source flavors/glue-lite/scripts/env.sh && kubectl -n spark-jobs get sparkapplication -o json \
	  | jq -r ".items[] | select(.status.applicationState.state | IN(\"COMPLETED\", \"FAILED\", \"SUBMISSION_FAILED\")) | .metadata.name" \
	  | xargs -r kubectl -n spark-jobs delete sparkapplication'

# What the Glue-Lite stack is running and using.
gl-status:
	bash -c 'source flavors/glue-lite/scripts/env.sh && kubectl get nodes && kubectl get pods -A --field-selector=status.phase=Running \
	  && (kubectl top nodes 2>/dev/null || true) && docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"'

gl-step:
	JOB_TIMEOUT_MIN=20 bash flavors/glue-lite/scripts/run-job.sh py scenario_step.py $(STEP)

# Group runs (profiles.py, coordinator.py): one group of config/profile.json per job.
#   make gl-group G=group_a [SHARD=0/1] [SCAN_ARGS=--full]
#   make gl-groups-parallel            group_a and group_b at the same time (small pods), then coverage
#   make gl-coverage                   which run scanned each table, latest runs
gl-group:
	bash flavors/glue-lite/scripts/run-job.sh py gl_scan.py --group $(G) --shard $(or $(SHARD),0/1) $(SCAN_ARGS)

gl-groups-parallel:
	bash flavors/glue-lite/scripts/run-groups-parallel.sh group_a group_b

# Housekeeping under parallel runs (ops_maintenance.py):
#   make gl-ops-check                  compaction during concurrent appends, upkeep actions (scratch namespace)
#   make gl-ops-maintain [DRY=1]       upkeep of the glue.ops tables now (takes the housekeeping claim)
#   make gl-ops-upkeep-log             latest upkeep rows
gl-ops-check:
	JOB_TIMEOUT_MIN=20 bash flavors/glue-lite/scripts/run-job.sh py check_ops_upkeep.py

gl-ops-maintain:
	JOB_TIMEOUT_MIN=30 bash flavors/glue-lite/scripts/run-job.sh py ops_maintenance.py $(if $(filter 1,$(DRY)),--dry-run)

gl-ops-upkeep-log:
ifeq ($(GL_LOGS),postgres)
	Q="SELECT run_id, regexp_replace(table_name, 'glue.ops.', '') AS t, data_files, excess_files, manifests, snapshots, expirable_snapshots, actions, result, seconds FROM advisor.ops_maintenance ORDER BY checked_at DESC, table_name LIMIT $(or $(N),40)" bash flavors/glue-lite/scripts/pg-sql.sh
else
	bash flavors/glue-lite/scripts/run-job.sh py run_sql.py -e "SELECT run_id, regexp_replace(table_name, 'glue.ops.', '') AS t, data_files, excess_files, manifests, snapshots, expirable_snapshots, actions, result, seconds FROM glue.ops.ops_maintenance ORDER BY checked_at DESC, table_name LIMIT $(or $(N),40)"
endif

# Deferred deletion of files a snapshot expiry frees (freed_files.py):
#   make gl-deferred-check             expiry that keeps files, then deletion after the grace (scratch namespace)
#   make gl-freed-files                files waiting per table, and how many are due
gl-deferred-check:
	JOB_TIMEOUT_MIN=20 bash flavors/glue-lite/scripts/run-job.sh py check_deferred_delete.py

gl-freed-files:
ifeq ($(GL_STATE),postgres)
	Q="SELECT item->>'table_name' AS table_name, count(*) AS files, sum(CASE WHEN (item->>'due_ms')::bigint <= extract(epoch FROM now()) * 1000 THEN 1 ELSE 0 END) AS due, to_timestamp(min((item->>'due_ms')::bigint) / 1000.0) AS first_due, to_timestamp(max((item->>'due_ms')::bigint) / 1000.0) AS last_due FROM advisor.freed_files GROUP BY 1 ORDER BY files DESC" bash flavors/glue-lite/scripts/pg-sql.sh
else
	bash flavors/glue-lite/scripts/run-job.sh py run_sql.py -e "SELECT table_name, count(*) AS files, sum(CASE WHEN due_ms <= unix_millis(current_timestamp()) THEN 1 ELSE 0 END) AS due, timestamp_millis(min(due_ms)) AS first_due, timestamp_millis(max(due_ms)) AS last_due FROM glue.ops.freed_files GROUP BY table_name ORDER BY files DESC"
endif

gl-coverage:
ifeq ($(GL_LOGS),postgres)
	Q="SELECT j.group_name, j.shard, j.status, j.tables_matched, j.tables_done, j.tables_skipped, j.housekeeping, j.started_at, j.ended_at FROM advisor.run_journal j ORDER BY j.started_at DESC LIMIT 6" bash flavors/glue-lite/scripts/pg-sql.sh
	Q="WITH last AS (SELECT run_id FROM advisor.run_journal WHERE status = 'ok' ORDER BY started_at DESC LIMIT $(or $(RUNS),2)) SELECT c.table_name, count(DISTINCT c.run_id) AS runs, string_agg(DISTINCT c.group_name, ',') AS groups, string_agg(DISTINCT c.status, ',') AS statuses FROM advisor.coverage c JOIN last l ON c.run_id = l.run_id GROUP BY c.table_name ORDER BY runs DESC, c.table_name" bash flavors/glue-lite/scripts/pg-sql.sh
else
	bash flavors/glue-lite/scripts/run-job.sh py run_sql.py -e "SELECT j.group_name, j.shard, j.status, j.tables_matched, j.tables_done, j.tables_skipped, j.housekeeping, j.started_at, j.ended_at FROM glue.ops.run_journal j ORDER BY j.started_at DESC LIMIT 6; WITH last AS (SELECT run_id FROM glue.ops.run_journal WHERE status = 'ok' ORDER BY started_at DESC LIMIT $(or $(RUNS),2)) SELECT c.table_name, count(DISTINCT c.run_id) AS runs, concat_ws(',', collect_set(c.group_name)) AS groups, concat_ws(',', collect_set(c.status)) AS statuses FROM glue.ops.coverage c JOIN last l ON c.run_id = l.run_id GROUP BY c.table_name ORDER BY runs DESC, c.table_name"
endif

# Postgres for state and logs (pg_store.py; the default in jobs/config/health.json,
# the Iceberg backends are kept). Keep one backend per environment and switch by copying.
#   make gl-pg-up                      Postgres pod in advisor-db + the advisor-pg Secret (safe to re-run; part of gl-up)
#   make gl-scan STATE_BACKEND=iceberg LOGS_BACKEND=iceberg   one run on the Iceberg ops tables (any gl-* job takes these)
#   make gl-pg-migrate [LOGS=1] [REPLACE=1]   copy state (and logs) from the Iceberg ops tables, once
#   make gl-pg-sql Q="SELECT ..."      psql in the pod; tables in schema advisor (logs typed, state as jsonb items)
#   make gl-pg-status                  row counts per table
export STATE_BACKEND LOGS_BACKEND Q
gl-pg-up:
	bash flavors/glue-lite/scripts/04-postgres.sh

gl-pg-migrate:
	JOB_TIMEOUT_MIN=45 bash flavors/glue-lite/scripts/run-job.sh py migrate_state.py $(if $(filter 1,$(LOGS)),--logs) $(if $(filter 1,$(REPLACE)),--replace)

gl-pg-sql:
	Q="$$Q" bash flavors/glue-lite/scripts/pg-sql.sh

gl-pg-status:
	Q= bash flavors/glue-lite/scripts/pg-sql.sh

gl-plan:
	bash flavors/glue-lite/scripts/run-job.sh py plan.py $(if $(T),--tables $(T)) $(if $(filter 1,$(APPLY)),--apply) $(if $(APPROVE),--approve $(APPROVE))
