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
        gl-health gl-bench gl-compact gl-report gl-demo gl-test-tables gl-metrics gl-symptoms gl-scorecard gl-scan gl-plan gl-step gl-sql gl-clean gl-status

gl-spike:
	bash flavors/glue-lite/spike/run-glue-spike.sh

gl-cluster: emulator-up provider-apply

gl-spark-operator:
	bash flavors/glue-lite/scripts/02-install-spark-operator.sh

gl-image:
	bash flavors/glue-lite/scripts/03-build-load-image.sh

gl-smoke:
	bash flavors/glue-lite/scripts/run-job.sh sql smoke

gl-up: gl-cluster gl-spark-operator gl-image gl-smoke

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
#   make gl-plan T=s0                print the fix plan for s0 from the latest scan (dry run)
#   make gl-plan T=s0 APPLY=1        run the plan's auto steps -> glue.ops.actions
#   make gl-step STEP=s12-mor        scripted approval step for a scenario (see scenario_step.py)
#   make gl-plan T=s14 APPROVE=MIXED_SPEC   run the plan's ASK statements for those symptoms
#   make gl-sql Q="SELECT ... ; ALTER ..."  run ad-hoc Spark SQL against the catalog
#   make gl-bench BENCH_LABEL=before BENCH_ARGS="--table glue.demo.s0_small_appends"
#   make gl-test-tables TT_ARGS="--only s0,s4"
TT_ARGS ?=
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
	bash flavors/glue-lite/scripts/run-job.sh py gl_scan.py $(SCAN_ARGS)

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

gl-plan:
	bash flavors/glue-lite/scripts/run-job.sh py plan.py $(if $(T),--tables $(T)) $(if $(filter 1,$(APPLY)),--apply) $(if $(APPROVE),--approve $(APPROVE))
