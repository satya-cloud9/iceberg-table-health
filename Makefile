SHELL := /bin/bash

# Which tenant every target below acts on. Override on the command
# line, e.g.:
#   make tenant-apply TENANT=tenant-b
# This fork is EKS-only: Floci-emulated EKS first, then real EKS. The
# other providers from upstream lakehouse-anywhere were removed, so
# PROVIDER is pinned rather than overridable.
override PROVIDER := aws
TENANT ?= tenant-a
export PROVIDER
export TENANT

.PHONY: preflight install emulator-up provider-apply platform-apply tenant-apply \
        flows status teardown destroy up

preflight:
	bash scripts/00-preflight.sh

install:
	bash scripts/01-install-deps.sh

emulator-up:
	bash scripts/02-start-emulator.sh

provider-apply:
	bash scripts/03-apply-provider.sh

platform-apply:
	bash scripts/04-apply-platform.sh

tenant-apply:
	bash scripts/05-apply-tenant.sh

flows:
	bash scripts/06-register-flows.sh

status:
	bash scripts/status.sh

# Stops the emulator/local state only -- leaves all Terraform state alone.
teardown:
	bash scripts/99-teardown.sh

# Actually runs `tofu destroy` at every applied stage (tenant, platform,
# provider), then stops the emulator. See scripts/99-teardown.sh.
destroy:
	DESTROY=1 bash scripts/99-teardown.sh

# Full run, phase by phase, against PROVIDER (default aws). Intended to be
# run interactively the first time so you can catch and report back any
# failure before the next phase starts -- e.g.:
#   make up PROVIDER=baremetal
up: preflight install emulator-up provider-apply platform-apply tenant-apply flows
	@echo ""
	@echo "=== Stack is up (PROVIDER=$(PROVIDER), TENANT=$(TENANT)). Run 'make status' for endpoints. ==="

# --- Glue-Lite flavor (flavors/glue-lite) --------------------------------
# Kestra, Spark Operator, Glue, S3, Iceberg on the emulated EKS cluster --
# no platform/tenant layers. Tear down with `make destroy` as usual.
.PHONY: gl-spike gl-cluster gl-spark-operator gl-image gl-smoke gl-up gl-generate

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
