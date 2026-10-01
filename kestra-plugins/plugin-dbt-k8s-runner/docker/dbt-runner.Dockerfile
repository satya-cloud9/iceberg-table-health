# Minimal image for examples/02-dbt-run.yaml: python + dbt-core + dbt-trino
# + the dbt project itself, baked in at build time. This is the "cheap
# phase 1" answer to file sync -- rebuild the image when the dbt project
# changes, rather than build the init-container/sidecar file-sync mechanism
# the EE Kubernetes runner has, until phase 1 mechanics are actually proven.
#
# Build (from repo root):
#   docker build -f kestra-plugins/plugin-dbt-k8s-runner/docker/dbt-runner.Dockerfile \
#     -t ghcr.io/yourorg/dbt-runner:latest .

FROM python:3.12-slim

WORKDIR /workspace/dbt

COPY dbt/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dbt/ .

# profiles.yml's target host/port need to resolve from inside
# tenant-data-eng, not from the worker's own network path -- Trino's
# Kubernetes Service DNS name (trino.platform.svc.cluster.local or
# equivalent) works from any namespace in-cluster, so no per-namespace
# profile changes should be needed. Confirm against your actual
# profiles.yml target before relying on this.
