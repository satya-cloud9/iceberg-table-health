# iceberg-table-health

An Iceberg Table Health & Compaction Advisor. It scans every table in an
Iceberg catalog, measures its health from metadata (small files, deletes,
snapshots, manifests, partition activity, late arrivals), turns the numbers
into findings, plans the fixes (compaction, expiry, orphan removal) and
checks them against a scorecard of test scenarios.

Forked from [lakehouse-anywhere-a](https://github.com/satya-cloud9/lakehouse-anywhere-a)
(`feature/oidc`), which stays untouched. This repo keeps only what the
advisor needs: the multi-tenant platform (Nessie, Kestra, Trino, MinIO,
Postgres, observability, dbt) was removed on 2026-10-04 and is still in
the upstream repo and in this repo's git history.

## What runs

| Piece | What it is |
|---|---|
| Floci | Local AWS emulator: S3 (the warehouse), Glue (the catalog), DynamoDB (advisor state, next), emulated EKS |
| Emulated EKS | One k3s node, created by `terraform/providers/aws` |
| Spark Operator | Runs every advisor job as a `SparkApplication` |
| `glue-lite-spark` image | Spark 3.5 + Iceberg + the advisor's Python jobs |

## Quick start

```bash
make preflight install      # once per machine
make gl-up                  # Floci + emulated EKS + Spark Operator + image + smoke test
make gl-test-tables         # build the test scenarios
make gl-scan                # metrics -> findings -> scorecard
make gl-status              # what is running, memory used
make teardown               # stop Floci and the cluster
```

Everything about the advisor (jobs, scenarios, rules, ops tables, the
scorecard) is in [flavors/glue-lite/README.md](flavors/glue-lite/README.md).

## Repo layout

| Path | What it is |
|---|---|
| `flavors/glue-lite/` | The advisor: Spark jobs, config, Dockerfile, SparkApplication template, scripts |
| `terraform/providers/aws/` | Floci-emulated EKS, IAM and the parity bucket (`CONTRACT.md` describes its outputs) |
| `scripts/00-03, 03b, 99` | Preflight, install, start Floci, apply the provider, verify it, tear down |
| `docker-compose.floci.yml` | The Floci container |
| `Makefile` | `gl-*` targets for the advisor; `emulator-up`, `provider-apply`, `teardown`, `destroy` for the stack |

The roadmap is kept in a separate document, not in this repo.
