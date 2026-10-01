# Iceberg Table Health & Compaction Advisor — Roadmap

Fork of [lakehouse-anywhere-a](https://github.com/satya-cloud9/lakehouse-anywhere-a)
(`feature/oidc` @ `83ceba0`). Upstream stays untouched; it is wired here as the
`upstream` remote for cherry-picking base fixes only.

Scope differences from upstream:
- **EKS only** — Floci-emulated EKS first, then real EKS. No provider-agnostic work.
- **Catalog: Nessie** (inherited from upstream). Glue Data Catalog comes later,
  once the Advisor is fully demonstrable, as an extra overlay.

## Milestones

| # | Milestone | Where | Done when |
|---|---|---|---|
| M0 | Baseline stack from upstream running (Nessie, Kestra, Trino, MinIO/S3, dbt runner) | Floci | A dbt model writes an Iceberg table end to end |
| M1 | Small-file generator + table-health metrics from Iceberg metadata tables (`files`, `partitions`, `snapshots`) | Floci | A health table shows per-partition file counts/sizes, with a deliberately fragmented table visible |
| M2 | Spark execution layer: Spark image with pinned Iceberg runtime + Nessie catalog config, Spark Operator, event logs to S3, History Server | Floci | One manual `rewrite_data_files` `SparkApplication` compacts a partition |
| M3 | **Advisor Phase 1** — dbt `meta.compaction` opt-in; Kestra reads `manifest.json` / `run_results.json`; planner scores partitions; scoped rewrite with partial progress; Nessie-branch safe mode (compact on branch → validate → merge); results logged | Floci | A dbt run triggers compaction of only the fragmented partitions |
| M4 | Promote to real EKS: Terraform, ECR, IRSA/Pod Identity for Spark, Karpenter node pool with spot executors, cost tags | EKS | M3 runs unchanged on EKS; benchmark vs Trino `optimize` |
| M5 | **Advisor Phase 2 — self-serve**: presets, Kestra form, dry-run plans, guardrails (allowlist, ResourceQuota, per-table concurrency), audit trail, developer-facing health views | EKS | A dbt developer opts a table in and triggers compaction without touching Spark |
| M6 | Write-up: architecture, comparison with Amoro / Glue optimizer / Trino `optimize`, cost + performance numbers | — | README and short demo |
| M7 | (Later) Glue Data Catalog overlay + side-by-side with Glue's built-in optimizer | EKS | Same plan runs against both catalogs |

## Maintenance sequence (Nessie)

1. `rewrite_data_files` (scoped, partial progress)
2. `rewrite_manifests`
3. **Nessie GC** on a schedule — *not* Iceberg's `expire_snapshots` /
   `remove_orphan_files`, which only see one branch and can delete files other
   Nessie branches or tags still reference.

## Notes

- Spark does not exist upstream; M2 starts from scratch.
- Floci may not emulate IRSA token exchange, Karpenter or spot behaviour fully;
  those are deliberately deferred to M4 on real EKS.
- Bring real EKS up only for M4–M5 sessions and tear it down after.
