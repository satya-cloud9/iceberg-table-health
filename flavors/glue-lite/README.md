# Glue-Lite flavor

A slim stack for showing Spark compaction and metrics-driven automation early:
Kestra, Spark Operator, Glue Data Catalog, S3 and Iceberg. No Trino, Nessie,
dbt, tenants or observability. Built on Floci first, then promoted to real EKS.

## GL0 step 1 — Glue spike (go/no-go)

Checks whether Iceberg's `GlueCatalog` can create and commit to a table
against Floci's Glue emulation. Needs only the Floci emulator running, not the
Kubernetes cluster.

```bash
make emulator-up                                 # if lakehouse-floci isn't running
bash flavors/glue-lite/spike/run-glue-spike.sh
```

The first run downloads the Spark image and the Iceberg jars, which takes a
few minutes.

- **Pass:** 5 rows, 2 `append` snapshots, 3 partitions, and `get-table` shows
  a `metadata_location` in S3. Continue GL0–GL3 on Floci.
- **Fail:** an error on `CREATE TABLE` or the second `INSERT`. Move GL0 to
  real EKS; the Spark image and jobs stay the same.

Overrides (environment variables): `FLOCI_ENDPOINT`, `WAREHOUSE_BUCKET`,
`AWS_REGION`, `SPARK_IMAGE` (default `apache/spark:3.5.6`), `ICEBERG_VERSION`
(default `1.10.0`).

**Result (2026-10-02): PASS** on Floci nightly — 2 snapshots, 3 partitions,
Glue `metadata_location` advanced `00001` → `00002`. GL0–GL3 run on Floci.

## GL0 step 2 — Spark in the cluster

Brings up only the emulated EKS cluster (no platform or tenant layers), installs
the Spark Operator, loads the Spark image and runs the same checks as the spike
as a `SparkApplication`. Proves pod → Floci networking, the image and the
operator together.

```bash
make gl-cluster          # Floci + emulated EKS (same as emulator-up + provider-apply)
make gl-spark-operator   # spark-jobs namespace, quota, aws-creds Secret, operator 2.5.2
make gl-image            # build glue-lite-spark:local, import into the nodes
make gl-smoke            # run jobs/sql/smoke.sql, print the driver output
```

Pass: the smoke job ends `COMPLETED` and prints 5 rows, 2 `append` snapshots
and 3 partitions for `glue.demo.smoke_events`.

### How it fits together

| Path | What it is |
|---|---|
| `docker/spark.Dockerfile` | `apache/spark:3.5.6` + Iceberg runtime and AWS bundle 1.10.0 + `jobs/` |
| `jobs/run_sql.py` | runs every statement in one SQL file and prints results |
| `jobs/sql/*.sql` | SQL jobs (`smoke.sql`, `report.sql`) |
| `jobs/generate_small_files.py` | GL1 generator: healthy vs fragmented day partitions |
| `jobs/gl_common.py` | shared helpers: ops tables, per-partition health query |
| `jobs/table_health.py` | GL2 metrics -> `glue.ops.table_health` |
| `jobs/compact.py` | GL2 `rewrite_data_files` per partition -> `glue.ops.compaction_runs` |
| `jobs/read_benchmark.py` | GL2 read timings -> `glue.ops.read_benchmarks` |
| `jobs/sql/report.sql` | before/after report over the three ops tables |
| `k8s/sparkapp.tmpl.yaml` | the one SparkApplication template every job renders |
| `k8s/spark-jobs.yaml` | job namespace + ResourceQuota |
| `helm/spark-operator-values.yaml` | operator watches `spark-jobs`, creates the `spark` service account |
| `scripts/env.sh` | shared settings; overrides via env vars |
| `scripts/run-job.sh sql <name>` / `py <script> [args]` | render, submit, wait, print driver log |

Pods reach Floci at `http://172.17.0.1:4566` (Docker bridge gateway, the same
address the base repo's aws provider uses). Override with `AWS_ENDPOINT` if
your Docker network differs.

Every change to `jobs/` needs `make gl-image` again before the next run.

## GL1 — Small-file generator

Builds `glue.demo.events`: 7 days of 200,000 events each. Healthy days are
written in one commit (one file). Fragmented days (2026-09-03 and 2026-09-05
by default) take 30 commits of 10 files each, i.e. 300 small files and 30
snapshots per day, the way frequent incremental runs leave a table. Every day
holds the same row count, so a later read benchmark compares like with like.

```bash
make gl-image      # jobs/ changed, so rebuild + reload first
make gl-generate   # GEN_ARGS="--recreate" by default
```

Pass: the result table shows ~1 file per healthy day and ~300 files per
fragmented day, with a much smaller `avg_file_kb` on the fragmented days.

Options (`GEN_ARGS`): `--table`, `--start`, `--days`, `--rows-per-day`,
`--fragmented 2026-09-03,2026-09-05`, `--commits`, `--files-per-commit`,
`--recreate`.

**Result (2026-10-02):** healthy days 1 file of ~7.6 MB; fragmented days 300
files of ~30 KB; 65 snapshots.

## GL2 — Measure, compact, benchmark

```bash
make gl-image   # jobs/ changed
make gl-demo    # health -> bench before -> compact -> health -> bench after -> report
```

Or step by step: `make gl-health`, `make gl-bench BENCH_LABEL=before`,
`make gl-compact`, `make gl-health`, `make gl-bench BENCH_LABEL=after`,
`make gl-report`. Each step is its own SparkApplication (~1 min of pod
start-up each).

**How "needs compaction" is decided** (`gl_common.partition_health`):

| Column | Meaning |
|---|---|
| `ideal_files` | target-sized files the partition's data needs (min 1) |
| `excess_files` | `data_files - ideal_files`: files compaction would remove |
| `small_files` | data files under 75% of target (Iceberg's own `min-file-size-bytes` default) |
| `needs_compaction` | `excess_files >= --min-excess` (default 4) |

A healthy day here is one 7.6 MB file: "small" against a 128 MB target, but
its data fits in one file, so `excess_files` is 0 and it is not flagged.

**Compaction** runs one `rewrite_data_files` CALL per day with a `where`
filter, partial progress on, then (`--maintenance`) `rewrite_manifests` and
`expire_snapshots` keeping the last 5. Orphan-file removal is left out for
now.

**Read benchmark** compares a healthy control day (09-02) with the
fragmented days, before and after. Queries read real column data, because a
bare `count(*)` is answered from Iceberg file statistics without opening
files. One warm-up, then 5 timed runs; the median is recorded.

Pass: fragmented days drop from ~300 files to 1, `still_flagged` false, and
the report shows the fragmented days' read times falling toward the healthy
day's.
