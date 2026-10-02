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

## GL2.5a/b — Test tables and metric inventory

Detection comes first, as a catalog-wide scan: a fixed set of metrics measures
every table the same way, without knowing which scenario built it.

```bash
make gl-image          # jobs/ changed
make gl-test-tables    # S0-S6 in glue.demo, rebuilt from scratch (~10 min)
make gl-metrics        # metric inventory over every table in glue.demo
```

**Test scale.** `jobs/config/health.json` sets the target file size to 8 MB
(64x below Iceberg's 512 MB default) so small tables show real symptoms. A
healthy day of 100,000 rows is one ~3.8 MB file.

| Table | Built as | Intended symptom |
|---|---|---|
| `s0_small_appends` | 7 days; 09-03 and 09-05 get 20 commits x 10 files | `SMALL_FILES` |
| `s1_late_arrivals` | 30 days, then 20 runs adding a small file to recent days (decaying, seed 42) | `SCATTERED_SMALL_FILES` |
| `s2_mor_deletes` | 7 days, then 10 merge-on-read `DELETE`s (1% of rows each) | `DELETE_BUILDUP` |
| `s3_hot_partition` | fragmented past day, wait out the hot window, then fragment today | `SMALL_FILES` + `HOT_PARTITION` |
| `s4_manifest_bloat` | 60 commits of one healthy day each, manifest merging off | `MANIFEST_BLOAT` |
| `s5_skewed_partition` | 29 normal days + one day with 30x the rows in 15 files | `PARTITION_SKEW` |
| `s6_over_partitioned` | `hours()` partitioning, 30 days: 720 one-file partitions | `OVER_PARTITIONED` |

`s3` is built last and waits `--hot-minutes` (default 3) + 30 s between its two
phases, so a scan started right after sees exactly one hot partition.

**Outputs.** `glue.ops.partition_metrics` (one row per partition: files,
deletes, size percentiles, small/oversized/excess files, spec and sort-order
coverage, minutes since update) and `glue.ops.table_metrics` (one row per
table: read amplification, skew, undersized partitions, snapshots and commit
pattern, manifests, metadata versions, retained bytes, clustering efficiency
on declared filter columns, sort order, write properties). Metric IDs match
the inventory in the roadmap.

**Clustering (C1).** For each declared filter column (`filter_columns` in the
config), per-file min/max bounds give the expected files a point lookup must
open; efficiency 1 = files don't overlap (sorted), 0 = every file spans the
whole range. Tested locally against mock metadata: 200 overlapping files -> 0,
10 sorted files -> 1.

## GL2.5c — Symptom engine

Rules turn one scan's metrics into named findings. Every threshold lives in
`jobs/config/health.json` under `thresholds` (per-table overrides merge key by
key). The rules are plain Python (`jobs/symptom_rules.py`), so they are
unit-tested without Spark.

```bash
make gl-image       # jobs/ changed
make gl-metrics     # new scan
make gl-symptoms    # rules over the latest scan -> glue.ops.symptoms
```

| Symptom | Level | Fires when (test thresholds) | Action |
|---|---|---|---|
| `SMALL_FILES` | partition | excess files >= 4 and not hot | auto |
| `HOT_PARTITION` | partition | would be `SMALL_FILES`/`DELETE_BUILDUP`, but written < 3 min ago | defer |
| `DELETE_BUILDUP` | partition | delete records / data records >= 5% (ratio, not count: Iceberg keeps ~1 position-delete file per data file) | auto |
| `OVERSIZED_FILES` | partition | any file > 180% of target | auto |
| `SCATTERED_SMALL_FILES` | table | >= 3 and >= 30% of partitions have >= 4 excess files (the ones `SMALL_FILES` flags, so it clears once they're compacted), and commits touch >= 3 partitions on average | auto |
| `SNAPSHOT_BUILDUP` | table | > 30 snapshots or oldest > 120 h | auto |
| `MANIFEST_BLOAT` | table | merging on: >= `commit.manifest.min-count-to-merge` (100); merging off: >= 20 manifests; and avg manifest < 8 MB | auto |
| `UNBOUNDED_RETENTION` | table | metadata versions > `previous-versions-max` (100) with no auto-delete | approval |
| `PARTITION_SKEW` | table | largest / median partition >= 10, >= 5 partitions, largest >= 2x target | approval |
| `OVER_PARTITIONED` | table | >= 100 partitions and >= 80% under 10% of target | approval |
| `MIXED_SPEC` | table | any file on an older partition spec | approval |
| `LEGACY_FORMAT` | table | format version 1 | approval |
| `METRICS_DISABLED` | table | a filter column has no min/max stats | approval |
| `POOR_CLUSTERING` | column | pruning efficiency < 0.3 | needs-evidence |

**Why these gates.** Calibrated on the first `gl-metrics` run:

- `avg_changed_partitions_per_commit` alone can't spot late arrivals: bulk
  loads (s5 = 15, s6 = 720) and deletes (s2 = 7) score high too. Late arrivals
  need many partitions *with excess files* as well (s1 has ~25 of 30).
- `OVER_PARTITIONED` needs a partition-count gate: the smoke/spike tables have
  3 tiny partitions (undersized share 1.0) and are fine.
- `MANIFEST_BLOAT`: with merging on, Iceberg merges by itself at 100
  manifests, so s0/s3 (45) are not flagged; s4 has merging off, so 60 is.
- `PARTITION_SKEW` needs the big partition to be big in absolute terms, not
  just relative to tiny neighbours.

**Workload-dependent findings wait for evidence.** `POOR_CLUSTERING` fires
from declared `filter_columns`, but it is written with action
`needs-evidence` until the evidence level reaches `workload_min_evidence`
(default `observed`, which the 2.5d++ spark-log adapter will provide).
Reports list these separately; the 2.5d scorecard ignores them.

**Production values** (to revisit at GL4): target 512 MB, `max_snapshots`
~100 with age as the main trigger, `over_partitioned_min_partitions` ~1000,
`hot_partition_minutes` ~60.

## GL2.5d — Catalog scan and scorecard

```bash
make gl-image               # jobs/ changed
make gl-scan                # metrics -> symptoms -> scorecard, one Spark job
make gl-scan FRESH_S3=1     # rebuild s3 first, then scan (checks HOT_PARTITION)
make gl-scorecard           # re-score the latest scan only
```

`gl_scan.py` runs the three steps in one job (one pod start) and measures
`s3_hot_partition` first. The scorecard checks each scenario table against
`jobs/config/expectations.json` and writes `glue.ops.scorecard`:

- **PASS**: every expected symptom found where expected (partition selectors:
  exact partitions, newest only, not newest, minimum partition value, count),
  and no other active symptom.
- **FAIL**: something missing, misplaced or unexpected.
- **STALE**: s3 only. Its check needs the newest partition to be inside the hot
  window at scan time. After 3 minutes that partition has cooled, and
  compacting it is the right call, so the result says to rebuild rather than
  fail. `FRESH_S3=1` does the rebuild.
- **NOT SCORED**: tables with no expectations (`events`, smoke, spike).
- Findings held for workload evidence are ignored.

First real run (2026-10-02, scan taken ~2.7 h after the build): 6/7 PASS, s3
STALE (today 162 min old). Unit-tested on that output plus a fresh-s3 case
(PASS) and injected regressions (all FAIL).

## GL2.5e — Fix and verify (first: S0)

Detect, fix, check: the scan's findings become maintenance statements, the
safe ones run, and the next scan must show the table healthy.

```bash
make gl-image
make gl-scan                                       # before: s0 = SMALL_FILESx2, SNAPSHOT_BUILDUP
make gl-bench BENCH_LABEL=before BENCH_ARGS="--table glue.demo.s0_small_appends"
make gl-plan T=s0                                  # dry run: print the plan
make gl-plan T=s0 APPLY=1                          # run its auto steps
make gl-scan                                       # after: s0 should score PASS (after fix)
make gl-bench BENCH_LABEL=after  BENCH_ARGS="--table glue.demo.s0_small_appends"
```

**`plan.py`** reads the latest scan's findings and builds, per table:
1. `rewrite_data_files` (binpack) over the flagged partitions (`SMALL_FILES`,
   `OVERSIZED_FILES`, `DELETE_BUILDUP`; top `max_partitions_per_run` by score
   when `SCATTERED_SMALL_FILES`), scoped with a `where` built from the partition
   keys and spec (day/hour/month/year/identity; bucket and truncate can't be
   scoped and are reported). Hot partitions are never included.
2. `rewrite_manifests` for `MANIFEST_BLOAT`.
3. `expire_snapshots` (keep last 5) for `SNAPSHOT_BUILDUP`, last so the
   snapshots the rewrites replaced can expire too.

Options come from the same config the scan used: target size = the target the
scan judged by; `min-input-files` = `min_excess_files` + 1 (so every flagged
partition is one the rewrite will change); `delete-file-threshold=1` and
`remove-dangling-deletes` when deletes are the problem; partial progress for
large rewrites. Approval and needs-evidence findings print suggested SQL
(spec change, sort order, property changes) but never run. With `APPLY=1`,
each statement run is recorded in `glue.ops.actions` with the table's UUID.

**Before/after in the scorecard.** A table plan.py has fixed (same UUID) is
scored against the `after` block in `expectations.json`; rebuilding the table
gives it a new UUID, so it goes back to `before`. s0's `after` is "healthy".

**Target size per table.** The scan now uses a per-table `target_file_bytes`
in `health.json` if set, else the table's own `write.target-file-size-bytes`,
else the default, and records which (`target_source` in `table_metrics`).

**Planning time.** The benchmark also times Iceberg's `planFiles()` for each
day (median of the runs, after a warm-up) and records the files it planned
(`plan_median_ms`, `planned_files` in `read_benchmarks`).

### S1, S2, S4 and the benchmark

```bash
make gl-plan T=s1,s2,s4            # dry run
make gl-plan T=s1,s2,s4 APPLY=1
make gl-scan                       # s1, s2, s4 scored against their 'after' blocks
make gl-bench BENCH_LABEL=before BENCH_ARGS="--table glue.demo.s4_manifest_bloat --days all"   # before APPLY
make gl-bench BENCH_LABEL=after  BENCH_ARGS="--table glue.demo.s4_manifest_bloat --days all"   # after APPLY
```

- **s1**: one run compacts the top 10 of 11 flagged days; `after` expects one
  `SMALL_FILES` left (the next run's work) and no `SCATTERED_SMALL_FILES`.
- **s2**: binpack with `delete-file-threshold=1` and `remove-dangling-deletes`.
  If this Iceberg rejects the option, plan.py retries without it and then runs
  `rewrite_position_delete_files`; both attempts are recorded in
  `glue.ops.actions`. `after` expects no `DELETE_BUILDUP`.
- **s4**: `rewrite_manifests` + `expire_snapshots`; `after` expects healthy.
  Re-enabling manifest merging is printed for approval.

Benchmark changes: Spark's INFO logging is off (output is just the results);
timed runs are interleaved in a shuffled order after a warm-up pass, so no day
always goes first; `--days all` benchmarks the whole table with no filter
(planning then reads every manifest, which is what s4 is about); after a
non-`before` run, a before/after table is printed against the latest `before`
run of the same table.

### Fixes from the first S2 run

- **Hot window counts writers only.** Iceberg's `last_updated_at` moves on any
  commit, so our own compaction made s2's just-rewritten partitions look hot
  (`HOT_PARTITION` x7). `minutes_since_update` now comes from the last
  non-`replace` commit that added files to the partition (`.all_entries` joined
  to `.snapshots`). If no writer commit is left in history, the partition is
  not hot.
- **Deletes need `rewrite_position_delete_files`.** The delete-aware rewrite
  applied the deletes, but each day's delete file stayed attached:
  `rewrite_data_files` gives new files the *starting* sequence number, so the
  delete file from the last `DELETE` still applies by sequence number and
  `remove-dangling-deletes` keeps it. Queries were correct; the metadata
  wasn't. plan.py now follows the data rewrite with
  `rewrite_position_delete_files(... 'rewrite-all' => 'true')`, which drops
  delete rows whose data files are gone.

```bash
make gl-image
make gl-scan                    # s2 still DELETE_BUILDUPx7 (after fix)
make gl-plan T=s2 APPLY=1       # rewrite + rewrite_position_delete_files
make gl-scan                    # s2 should PASS (after fix), not HOT_PARTITION
```

### S3 end to end

```bash
make gl-image
make gl-scan FRESH_S3=1         # rebuild s3, scan straight after: s3 PASS (before)
make gl-plan T=s3 APPLY=1       # compacts the past day, holds today, expires snapshots
make gl-scan                    # s3 PASS (after fix): today HOT_PARTITION or SMALL_FILES
make gl-plan T=s3 APPLY=1       # once today has cooled: compacts today
make gl-scan                    # s3 PASS (after fix): healthy
```

`plan.py` acts on the scan's findings, not on the clock: if today was hot when
scanned, it is held even if it has cooled by the time the plan runs.

s3's `after` block uses `optional` expectations: `HOT_PARTITION` or
`SMALL_FILES` may appear, but only on today; anything on the past day, or
`SNAPSHOT_BUILDUP`, fails. All three states (still hot, cooled, compacted by
a second run) pass.

## GL2.5f — Shapes not yet tested (s7–s11)

Tables for structures the probes and plan.py hadn't met on real data, so
"generic in code" becomes "proven generic" through the same scorecard.

| Table | Shape | Before | What the fix proves |
|---|---|---|---|
| `s7_unpartitioned` | no partition spec, 40 small appends | `SMALL_FILES`, `SNAPSHOT_BUILDUP` | whole-table rewrite with no `where` |
| `s8_identity_bucket` | `region, bucket(8, customer_id)`; each eu commit writes to all 8 buckets | `SMALL_FILES` x8 on `eu/*` (`SCATTERED_SMALL_FILES` allowed) | bucket can't be a range: scope widens to `region = 'eu'` |
| `s9_hourly_small` | `hours()`, 2 of 48 hours fragmented | `SMALL_FILES` x2 | hour partition values turned into the right time ranges |
| `s10_equality_deletes` | unpartitioned v2, 10 equality-delete commits (via Iceberg's Java API) | `DELETE_BUILDUP` | eq deletes applied, eq-delete files dropped |
| `s11_string_keys` | identity on `north america`, `o'neil`, `a/b`, `x=y` | `SMALL_FILES` on `a/b`, `o'neil` | quoting in the `where` |

Engine changes:
- `DELETE_BUILDUP` also fires on equality-delete files (>= 5 per partition).
  Their record count says nothing about rows deleted, and each one is checked
  against every older data file on read, so the file count is what matters.
- plan.py: a field that can't be a range (bucket, truncate) is dropped from
  the scope instead of skipping the partition; the note says the scope was
  widened. The rewrite still only picks small files. Unpartitioned tables get
  no `where` at all.
- Scorecard: `prefix` selector (multi-field partition values are joined with `/`).

```bash
make gl-image
make gl-test-tables TT_ARGS="--only s7,s8,s9,s10,s11"   # waits out the hot window at the end
make gl-scan                                            # s7-s11 PASS (before)
make gl-plan T=s7,s8,s9,s10,s11                         # check the statements
make gl-plan T=s7,s8,s9,s10,s11 APPLY=1
make gl-scan                                            # s7-s11 PASS (after fix)
```

s10's equality deletes go through `GenericAppenderFactory` over py4j. If that
class isn't in the image, the builder says so and s10's scorecard check fails
instead of the whole build.
