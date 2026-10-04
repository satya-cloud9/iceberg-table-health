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

### What the stack runs (checked 2026-10-04)

| Runs | Where | Memory |
|---|---|---|
| Floci (S3, Glue; DynamoDB next, for advisor state) | `lakehouse-floci` container | ~190 MiB |
| Emulated EKS: one k3s node | `floci-eks-lakehouse-aws` container | ~1 GiB idle |
| Image registry for the Spark image | `floci-ecr-registry` container | ~20 MiB |
| Spark Operator (controller + webhook) | `spark-operator` namespace | ~130 MiB |
| Spark jobs, one driver + one executor at a time | `spark-jobs` namespace, 8 GiB quota | while running |

Nothing else is needed: Nessie, Kestra, Trino, MinIO, Postgres and the
observability stack belong to the base repo's platform and tenant stages,
which this flavor never applies. Trino comes back only for the Trino facts
source, as one pod pointed at Floci's Glue. Query evidence from Trino or dbt
is read from exported files, not from running services.

Finished jobs delete themselves a day after they end (`JOB_TTL_SECONDS`,
default 86400). `make gl-clean` deletes them now; `make gl-status` shows what
is running and how much memory it uses.

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

### s11 caught a quoting bug

First run: 11/12 PASS; s11 kept `SMALL_FILES` on `o'neil` after the fix.
plan.py quoted it SQL-standard style as `'o''neil'`, but Spark SQL treats
that as two adjacent literals and concatenates them into `'oneil'`, which
matches nothing. Spark escapes with backslashes: `'o\'neil'`, and because
the `where` sits inside the CALL's double-quoted string the backslash is
escaped once more there. Verified locally with Spark through both parse
layers for `o'neil`, `a/b`, `x=y`, a backslash and a double quote.

Note for the Trino port: Trino is the opposite (standard SQL: `'o''neil'`
is correct, backslash is not an escape), so the predicate builder needs a
dialect switch there.

```bash
make gl-image
make gl-plan T=s11 APPLY=1      # latest scan still shows SMALL_FILES on o'neil
make gl-scan                    # s11 PASS (after fix)
```

## GL2.5g — Copy-on-write churn (s12)

Copy-on-write `MERGE`/`UPDATE`/`DELETE` rewrites every file holding a changed
row. Files stay well-sized, so no file-layout rule fires, but a 50-row merge
spread across a table can rewrite most of it on every commit.

**Metric W2** (from `.snapshots`, cheap): over the recent commits, overwrite
commits that removed data files (copy-on-write rewrites, INSERT OVERWRITE,
full refreshes; compaction is `replace` and merge-on-read deltas remove no
data files, so neither counts): their count, the average share of the
table's files each one removed, and bytes removed in 24 h ÷ table size
(`table_turnover_24h`).

**`REWRITE_CHURN`** (write configuration, approval): >= 5 such commits,
average share >= 30%, turnover >= 2x in 24 h. Suggested SQL: switch the
merge/update/delete modes to merge-on-read; alternatives in the remedy:
narrow each merge (partition predicate, dbt `incremental_predicates`) or sort
on the merge key. While it's active, `SMALL_FILES`/`SCATTERED_SMALL_FILES`
on the table become **advisory** (shown, held, never planned): bigger files
would make every copy-on-write merge rewrite more bytes.

**s12_cow_merge_churn**: 30 days, then 15 copy-on-write MERGEs of 50 random
rows (each rewrites ~80% of the day files; ~12x turnover). Before: only
`REWRITE_CHURN`. The fix needs approval, so `scenario_step.py s12-mor` plays
the approver: switches to merge-on-read, expires the old snapshots (the
copy-on-write commits would otherwise stay in the churn window), runs 10
merge-on-read MERGEs of 1,000 rows, waits out the hot window, and records the
approval in `glue.ops.actions`. After: no churn; `DELETE_BUILDUP` instead
(plus small files from the merged rows) - work scheduled compaction handles.

```bash
make gl-image
make gl-test-tables TT_ARGS="--only s12"      # waits out the hot window
make gl-scan                                  # s12 PASS (before): REWRITE_CHURN
make gl-plan T=s12                            # ASK: the merge-on-read ALTER
make gl-step STEP=s12-mor                     # approve + more merges (~6 min)
make gl-scan                                  # s12 PASS (after fix): DELETE_BUILDUP
make gl-plan T=s12 APPLY=1                    # optional: compaction now handles it
```

Note for the work platform: Trino's Iceberg connector writes position
deletes for row-level changes (merge-on-read) whatever the table's
write.*.mode says, as far as I know; copy-on-write churn there would come
from Spark-based writers. Check against their Trino version.

## GL2.5h — Remaining physical gaps (s13–s16)

| Table | Shape | Before | Fix |
|---|---|---|---|
| `s13_oversized_file` | one day written as a single ~20 MB file | `OVERSIZED_FILES` on that day | auto: binpack splits it toward the target |
| `s14_spec_evolution` | `days()` evolved to `months()` mid-life | `MIXED_SPEC` | approval: `rewrite_data_files` with `rewrite-all` into the current spec |
| `s15_metadata_retention` | `previous-versions-max` 5, 12 commits, no auto-delete | `UNBOUNDED_RETENTION` (+ orphaned metadata.json) | approval: `delete-after-commit` on; `remove_orphan_files` for what's already left |
| `s16_orphan_files` | stray objects under the table location | `ORPHAN_FILES` | approval: `remove_orphan_files` |

**Orphan scan (O1).** Lists every object under the table location through the
table's FileIO (S3 prefix listing, with a trailing `/` so sibling tables
don't match) and subtracts everything a retained snapshot or metadata version
references: `all_files` (data and delete files), `all_manifests`, manifest
lists, `metadata_log_entries`, the current metadata.json and statistics
files. Objects younger than `orphan_min_age_minutes` are skipped because they
may belong to a commit in flight: 3 minutes at test scale, days in production
(Iceberg's own default is 3 days). It is the most expensive probe, so it can
be switched off (`orphan_scan`); the scan-cost work runs it less often.

**`UNBOUNDED_RETENTION` fixed.** It compared metadata versions to
`previous-versions-max`, but Iceberg caps the metadata log at that number, so
it could never fire. It now fires when the log is full and
`delete-after-commit` is off: from then on every commit leaves the oldest
metadata.json behind in storage. Those leftovers are what the orphan scan
finds.

**Approving a suggestion.** `make gl-plan T=<tables> APPROVE=<SYMPTOM,...>`
runs the ASK statements for those symptoms and records them in
`glue.ops.actions` as `approved:<symptom>`, so the scorecard switches the
table to its `after` expectations. `remove_orphan_files` gets
`prefix_listing => true` (FileIO listing instead of Hadoop's); if this
Iceberg doesn't know the argument, it is retried without.

The `remove_orphan_files` procedure refuses a cutoff younger than 24 hours
(`Cannot remove orphan files with an interval less than 24 hours`). The test
setup uses `orphan_min_age_minutes` 3, so plan.py then runs the same cleanup
through Iceberg's action API (`SparkActions.deleteOrphanFiles(...).olderThan(...)`),
which has no floor; the actions row records that statement. With a production
value (days) the CALL is used, and the 24-hour floor is a guard worth keeping.

**Fresh location per build.** `DROP TABLE ... PURGE` only deletes what the
dropped table still references, so the builder now gives every build its own
location (`<table>-<timestamp>`). Rebuild all tables once after this patch so
no table carries leftovers from earlier builds into the orphan scan.

**Spec evolution in the planner.** After evolution, old-spec partition keys
also carry the new field as null; a null next to a set field on the same
source column is now ignored instead of becoming `IS NULL`.

```bash
make gl-image
make gl-test-tables                     # rebuild everything once (fresh locations)
make gl-scan                            # s0-s16 PASS (before)
make gl-plan T=s13 APPLY=1
make gl-plan T=s14 APPROVE=MIXED_SPEC
make gl-plan T=s15 APPROVE=UNBOUNDED_RETENTION,ORPHAN_FILES
make gl-plan T=s16 APPROVE=ORPHAN_FILES
make gl-scan                            # s13-s16 PASS (after fix)
```

## GL2.5i — Maintenance falling behind

Two rules read history instead of a single scan, matched by table UUID (a
rebuilt table starts a new history):

- **`MAINTENANCE_LAG`**: over the last 3 scans, total excess files (new
  `excess_files_total`), delete files or data manifests rose every time and
  grew by >= 50%, ending above a floor (10 excess files / 5 delete files / 10
  manifests). The evidence lists the values and how many actions ran in that
  span, so "maintenance ran but can't keep up" and "maintenance isn't
  running" read differently.
- **`MAINTENANCE_FAILING`**: the last 2 actions of the same kind on a table
  both failed. Evidence carries the last error.

Both are approval-level alerts. Thresholds: `trend_*` and `fail_streak` in
`health.json`; `trend_min_span_minutes` is 0 at test scale (production would
want hours, so three quick scans don't count as a trend).

History is bounded by the scan's own timestamp inside SQL (not a Python
datetime literal, which shifts with the driver's time zone).

**s17_growing** plus `make gl-step STEP=grow` (10 more small files on the
fragmented day, then a hot-window wait):

```bash
make gl-image
make gl-test-tables TT_ARGS="--only s17"
make gl-scan                    # s17: SMALL_FILES (excess 11)
make gl-step STEP=grow
make gl-scan                    # excess 21
make gl-step STEP=grow
make gl-scan                    # excess 31: MAINTENANCE_LAG in the per-table output
```

## GL2.5j — Scan cost at scale

Metadata tables are views over metadata files: `snapshots`, `history`,
`properties` and `metadata_log_entries` read metadata.json only (cheap);
`manifests` reads the manifest list (cheap); `files`, `entries`, `partitions`
read every manifest of the current snapshot (grows with file count);
`all_files` / `all_entries` read the manifests of every retained snapshot
(most expensive). The scan now avoids the expensive ones when they can't
change the answer:

- **Unchanged tables are reused.** Iceberg never rewrites metadata in place,
  so the metadata.json location is a fingerprint. If it matches the table's
  last scan, the scan copies that scan's partition and table rows and only
  refreshes what moves with the clock: partition ages (old age + time since
  that scan) and the snapshot metrics (from metadata.json). Cost per
  unchanged table: one table load. `scan_mode` in `table_metrics` says
  `full`, `reused` or `failed`; `scan_seconds` records the time per table.
- **Hot window only when it can matter.** `all_entries` (per-partition last
  writer commit) is read only if the table's last writer commit, from
  `snapshots`, is inside the hot window. Otherwise every partition gets the
  table-level age, a safe lower bound.
- **Interval-gated probes.** Retained bytes (`all_files`) and the orphan
  listing run when due: `retained_bytes_every_hours`,
  `orphan_scan_every_hours` (0 = whenever the table changed, the test
  setting; ~24 in production). Between runs their last values are carried.
- **Actions that commit nothing still count as a change.**
  `remove_orphan_files` deletes objects without writing a new metadata.json,
  so the fingerprint can't see it. Any row in `glue.ops.actions` for the
  table (by UUID) newer than its last orphan listing re-runs the listing on
  the next scan; `scan_mode` then says `reused+orphans`.
- `make gl-scan SCAN_ARGS=--full` measures everything from scratch.

Check it: run `make gl-scan` twice with no writes in between. The second run
should report every table `reused` and finish much faster, with the same
scorecard.

**Not done yet (next):** rescanning only the changed partitions of a large
changed table (from the manifests added since the last scan), and finding
changed tables in bulk from Glue `GetTables` metadata locations instead of
loading each table - that matters most for the Trino port at 1,000+ tables.

## GL2.5k — Rollback record and per-table opt-out

**Rollback record.** Every statement plan.py (or a scenario step) runs now
records the table's current snapshot id before and after it in
`glue.ops.actions` (`snapshot_before`, `snapshot_after`), plus a ready
`rollback_hint`, e.g.

    CALL glue.system.rollback_to_snapshot('demo.s0_small_appends', 512...)

It is only written when the statement made a new snapshot (property changes
and expire_snapshots don't). Two limits are in the hint itself: rolling back
also undoes anything writers committed after that snapshot, and it is
impossible once the snapshot has been expired. Older actions tables gain the
columns automatically.

    SELECT started_at, table_name, kind, snapshot_before, snapshot_after, rollback_hint
    FROM glue.ops.actions ORDER BY started_at DESC

**Per-table opt-out.** The table property `advisor.mode`, set by the table's
owner, wins over the config default `advisor_mode`:

| advisor.mode | What plan.py does |
|---|---|
| `auto` (default) | auto steps run with `APPLY=1` |
| `approve-only` | auto steps are shown as ASK and run only with `APPROVE=<symptom>` |
| `off` | findings are still recorded; nothing runs on the table, not even with APPROVE |

```bash
# e.g. hand s0 to its owner for sign-off
make gl-sql Q="ALTER TABLE glue.demo.s0_small_appends SET TBLPROPERTIES ('advisor.mode' = 'approve-only')"
make gl-scan                      # the property change is a new metadata version, so s0 is rescanned
make gl-plan T=s0 APPLY=1         # s0: HOLD advisor.mode=approve-only, nothing runs
make gl-plan T=s0 APPROVE=SMALL_FILES
```

## GL2.5l — Reading the scorecard

Every test table is scored in one of two phases, and PASS means something
different in each:

| RESULT | PHASE | Means |
|---|---|---|
| PASS | detect | No fix has run yet; the scan found the problem the table was built with ("found as built: ..."). A healthy result here would be a FAIL: detection missed it. |
| PASS | fixed | A fix ran (a successful row in `glue.ops.actions` for this table UUID) and the scan now finds the table healthy. |
| FAIL | detect | Detection is wrong: something missing or unexpected. |
| FAIL | fixed | The fix ran but the problem (or another) is still there; `remaining fix:` says what would clear it. |
| STALE | detect | Too late to judge (s3's hot window); rebuild and scan straight after. |

Rows are grouped by phase, failures first in each group. Fixed rows show
`last fix:` (the latest successful action and its time); detect rows show
`next:` (auto fix and the command, or which symptoms need approval). The
header splits the count: detected as built, fixed and verified, stale, failed.
`glue.ops.scorecard` gains a `last_fix` column.

```
=== Scorecard for scan scan-... ===
17/18 pass: 12 detected as built, 5 fixed and verified, 1 stale

RESULT  PHASE   TABLE                      OUTCOME
-- Fixed: a fix ran; the scan must now find the table healthy ---------------
PASS    fixed   s16_orphan_files           healthy after fix
                                            last fix: approved:ORPHAN_FILES ok 2026-10-03 13:12 UTC
-- Detect: no fix yet; the scan must find the problem the table was built with
PASS    detect  s0_small_appends           found as built: SMALL_FILES x2, SNAPSHOT_BUILDUP
                                            next: auto fix (make gl-plan T=s0 APPLY=1)
```

## GL2.5m — Incremental scan, family 1: the snapshot ledger (shadow)

Design: roadmap, "Incremental metadata scan: design". This patch is family 1;
it runs in **shadow**: the full path still decides every finding, and the
ledger's numbers are compared against it on every scan.

What it keeps (new `glue.ops` tables, written once per scan):

| Table | Holds |
|---|---|
| `snapshot_log` | One row per snapshot ever seen, kept after `expire_snapshots`: operation, summary numbers, and the gap to the previous writer commit, computed once when the snapshot is first seen |
| `ledger_state` | Per table UUID: the watermark (last snapshot ingested, last writer commit, the current snapshot at that scan) |
| `commit_gap_hist` | Gap counts per table, day and bucket (1, 2, 5, 10, 15, 30, 60, 120, 360, 720, 1440, 2880, 10080 min, longer); the 95th percentile sums 30 days of buckets |
| `incremental_check` | Per scan, table and metric: full value, ledger value, agree |

Per table and scan: read the snapshots from the already loaded table (no
extra I/O), check lineage and gaps, ingest only snapshots newer than the
watermark, compute the snapshot metrics from the ledger, compare.

Events in `table_metrics.ledger_event`: `bootstrap` (first sight or new UUID),
`new`, `unchanged`, `lineage-break` (the previously current snapshot is no
longer an ancestor: rollback, replaced table), `ledger-gap` (the oldest new
snapshot's parent was never ingested: snapshots expired between scans; the gap
before it is stored as unknown, not invented). New columns:
`commit_gap_p95_min` (bucket edge), `commit_gaps_window`,
`ledger_new_snapshots`, `ledger_event`.

Config (`health.json` → `incremental`): `snapshot_ledger` off | shadow | on;
in `on`, reused tables skip the `.snapshots` read and use the ledger, a random
`spot_check_share` of tables and any table not compared for
`reconcile_every_days` are still compared, and a table that disagrees falls
back to the full values for that scan. `retention_days` (365) bounds
`snapshot_log`; check rows are kept 30 days.

Each scan ends with:
```
=== Incremental check (shadow, snapshot ledger): 21/21 tables agree; 412 new snapshots ingested; bootstrap 21 ===
```
and one line per disagreement (table, metric, full vs ledger, note).

Shadow test sequence (each edge case, then a scan):
```bash
make gl-image
make gl-scan                          # bootstrap: every table ingested, all agree
make gl-scan                          # unchanged: 0 new snapshots, all agree
make gl-step STEP=grow && make gl-scan          # new: 10 snapshots on s17, gaps stored
make gl-plan T=s0 APPLY=1 && make gl-scan       # replace commits: not writer commits
make gl-step STEP=rollback && make gl-scan      # s17 lineage-break, still agrees
make gl-step STEP=expire-gap && make gl-scan    # s17 ledger-gap, first gap unknown
make gl-sql Q="SELECT table_name, metric, full_value, ledger_value, note FROM glue.ops.incremental_check WHERE NOT agree ORDER BY checked_at DESC"
```
Family 1 can switch to `on` after 5 clean scans in a row covering these.

## GL2.5n — Incremental scan, family 2: partition activity (shadow); family 1 on

**Family 1 is on.** The snapshot ledger passed shadow on 2026-10-03 (5 clean
scans covering grow, compaction, rollback and an expiry gap). Reused tables no
longer read `.snapshots`; the ledger supplies the snapshot metrics. 5% of
tables per scan, and any table not compared for 7 days, are still compared;
a disagreement makes that table use the full values for that scan.

**Family 2: partition activity, in shadow.** For every snapshot not processed
yet, only the manifests that snapshot wrote are read (`addedDataFiles`,
`removedDataFiles`, `addedDeleteFiles`, `removedDeleteFiles` via the Java API),
and kept per partition:

| Table | Holds |
|---|---|
| `partition_activity` | One row per (snapshot, partition): data / delete files and bytes added and removed, and the batch's lateness (hours after the partition's time range ended; day, hour, month, year and identity-date partitions) |
| `partition_state` | Per (table, partition): last writer write, last compaction, reopen count (written again after a compaction); one MERGE per scan |
| `lateness_hist` | Lateness counts per table, day and bucket (1, 6, 24, 48, 72, 168, 720 h, longer) |
| `activity_state` | Per table: the last snapshot processed |

Partition keys are built to match `to_json(partition)` exactly (field id
order, nulls left out, decimals keep their scale), so they join with
`partition_metrics`.

New `table_metrics` columns: `activity_new_snapshots`, `activity_event`,
`lateness_p95_h`, `lateness_batches_window`, `reopened_partitions`,
`hot_partitions_ledger` (live partitions whose last writer write is inside the
hot window, from the ledger).

Shadow checks (family `partition_activity` in `incremental_check`), against a
full `all_entries` read that runs only in shadow:

- `activity`: per retained snapshot and partition, files and bytes added and
  removed must be equal
- `last_write`: last writer write per live partition must be equal (a
  partition last written before every retained snapshot is known only to the
  ledger: agrees)
- `lateness_p95_h`: the histogram bucket must contain the exact value

Each scan prints:
```
=== Incremental check (shadow, partition activity): 21/21 tables agree; 412 snapshots read; bootstrap 21 ===
```
Shadow scans read `all_entries` twice per table, so they are slower; that
goes away when family 2 is switched on. Family 2 changes no finding yet: the
hot window still comes from the full path. Family 3 (learned windows, settle
rule) will read `partition_state` and `lateness_hist`.

Shadow sequence for family 2:
```bash
make gl-image
make gl-scan                                   # bootstrap: every table's retained snapshots read
make gl-scan                                   # unchanged: 0 snapshots read
make gl-step STEP=grow && make gl-scan         # new files in s17's day partition
make gl-plan T=s0 APPLY=1 && make gl-scan      # compaction: removed + added, last_compaction set
make gl-test-tables TT_ARGS="--only s1" && make gl-scan   # late arrivals: lateness > 0
make gl-scan FRESH_S3=1                        # hot partition: hot_partitions_ledger = 1 on s3
```

## GL2.5o — Incremental scan, family 3: learned windows (shadow)

Two waits are learned per table instead of one fixed number (`windows.py`):

| Window | Learned from | Rule | Bounds |
|---|---|---|---|
| Hot window | Family 1's commit-gap histogram | 2 × the 95th-percentile gap between writer commits | at least `hot_partition_minutes` (floor), at most `hot_cap_minutes` (24 h); needs `min_samples` (20) gaps, else the configured value |
| Settle window | Family 2's lateness histogram | the 99th-percentile lateness (hours after a partition's time range ended) | at most `settle_cap_hours` (7 days); time-based partitions only; needs 20 batches, else none |

New rule, **SETTLING** (defer): a partition whose time range ended less than
the settle window ago still receives late data, so a small-files-only
compaction waits, unless the excess is already `settle_big_factor` (3) × the
threshold. Partitions with delete buildup are not held. `plan.py` holds
SETTLING partitions like hot ones.

The learned variant takes each partition's age from family 2's
`partition_state` (last writer write), not from the full path: the full path
only computes per-partition age inside the configured window, so under a longer
learned window every partition would get the table-level age and look hot.

**Shadow:** detection runs the rules twice, configured windows (these decide)
and learned windows, and records what would change in `incremental_check`
(family `learned_windows`, `agree` = no change). Each detect prints:
```
=== Learned windows (shadow): 3/21 tables would change ===
  s1_late_arrivals           hot 3 min [config floor], settle 168.0 h [learned (capped)]
                               {"occurred_at_day":"2026-09-30"}: SMALL_FILES -> SETTLING
```
These aren't errors: each change is a decision to review. Family 3 goes on
(`incremental.learned_windows: on`) once every change on the test tables is
justified; the scenario expectations get updated then.

New `table_metrics` columns: `lateness_p99_h`, `hot_window_min`,
`hot_window_source`, `settle_window_h`, `settle_window_source`. Reopened
partitions (written again after a compaction) are already counted per table in
`reopened_partitions` (family 2).

Run it: `make gl-image && make gl-scan`, then
```bash
make gl-sql Q="SELECT table_name, ledger_value AS windows, note AS changes FROM glue.ops.incremental_check WHERE family = 'learned_windows' AND NOT agree AND scan_id = (SELECT max(scan_id) FROM glue.ops.incremental_check)"
```

### GL2.5o+ — Settle rule revised: bounded re-compaction, LATE_ARRIVALS, possible full refreshes

The first SETTLING rule (wait unless excess ≥ 3× the threshold) looked only at
file counts. Re-compaction is often cheap (binpack rewrites only files outside
the target size range, so a partition bigger than the target only rewrites its
late small files), while waiting can hurt reads badly on a partition people
query. Until query evidence (2.5d++) can weigh read cost against rewrite cost,
the interim policy keeps rewrites bounded instead of waiting outright:

| Inside the settle window | Outcome |
|---|---|
| Delete buildup | Never waits |
| Fewer than `max_settle_compactions` (2) compactions since the partition ended | Compact (SMALL_FILES): the first compaction, plus one interim |
| That many already | **SETTLING**: wait until the partition has settled |
| Partition settled | Compact normally |

Each settle-window finding carries its reasoning: hours since the partition
ended, compactions so far, `p_more_late` (upper bound on the chance more late
batches still land, from the lateness histogram) and `rewrite_bytes_now`
(bytes binpack would rewrite: files outside the size range, new
`partition_metrics.rewrite_bytes`). Once query evidence exists, the decision
becomes: compact when queries/h × extra-file cost × hours left exceeds
rewrite bytes × p_more_late.

**LATE_ARRIVALS** (approval, learned windows only): 99th-percentile lateness ≥
`late_arrivals_hours` (24) or ≥ `late_reopened_partitions` (3) partitions
reopened. Remedies point upstream: batch late rows, narrow the incremental
lookback, stage late data and merge once, or partition by ingestion date and
sort by event time.

**Batch labels.** Engines don't mark reloads or full refreshes, so each
(snapshot, partition) batch is labelled from what the commit did:

| Label | Rule | Lateness, reopens? |
|---|---|---|
| `compaction` | a `replace` commit | no |
| `possible_full_refresh` | the commit covered > 90% of live partitions and replaced > 80% of the table's files and > 80% of its bytes (`refresh_*` thresholds) | no |
| `rewrite` | files removed in that partition (copy-on-write, overwrite) or delete files only | no |
| `possible_backfill` | append-only, later than the table's 99th-percentile lateness as it stood before this scan (needs 20 samples) and at least a median partition's bytes | no |
| `on_time` / `late` | append-only, before / after the partition's range ended | **yes** |
| `append` | append-only into a partition with no time range | — |

Only `on_time` and `late` batches feed the lateness histogram, so the settle
window measures real late data; only a `late`/`on_time` write after a
compaction counts as a reopen. Every writer write still moves the partition's
last write (the hot window). Labels are stored on `partition_activity.label`;
`partition_state.last_write_label` keeps the latest per partition.

What the labels drive (all in the learned-windows variant, shadow):

- **Trend reset:** after a possible full refresh, `MAINTENANCE_LAG` only
  compares scans taken since (a refresh replaces everything).
- **FREQUENT_FULL_REFRESH** (advisory, never acted on): ≥
  `frequent_full_refresh_min` (4) possible full refreshes in 30 days on a table
  of ≥ `frequent_full_refresh_min_bytes` (1 GiB). It may be deliberate: the
  table property `advisor.ack = FREQUENT_FULL_REFRESH` keeps it recorded as
  `acknowledged` and out of plans and rankings (any symptom can be acknowledged
  this way).
- **Full copies kept by old snapshots:** more than `keep_full_copies` (1)
  retained possible full refreshes raise `SNAPSHOT_BUILDUP` (evidence: copies
  and bytes kept beyond the current table), so expiry runs sooner. The plan's
  `expire_snapshots` keeps the snapshot before the latest possible full
  refresh (older_than = its time), so a bad refresh can still be rolled back.
- **SMALL_FILES remedy:** when a partition's files came from a possible full
  refresh, the remedy points at the refresh job's file sizing, not just
  compaction. There is no separate small-file finding: one small file per
  partition has no excess and isn't flagged; many undersized partitions is
  `OVER_PARTITIONED`, which depends on cross-partition queries.
- **LATE_ARRIVALS evidence:** possible backfills and possible full refreshes in
  the last 30 days.

New `table_metrics` columns: `possible_full_refreshes_30d`,
`possible_backfill_batches_30d`, `last_full_refresh_ms`,
`full_refresh_avg_bytes`, `retained_full_copies`, `pre_refresh_snapshot_ms`.
Writer attribution (2.5d+) can later turn "possible" into "confirmed".

### GL2.5o++ — Sample minimums, adaptive lookback, idle gaps, cold start

The first family 3 shadow review found three wrong changes and fixed them here:

- **LATE_ARRIVALS needs enough batches.** The lateness trigger (p99 ≥ 24 h)
  needs `min_batches` (20) batches in the lookback; a p99 of 4 batches is just
  the worst of 4. The reopen trigger (≥ 3 reopened partitions) is a count and
  needs none.
- **Cold start possible backfill.** Until a table has `min_batches` batches
  (no p99 baseline yet), an append at least a median partition's size and later
  than `cold_start_backfill_hours` (168) is labelled `possible_backfill`, so a
  historical load doesn't become the table's lateness. A baseline p99 beyond
  the last bucket (> 720 h) means nothing is unusually late.
- **Idle gaps don't stretch the hot window.** Commit gaps in buckets entirely
  above `idle_gap_factor` (10) × the median gap (nights, weekends, a paused
  job) are left out before the p95; `min_gaps` (20) counts what's left.
- **Adaptive lookback.** Both histograms are read over `histogram_days` (30);
  a table with fewer than `min_batches` / `min_gaps` samples there reaches
  further back, whole days at a time, until it has them or hits
  `lookback_max_days` (365). `adaptive_lookback: false` keeps a fixed window.
  The p_more_late figure in detect uses each table's own lookback. The family 2
  shadow comparison (lateness p95) and family 1's commit_gap_p95_min keep the
  fixed 30 days, so their comparisons are unchanged.

`min_samples` is replaced by `min_batches` and `min_gaps` (the old key is still
read as the default). Ops histograms are kept for `lookback_max_days`.

New `table_metrics` columns: `lateness_p99_batches`, `lateness_lookback_days`,
`hot_gap_p95_min`, `hot_gaps_used`, `idle_gaps_ignored`, `gap_lookback_days`.
The shadow report line shows them:
`hot 6 min [learned; 140 gaps / 30 d, 12 idle gaps ignored], settle 24 h [learned; 35 batches / 30 d]`.

**Loads into an empty table** (checked against Iceberg's snapshot summary: the
table's data files just before the commit = total − added + deleted = 0):
- with an earlier snapshot (a parent, or any older snapshot: `CREATE OR REPLACE
  TABLE AS` starts a new lineage with no parent and totals from 0) → the insert
  after a truncate or a table replace → `possible_full_refresh`, so the
  refresh-aware rules apply.
- with none → the table's first load → late batches in it are
  `possible_backfill`.
A reload spread over several commits is only caught on its first commit; writer
attribution (2.5d+) closes that.

Batches already labelled by the earlier version keep their labels, so reset the
family 2 tables once after this change (they rebuild from retained snapshots on
the next scan):

```bash
make gl-sql Q="DROP TABLE glue.ops.partition_activity; DROP TABLE glue.ops.partition_state; DROP TABLE glue.ops.lateness_hist; DROP TABLE glue.ops.activity_state"
```

### Scorecard: time-limited scenarios

An expectation can set `stale_after_hours`: past that many hours since the
table's last writer commit, a failing detect-phase result is STALE (with
"rebuild it" in the note), not FAIL. s12 uses 24: `REWRITE_CHURN` needs table
turnover over the last 24 h, so the churn the builder created ages out a day
later. (Production note: a 24 h turnover window misses a once-a-day
copy-on-write job rewriting half the table; the window should follow the
writer's cadence. On the roadmap as a calibration item.)

## GL2.6a — Plan fixes from the traceability review (group 1)

Five corrections to how statements are built, found while tracing each action
to its metrics and facts (Traceability tab of the implementation strategy doc).

- **Target size: the writer's property wins.** `resolve_target` now takes the
  table's `write.target-file-size-bytes` first, then a per-table config entry,
  then the default. Writers size their own files by the property, so the
  advisor judges and compacts by the same number. The source is recorded in
  `table_metrics.target_source`.
- **Explicit rewrite band.** `rewrite_data_files` gets `min-file-size-bytes` =
  small_file_ratio × target and `max-file-size-bytes` = oversized_file_ratio ×
  target, so the rewrite picks exactly the files the scan flagged even if the
  ratios change (today they equal Iceberg's own defaults, 75% and 180%).
- **The sort order is honored.** A table with a sort order is compacted with
  `strategy => 'sort'` (Iceberg uses the table's order); binpack would
  concatenate sorted files unsorted and widen every file's value range.
  Unsorted tables keep binpack.
- **`rewrite_position_delete_files` is scoped** with the same `where` as the
  data rewrite, instead of the whole table.
- **POOR_CLUSTERING's suggestion** sets `WRITE ORDERED BY` only when the table
  has no sort order yet.

New scenario **s18_sorted_small**: sort order on customer_id and one day in 200
small files that all span the full customer range. Before: SMALL_FILES and
POOR_CLUSTERING (its filter column is declared with evidence `observed`, so the
finding is scored). After `make gl-plan T=s18 APPLY=1`: healthy, because the
sort rewrite writes files with disjoint ranges; a binpack rewrite would leave
POOR_CLUSTERING.

```bash
make gl-image
JOB_TIMEOUT_MIN=20 bash scripts/run-job.sh py build_test_tables.py --only s18
make gl-scan                       # s18: SMALL_FILES, POOR_CLUSTERING (expected)
make gl-plan T=s18 APPLY=1         # strategy => 'sort'
make gl-scan                       # s18 after: PASS
```
