"""GL2.5g: scripted human steps for scenarios whose fix needs approval.

plan.py never runs approval actions; it prints them. To test what happens
after someone approves one, this job plays that person: it runs the approved
change, records it in glue.ops.actions (so the scorecard scores the table's
'after' expectations), then simulates the writer continuing.

Steps:
  s12-mor   approve the REWRITE_CHURN fix on s12_cow_merge_churn: switch
            MERGE/UPDATE/DELETE to merge-on-read, expire the old snapshots
            (the copy-on-write commits would otherwise stay in the churn
            window), then run 10 merge-on-read MERGEs of 1,000 rows each.
            Expected afterwards: no REWRITE_CHURN; DELETE_BUILDUP instead
            (and small files from the merges' new rows) - the work moves to
            scheduled compaction, which the advisor already handles.

  grow      add 10 more small files to s17_growing's fragmented day, as a
            writer would between maintenance runs that never come. Not
            recorded as an action (it's the writer, not the advisor). Run
            scan / grow / scan / grow / scan: excess files rise 11 -> 21 -> 31
            and MAINTENANCE_LAG fires on the third scan.

Usage (via scripts/run-job.sh py scenario_step.py ...):
  scenario_step.py s12-mor [--hot-minutes 3]
  scenario_step.py grow [--hot-minutes 3]
"""
import argparse
import os
import random
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession

import gl_common as gl
import probes
from build_test_tables import NS, S17_DAY, Builder, merge_random_rows
from plan import ACTIONS_DDL, action_row, current_snapshot, rollback_hint, run_sql


def record(spark, run_id, table, uuid, kind, symptom, stmt, status, dur, result, before=None, after=None):
    ns = gl.OPS_NAMESPACE
    spark.sql(f"CREATE TABLE IF NOT EXISTS {ns}.actions ({ACTIONS_DDL}) USING iceberg")
    gl.ensure_columns(spark, f"{ns}.actions", ACTIONS_DDL)
    schema = spark.table(f"{ns}.actions").schema
    row = action_row(schema, run_id=run_id, started_at=datetime.now(timezone.utc), table_name=table,
                     table_uuid=uuid, kind=kind, symptoms=symptom, statement=stmt, status=status,
                     duration_s=float(dur), result_json=result, snapshot_before=before, snapshot_after=after,
                     rollback_hint=rollback_hint(table.split(".", 1)[1], before, after))
    spark.createDataFrame([row], schema).writeTo(f"{ns}.actions").append()


def s12_mor(spark, args):
    table = f"{NS}.s12_cow_merge_churn"
    ident = table.split(".", 1)[1]
    run_id = gl.new_run_id("approved")
    uuid = probes.table_info(spark, table).get("uuid")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    steps = [
        ("approved:merge_on_read", f"ALTER TABLE {table} SET TBLPROPERTIES ("
                                   f"'write.merge.mode' = 'merge-on-read', "
                                   f"'write.update.mode' = 'merge-on-read', "
                                   f"'write.delete.mode' = 'merge-on-read')"),
        ("expire_snapshots", f"CALL glue.system.expire_snapshots(table => '{ident}', "
                             f"older_than => TIMESTAMP '{now}+00:00', retain_last => 1)"),
    ]
    for kind, stmt in steps:
        before = current_snapshot(spark, table)
        status, result, dur = run_sql(spark, stmt)
        after = current_snapshot(spark, table)
        print(f"  {kind}: {status} in {dur}s {result[:200]}", flush=True)
        record(spark, run_id, table, uuid, kind, "REWRITE_CHURN", stmt, status, dur, result, before, after)
        if status != "ok":
            raise SystemExit(f"{kind} failed; stopping")

    # The writer carries on, now merge-on-read: position deletes + new small files.
    lo, hi = spark.sql(f"SELECT min(CAST(substr(event_id, 5) AS BIGINT)) AS lo, "
                       f"max(CAST(substr(event_id, 5) AS BIGINT)) AS hi FROM {table}").collect()[0]
    rng = random.Random(1212)
    for i in range(10):
        merge_random_rows(spark, table, int(lo), int(hi) - int(lo) + 1, 1000, rng, f"mor-{i}")
        print(f"  merge-on-read MERGE {i + 1}/10 done", flush=True)
    wait = args.hot_minutes * 60 + 30
    print(f"  waiting {wait}s so the merged partitions are past the hot window", flush=True)
    time.sleep(wait)


def grow(spark, args):
    table = f"{NS}.s17_growing"
    b = Builder(spark)
    b.next_id = 50_000_000 + int(time.time()) % 1_000_000 * 100   # ids that don't clash with the build
    b.fragment(table, S17_DAY, commits=10, files_per_commit=1, rows_per_commit=300)
    n = spark.sql(f"SELECT count(*) AS n FROM {table}.files").collect()[0].n
    print(f"  {table}: 10 small files added ({n} data files now)", flush=True)
    wait = args.hot_minutes * 60 + 30
    print(f"  waiting {wait}s so the day is past the hot window", flush=True)
    time.sleep(wait)


STEPS = {"s12-mor": s12_mor, "grow": grow}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=sorted(STEPS))
    p.add_argument("--hot-minutes", type=int, default=3)
    a = p.parse_args()
    spark = SparkSession.builder.appName(f"gl25-step-{a.step}").getOrCreate()
    print(f"=== scenario step {a.step} ===", flush=True)
    STEPS[a.step](spark, a)
    print("Done. Run make gl-scan to score the table's 'after' expectations.", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
