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

  rollback  (GL2.5m) roll s17_growing back one snapshot: the next scan must
            report a lineage break and the ledger must still agree.
  expire-gap (GL2.5m) 10 commits into s17, then expire all but the current
            snapshot before any scan: the next scan must report a ledger gap.

  rapid-commits (GL2.7a) rebuilds the scratch table live_rapid_commits with 40
            tiny appends, then scans and detects it: the writer findings
            (shadow) should show +SNAPSHOT_RATE with a suggested trigger.
            expire-gap followed by make gl-scan shows +HISTORY_LOST on s17.

  append-live (GL2.6b) a writer still appending: rebuilds two scratch tables,
            glue.demo.live_append_unpart (unpartitioned) and live_append_days
            (by day), each 1 healthy file + 6 small appends into 2026-09-03 (a
            day long ended), then scans and detects them in the same job, inside
            the hot window. Appends never hold a partition (the only hold is
            a conflicting commit), so both get SMALL_FILES:auto. The
            scratch tables aren't in expectations.json, so the scorecard ignores
            them. The scan covers only these two tables, so run make gl-scan
            afterwards before make gl-plan, which reads the latest scan.

  unpart-appends (item 12) rebuilds the scratch table live_unpart_appends:
            unpartitioned, target 1 MiB, sorted by customer_id, 24 appends
            (each a new hour of occurred_at, each spanning every customer_id),
            then scans and detects it: UNPARTITIONED_APPENDS (approval) and
            SMALL_FILES as a minor (never cold: minors only).

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
from plan import current_snapshot, rollback_hint, run_sql


def record(spark, run_id, table, uuid, kind, symptom, stmt, status, dur, result, before=None, after=None):
    """An action this step took, in the actions log and in action_state (what the
    next scan and detect read), through the configured backends."""
    import scan_state
    import state_store as ss
    ns = gl.OPS_NAMESPACE
    config = gl.load_config(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "health.json"))
    rec = dict(run_id=run_id, started_at=datetime.now(timezone.utc), table_name=table,
               table_uuid=uuid, kind=kind, symptoms=symptom, statement=stmt, status=status,
               duration_s=float(dur), result_json=result, snapshot_before=before, snapshot_after=after,
               rollback_hint=rollback_hint(table.split(".", 1)[1], before, after))
    log = ss.make_log_sink(spark, config, ns)
    log.append("actions", [rec])
    log.flush()
    store = ss.make_state_store(spark, config, ns)
    scan_state.ensure_tables(spark, ns, store)
    scan_state.record_actions(store, [rec], datetime.now(timezone.utc))
    store.flush()


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


def rollback(spark, args):
    """GL2.5m edge case: roll s17 back one snapshot. The rolled-back snapshot
    stays in metadata, so the next scan must report a lineage break, and the
    ledger (which keeps every snapshot by id) must still agree with the full path.
    Not recorded as an advisor action: it plays an operator, not the advisor."""
    table = f"{NS}.s17_growing"
    ident = table.split(".", 1)[1]
    cur = current_snapshot(spark, table)
    jt = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    parent = jt.currentSnapshot().parentId()
    if parent is None:
        raise SystemExit(f"{table}: current snapshot has no parent to roll back to")
    status, result, dur = run_sql(spark, f"CALL glue.system.rollback_to_snapshot('{ident}', {int(parent)})")
    print(f"  {table}: rollback {cur} -> {int(parent)}: {status} {result[:200]}", flush=True)


def expire_gap(spark, args):
    """GL2.5m edge case: snapshots expire before the ledger sees them. Adds 10
    commits to s17, then expires everything but the current snapshot straight
    away, so the next scan finds a new snapshot whose parent it never ingested
    (event ledger-gap) and must not invent the gap before it."""
    table = f"{NS}.s17_growing"
    ident = table.split(".", 1)[1]
    b = Builder(spark)
    b.next_id = 60_000_000 + int(time.time()) % 1_000_000 * 100
    b.fragment(table, S17_DAY, commits=10, files_per_commit=1, rows_per_commit=300)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    status, result, dur = run_sql(spark, f"CALL glue.system.expire_snapshots(table => '{ident}', "
                                         f"older_than => TIMESTAMP '{now}+00:00', retain_last => 1)")
    print(f"  {table}: 10 commits added, then expire_snapshots(retain_last 1): {status} {result[:200]}",
          flush=True)
    wait = args.hot_minutes * 60 + 30
    print(f"  waiting {wait}s so the day is past the hot window", flush=True)
    time.sleep(wait)


def append_live(spark, args):
    """Two scratch tables (not in expectations.json, so the scorecard ignores
    them and the fixed scenario tables keep their 'after' state): one
    unpartitioned, one by day. Each gets one healthy file, then 6 small appends
    (5 excess files, over min_excess_files) into a long-ended day; then the two
    are scanned and detected in this job, inside the hot window."""
    from datetime import date
    from detect_symptoms import run_detect
    from scan_metrics import run_scan
    b = Builder(spark)
    b.next_id = 70_000_000 + int(time.time()) % 1_000_000 * 100
    d = date(2026, 9, 3)
    names = []
    for name, spec in (("live_append_unpart", ""), ("live_append_days", "days(occurred_at)")):
        t = f"{NS}.{name}"
        b.create(t, spec, {})
        b.day(t, d, 100_000)
        b.fragment(t, d, commits=6, files_per_commit=1, rows_per_commit=500)
        names.append(name)
        print(f"  {t}: rebuilt; 1 healthy file + 6 small appends into {d}", flush=True)
    config = gl.load_config(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "health.json"))
    scan_id = run_scan(spark, NS, config, tables=names, report=False)
    run_detect(spark, scan_id, config, report=True)
    print("  look for the SMALL_FILES findings above: appends never hold a partition (no HOT_PARTITION); "
          "with tiering on, 6 fragments just written wait (minor at 8 fragments or once the oldest has waited), "
          "and a scan after the cold time (compaction.cold_hours) gives each a major", flush=True)


def rapid_commits(spark, args):
    """GL2.7a SNAPSHOT_RATE: a writer committing far too often. Rebuilds the scratch
    table glue.demo.live_rapid_commits (not in expectations.json) with one healthy
    file, then 40 tiny appends of one small file each, then scans and detects it in
    this job. At test scale (snapshot_rate_max_commits_24h 24) the writer findings
    (shadow) show +SNAPSHOT_RATE with a suggested trigger interval."""
    from datetime import date
    from detect_symptoms import run_detect
    from scan_metrics import run_scan
    b = Builder(spark)
    b.next_id = 80_000_000 + int(time.time()) % 1_000_000 * 100
    d = date(2026, 9, 3)
    t = f"{NS}.live_rapid_commits"
    b.create(t, "days(occurred_at)", {})
    b.day(t, d, 100_000)
    b.fragment(t, d, commits=40, files_per_commit=1, rows_per_commit=200)
    print(f"  {t}: rebuilt; 1 healthy file + 40 tiny appends", flush=True)
    config = gl.load_config(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "health.json"))
    scan_id = run_scan(spark, NS, config, tables=["live_rapid_commits"], report=False)
    run_detect(spark, scan_id, config, report=True)
    print("  look for '=== Writer findings (shadow)' above: live_rapid_commits +SNAPSHOT_RATE", flush=True)


def unpart_appends(spark, args):
    """Item 12, UNPARTITIONED_APPENDS: the scratch table glue.demo.live_unpart_appends
    (not in expectations.json), unpartitioned, target file size 1 MiB, sorted by
    customer_id, advisor.time-column = occurred_at. 16 appends of 20,000 rows, each
    an hour of event time (occurred_at follows arrival) but every customer_id
    (each append spans the whole key range), then 8 tiny appends (fragments).
    Large (over 8 target files) and appended to 24 times in 24 h: never cold, so
    the tier rule gives minors only; customer_id's overlap depth is about the file
    count, occurred_at's about 1, so UNPARTITIONED_APPENDS advises partitioning by
    days(occurred_at) and sorting within by customer_id."""
    from datetime import date
    from build_test_tables import epoch, events
    from detect_symptoms import run_detect
    from scan_metrics import run_scan
    b = Builder(spark)
    b.next_id = 90_000_000 + int(time.time()) % 1_000_000 * 100
    t = f"{NS}.live_unpart_appends"
    b.create(t, "", {"write.target-file-size-bytes": str(1 << 20), "advisor.time-column": "occurred_at"})
    spark.sql(f"ALTER TABLE {t} WRITE ORDERED BY customer_id")
    start = epoch(date(2026, 9, 3))
    hour = 0
    for rows, commits in ((20_000, 16), (500, 8)):
        for _ in range(commits):
            events(spark, b.take(rows), rows, start + hour * 3600, 3600).coalesce(1).writeTo(t).append()
            hour += 1
    b.finish(t)
    print(f"  {t}: rebuilt; 16 appends of 20,000 rows + 8 tiny appends, one event hour each, sorted by "
          f"customer_id, target 1 MiB", flush=True)
    config = gl.load_config(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "health.json"))
    scan_id = run_scan(spark, NS, config, tables=["live_unpart_appends"], report=False)
    run_detect(spark, scan_id, config, report=True)
    print("  look for live_unpart_appends above: UNPARTITIONED_APPENDS (approval; customer_id overlap depth ~ the "
          "file count, occurred_at ~ 1) and SMALL_FILES as a minor (never cold: a large unpartitioned table with "
          "continuous appends gets minors only)", flush=True)


STEPS = {"append-live": append_live, "unpart-appends": unpart_appends, "s12-mor": s12_mor, "grow": grow, "rollback": rollback, "expire-gap": expire_gap,
         "rapid-commits": rapid_commits}


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
