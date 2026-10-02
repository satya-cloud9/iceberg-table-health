"""GL2: time a fixed set of read queries per day partition.

Run once before compaction and once after (different --label), then compare:
a healthy day is the control, a fragmented day is the subject. Each query
gets one warm-up run, then --runs timed runs; the median goes to
glue.ops.read_benchmarks together with how many files the partition had.

Planning time is measured separately: Iceberg's own planFiles() for the same
day filter, timed in the JVM (the step that reads manifests and picks files),
with the number of data files it planned. That isolates what manifest and
file counts cost before any data is read.

The queries read real column data on purpose. A bare count(*) is answered
from Iceberg's file statistics without opening data files, so it can't show
the small-file cost.

Timed runs are interleaved in a shuffled order (all day x query pairs, one
round per run) after one warm-up pass, so no day always runs first and
absorbs JVM warm-up.

--days all benchmarks the whole table with no filter: planning then reads
every manifest, which is where manifest count shows (s4).

After a non-"before" run, a before/after summary is printed against the
latest "before" run of the same table.

Usage (via scripts/run-job.sh py read_benchmark.py ...):
  read_benchmark.py --label before --days 2026-09-02,2026-09-03,2026-09-05
                    [--table glue.demo.s0_small_appends]
  read_benchmark.py --label before --days all --table glue.demo.s4_manifest_bloat
"""
import argparse
import os
import random
import statistics
import sys
import time
from datetime import date, datetime, timedelta, timezone

EPOCH = datetime(1970, 1, 1)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession

import gl_common as gl

QUERIES = {
    "distinct_customers": "SELECT count(DISTINCT customer_id) FROM {t} WHERE {w}",
    "agg_by_type": ("SELECT event_type, count(*), max(length(payload)) "
                    "FROM {t} WHERE {w} GROUP BY event_type"),
    "payload_filter": "SELECT count(*) FROM {t} WHERE {w} AND payload LIKE 'ab%'",
}


def _micros(day):
    return int((datetime.combine(date.fromisoformat(day), datetime.min.time()) - EPOCH)
               .total_seconds()) * 1_000_000


def plan_timing(spark, table, ts_column, day, nxt, runs):
    """Median ms of Iceberg planFiles() for one day (or the whole table), and
    the data files it planned."""
    jvm = spark._jvm
    jt = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    E = jvm.org.apache.iceberg.expressions.Expressions
    if day == "all":
        expr = E.alwaysTrue()
    else:
        expr = getattr(E, "and")(E.greaterThanOrEqual(ts_column, _micros(day)),
                                 E.lessThan(ts_column, _micros(nxt)))
    Iterables = jvm.org.apache.iceberg.relocated.com.google.common.collect.Iterables
    times, files = [], 0
    for i in range(runs + 1):                      # first run is a warm-up
        jt.refresh()
        t0 = time.perf_counter()
        tasks = jt.newScan().filter(expr).planFiles()
        try:
            files = int(Iterables.size(tasks))     # iterate inside the JVM
        except Exception:                          # fallback: iterate over py4j (slower)
            it, files = tasks.iterator(), 0
            while it.hasNext():
                it.next()
                files += 1
        tasks.close()
        if i:
            times.append((time.perf_counter() - t0) * 1000)
    return statistics.median(times), files


def summary(spark, table, run_id):
    """Before/after per day and query: this run vs the latest 'before' run of the table."""
    rb = f"{gl.OPS_NAMESPACE}.read_benchmarks"
    before = spark.sql(f"""
        SELECT max_by(run_id, measured_at) AS r FROM {rb}
        WHERE table_name = '{table}' AND label = 'before'""").collect()[0].r
    if not before or before == run_id:
        return
    print(f"\n=== Before/after for {table} (before = {before}) ===", flush=True)
    spark.sql(f"""
        SELECT a.day, a.query_name,
               b.files_in_partition AS files_before, a.files_in_partition AS files_after,
               b.plan_median_ms AS plan_ms_before, a.plan_median_ms AS plan_ms_after,
               b.median_ms AS ms_before, a.median_ms AS ms_after,
               round(b.median_ms / a.median_ms, 1) AS speedup
        FROM {rb} a JOIN {rb} b ON a.day = b.day AND a.query_name = b.query_name
        WHERE a.run_id = '{run_id}' AND b.run_id = '{before}'
        ORDER BY a.day, a.query_name""").show(100, truncate=False)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--table", default="glue.demo.events")
    p.add_argument("--days", default="2026-09-02,2026-09-03,2026-09-05",
                   help="comma-separated days (healthy control + fragmented days), or 'all'")
    p.add_argument("--ts-column", default="occurred_at")
    p.add_argument("--runs", type=int, default=5)
    p.add_argument("--label", required=True, help="e.g. before / after")
    p.add_argument("--run-id", default=None)
    a = p.parse_args()

    run_id = a.run_id or gl.new_run_id(f"bench-{a.label}")
    spark = SparkSession.builder.appName(f"gl2-read-benchmark-{a.label}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")         # keep the job output to the results
    gl.ensure_ops_tables(spark)
    gl.ensure_columns(spark, f"{gl.OPS_NAMESPACE}.read_benchmarks", gl.OPS_TABLES["read_benchmarks"])
    health = gl.partition_health(spark, a.table, 128 * 1024 * 1024, 1).collect()

    days = [d.strip() for d in a.days.split(",") if d.strip()]
    cases, info = [], {}
    for day in days:
        if day == "all":
            nxt, where = None, "TRUE"
            files = sum(r.data_files for r in health)
        else:
            nxt = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
            where = (f"{a.ts_column} >= TIMESTAMP '{day} 00:00:00' AND "
                     f"{a.ts_column} < TIMESTAMP '{nxt} 00:00:00'")
            files = sum(r.data_files for r in health if f'"{day}"' in r.partition_key)
        plan_ms, planned = plan_timing(spark, a.table, a.ts_column, day, nxt, a.runs)
        info[day] = (files, plan_ms, planned)
        print(f"{a.label:>7} {day} {'planning (planFiles)':<20} files={planned:<5} "
              f"median={plan_ms:8.1f} ms", flush=True)
        for name, sql in QUERIES.items():
            cases.append((day, name, sql.format(t=a.table, w=where)))

    for _, _, q in cases:                          # warm-up pass
        spark.sql(q).collect()
    times = {(d, n): [] for d, n, _ in cases}
    for r in range(a.runs):                        # interleaved, shuffled rounds
        order = list(cases)
        random.Random(r).shuffle(order)
        for d, n, q in order:
            t0 = time.perf_counter()
            spark.sql(q).collect()
            times[(d, n)].append((time.perf_counter() - t0) * 1000)

    results = []
    for d, n, _ in cases:
        ts, (files, plan_ms, planned) = times[(d, n)], info[d]
        med = statistics.median(ts)
        print(f"{a.label:>7} {d} {n:<20} files={files:<5} "
              f"median={med:8.1f} ms  (min {min(ts):.1f}, max {max(ts):.1f})", flush=True)
        results.append((run_id, datetime.now(timezone.utc).replace(tzinfo=None), a.table,
                        a.label, d, n, int(files), a.runs,
                        round(med, 1), round(min(ts), 1), round(max(ts), 1),
                        round(plan_ms, 1), int(planned)))

    schema = spark.table(f"{gl.OPS_NAMESPACE}.read_benchmarks").select(
        *[c.strip().split()[0] for c in gl.OPS_TABLES["read_benchmarks"].split(",")]).schema
    spark.createDataFrame(results, schema).writeTo(f"{gl.OPS_NAMESPACE}.read_benchmarks").append()
    print(f"\nRecorded {len(results)} rows under run {run_id}", flush=True)
    if a.label != "before":
        summary(spark, a.table, run_id)
    spark.stop()


if __name__ == "__main__":
    main()
