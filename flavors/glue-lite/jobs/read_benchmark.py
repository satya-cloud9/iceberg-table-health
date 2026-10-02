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

Usage (via scripts/run-job.sh py read_benchmark.py ...):
  read_benchmark.py --label before --days 2026-09-02,2026-09-03,2026-09-05
                    [--table glue.demo.s0_small_appends]
"""
import argparse
import os
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


def plan_timing(spark, table, ts_column, day, nxt, runs):
    """Median ms of Iceberg planFiles() for one day, and the data files it planned."""
    jvm = spark._jvm
    jt = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    E = jvm.org.apache.iceberg.expressions.Expressions
    lo = int((datetime.combine(date.fromisoformat(day), datetime.min.time()) - EPOCH).total_seconds()) * 1_000_000
    hi = int((datetime.combine(date.fromisoformat(nxt), datetime.min.time()) - EPOCH).total_seconds()) * 1_000_000
    expr = getattr(E, "and")(E.greaterThanOrEqual(ts_column, lo), E.lessThan(ts_column, hi))
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


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--table", default="glue.demo.events")
    p.add_argument("--days", default="2026-09-02,2026-09-03,2026-09-05",
                   help="healthy control first, then fragmented days")
    p.add_argument("--ts-column", default="occurred_at")
    p.add_argument("--runs", type=int, default=5)
    p.add_argument("--label", required=True, help="e.g. before / after")
    p.add_argument("--run-id", default=None)
    a = p.parse_args()

    run_id = a.run_id or gl.new_run_id(f"bench-{a.label}")
    spark = SparkSession.builder.appName(f"gl2-read-benchmark-{a.label}").getOrCreate()
    gl.ensure_ops_tables(spark)
    gl.ensure_columns(spark, f"{gl.OPS_NAMESPACE}.read_benchmarks", gl.OPS_TABLES["read_benchmarks"])
    health = gl.partition_health(spark, a.table, 128 * 1024 * 1024, 1).collect()

    results = []
    for day in [d.strip() for d in a.days.split(",") if d.strip()]:
        nxt = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
        where = (f"{a.ts_column} >= TIMESTAMP '{day} 00:00:00' AND "
                 f"{a.ts_column} < TIMESTAMP '{nxt} 00:00:00'")
        files = sum(r.data_files for r in health if f'"{day}"' in r.partition_key)
        plan_ms, planned = plan_timing(spark, a.table, a.ts_column, day, nxt, a.runs)
        print(f"{a.label:>7} {day} {'planning (planFiles)':<20} files={planned:<5} "
              f"median={plan_ms:8.1f} ms", flush=True)

        for name, sql in QUERIES.items():
            q = sql.format(t=a.table, w=where)
            spark.sql(q).collect()  # warm-up: class loading, first connections
            times = []
            for _ in range(a.runs):
                t0 = time.perf_counter()
                spark.sql(q).collect()
                times.append((time.perf_counter() - t0) * 1000)
            med = statistics.median(times)
            print(f"{a.label:>7} {day} {name:<20} files={files:<5} "
                  f"median={med:8.1f} ms  (min {min(times):.1f}, max {max(times):.1f})", flush=True)
            results.append((run_id, datetime.now(timezone.utc).replace(tzinfo=None), a.table,
                            a.label, day, name, int(files), a.runs,
                            round(med, 1), round(min(times), 1), round(max(times), 1),
                            round(plan_ms, 1), int(planned)))

    schema = spark.table(f"{gl.OPS_NAMESPACE}.read_benchmarks").select(
        *[c.strip().split()[0] for c in gl.OPS_TABLES["read_benchmarks"].split(",")]).schema
    spark.createDataFrame(results, schema).writeTo(f"{gl.OPS_NAMESPACE}.read_benchmarks").append()
    print(f"\nRecorded {len(results)} rows under run {run_id}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
