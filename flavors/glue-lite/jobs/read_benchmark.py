"""GL2: time a fixed set of read queries per day partition.

Run once before compaction and once after (different --label), then compare:
a healthy day is the control, a fragmented day is the subject. Each query
gets one warm-up run, then --runs timed runs; the median goes to
glue.ops.read_benchmarks together with how many files the partition had.

The queries read real column data on purpose. A bare count(*) is answered
from Iceberg's file statistics without opening data files, so it can't show
the small-file cost.

Usage (via scripts/run-job.sh py read_benchmark.py ...):
  read_benchmark.py --label before --days 2026-09-02,2026-09-03,2026-09-05
"""
import argparse
import os
import statistics
import sys
import time
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession

import gl_common as gl

QUERIES = {
    "distinct_customers": "SELECT count(DISTINCT customer_id) FROM {t} WHERE {w}",
    "agg_by_type": ("SELECT event_type, count(*), max(length(payload)) "
                    "FROM {t} WHERE {w} GROUP BY event_type"),
    "payload_filter": "SELECT count(*) FROM {t} WHERE {w} AND payload LIKE 'ab%'",
}


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
    health = gl.partition_health(spark, a.table, 128 * 1024 * 1024, 1).collect()

    results = []
    for day in [d.strip() for d in a.days.split(",") if d.strip()]:
        nxt = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
        where = (f"{a.ts_column} >= TIMESTAMP '{day} 00:00:00' AND "
                 f"{a.ts_column} < TIMESTAMP '{nxt} 00:00:00'")
        files = sum(r.data_files for r in health if f'"{day}"' in r.partition_key)

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
                            round(med, 1), round(min(times), 1), round(max(times), 1)))

    cols = [c.strip().split()[0] for c in gl.OPS_TABLES["read_benchmarks"].split(",")]
    spark.createDataFrame(results, cols).writeTo(f"{gl.OPS_NAMESPACE}.read_benchmarks").append()
    print(f"\nRecorded {len(results)} rows under run {run_id}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
