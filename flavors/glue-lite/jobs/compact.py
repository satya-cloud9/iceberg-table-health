"""GL2: compact chosen day partitions with Iceberg's rewrite_data_files.

One CALL per partition, each scoped with a `where` filter, with partial
progress on (file groups commit as they finish). Before/after file counts and
the procedure's own result row go to glue.ops.compaction_runs.

Usage (via scripts/run-job.sh py compact.py ...):
  compact.py --table glue.demo.events --days 2026-09-03,2026-09-05
             --ts-column occurred_at --target-mb 128 [--maintenance]
"""
import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession

import gl_common as gl


def files_and_bytes(spark, table, target_bytes, day):
    """Data files and bytes for the partition whose key mentions this day."""
    row = (gl.partition_health(spark, table, target_bytes, 1)
           .filter(f"partition_key LIKE '%\"{day}\"%'")
           .selectExpr("coalesce(sum(data_files), 0) AS f", "coalesce(sum(data_bytes), 0) AS b")
           .collect()[0])
    return int(row.f), int(row.b)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--table", default="glue.demo.events")
    p.add_argument("--days", required=True, help="comma-separated YYYY-MM-DD partitions")
    p.add_argument("--ts-column", default="occurred_at",
                   help="timestamp column the table is partitioned by days() on")
    p.add_argument("--target-mb", type=int, default=128)
    p.add_argument("--min-input-files", type=int, default=5)
    p.add_argument("--maintenance", action="store_true",
                   help="afterwards: rewrite_manifests + expire_snapshots (keep last 5)")
    p.add_argument("--run-id", default=None)
    a = p.parse_args()

    run_id = a.run_id or gl.new_run_id("compact")
    target_bytes = a.target_mb * 1024 * 1024
    ident = gl.catalog_relative(a.table)

    spark = SparkSession.builder.appName("gl2-compact").getOrCreate()
    gl.ensure_ops_tables(spark)

    results = []
    for day in [d.strip() for d in a.days.split(",") if d.strip()]:
        nxt = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
        where = (f"{a.ts_column} >= TIMESTAMP '{day} 00:00:00' AND "
                 f"{a.ts_column} < TIMESTAMP '{nxt} 00:00:00'")
        files_before, bytes_before = files_and_bytes(spark, a.table, target_bytes, day)

        print(f"\n=== {day}: {files_before} files before; rewriting ===", flush=True)
        started = datetime.now(timezone.utc).replace(tzinfo=None)
        t0 = time.perf_counter()
        res = spark.sql(f"""
            CALL glue.system.rewrite_data_files(
                table   => '{ident}',
                where   => "{where}",
                options => map(
                    'partial-progress.enabled', 'true',
                    'target-file-size-bytes',   '{target_bytes}',
                    'min-input-files',          '{a.min_input_files}'))
        """).collect()[0].asDict()
        duration = time.perf_counter() - t0

        files_after, bytes_after = files_and_bytes(spark, a.table, target_bytes, day)
        print(f"{day}: {files_before} -> {files_after} files in {duration:.1f}s; "
              f"procedure says {res}", flush=True)
        results.append((
            run_id, started, a.table, day, where, round(duration, 2),
            files_before, files_after, bytes_before, bytes_after,
            int(res.get("rewritten_data_files_count", 0)),
            int(res.get("added_data_files_count", 0)),
            int(res.get("rewritten_bytes_count", 0)),
            int(res.get("failed_data_files_count", 0)),
        ))

    cols = [c.strip().split()[0] for c in gl.OPS_TABLES["compaction_runs"].split(",")]
    spark.createDataFrame(results, cols).writeTo(f"{gl.OPS_NAMESPACE}.compaction_runs").append()

    if a.maintenance:
        print("\n=== Maintenance: rewrite_manifests ===", flush=True)
        spark.sql(f"CALL glue.system.rewrite_manifests(table => '{ident}')").show(truncate=False)
        print("=== Maintenance: expire_snapshots (keep last 5) ===", flush=True)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        spark.sql(f"""
            CALL glue.system.expire_snapshots(
                table => '{ident}', older_than => TIMESTAMP '{now}', retain_last => 5)
        """).show(truncate=False)

    print(f"\n=== Compaction run {run_id} ===", flush=True)
    spark.sql(f"""
        SELECT partition_key AS day, files_before, files_after, duration_s,
               rewritten_files, added_files, failed_files
        FROM {gl.OPS_NAMESPACE}.compaction_runs WHERE run_id = '{run_id}'
        ORDER BY day
    """).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
