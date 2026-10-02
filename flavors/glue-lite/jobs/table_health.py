"""GL2: measure per-partition health of an Iceberg table and record it.

Appends one row per partition to glue.ops.table_health and prints them.
Reads only Iceberg metadata tables, so it's cheap enough to run before and
after every compaction.

Usage (via scripts/run-job.sh py table_health.py ...):
  table_health.py --table glue.demo.events --target-mb 128 --min-excess 4
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import gl_common as gl


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--table", default="glue.demo.events")
    p.add_argument("--target-mb", type=int, default=128)
    p.add_argument("--min-excess", type=int, default=4,
                   help="flag a partition when it has at least this many surplus files")
    p.add_argument("--run-id", default=None)
    a = p.parse_args()

    run_id = a.run_id or gl.new_run_id("health")
    spark = SparkSession.builder.appName("gl2-table-health").getOrCreate()
    gl.ensure_ops_tables(spark)

    health = gl.partition_health(spark, a.table, a.target_mb * 1024 * 1024, a.min_excess)
    rows = (health
            .withColumn("run_id", F.lit(run_id))
            .withColumn("measured_at", F.current_timestamp())
            .withColumn("table_name", F.lit(a.table)))
    rows.select(*[c.strip().split()[0] for c in gl.OPS_TABLES["table_health"].split(",")]) \
        .writeTo(f"{gl.OPS_NAMESPACE}.table_health").append()

    print(f"\n=== Health of {a.table} (run {run_id}, target {a.target_mb} MB) ===", flush=True)
    health.select(
        "partition_key", "data_files", "delete_files", "records",
        F.round(F.col("avg_file_bytes") / 1024, 1).alias("avg_file_kb"),
        "small_files", "ideal_files", "excess_files", "needs_compaction",
    ).show(100, truncate=False)

    flagged = [r.partition_key for r in health.filter("needs_compaction").collect()]
    print(f"Partitions needing compaction: {len(flagged)}", flush=True)
    for k in flagged:
        print(f"  {k}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
