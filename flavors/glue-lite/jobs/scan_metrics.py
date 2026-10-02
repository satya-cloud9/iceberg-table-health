"""GL2.5b: run the metric inventory over every table in a namespace.

Appends one row per partition to glue.ops.partition_metrics and one row per
table to glue.ops.table_metrics, all under one scan_id, then prints a summary.
Reads only Iceberg metadata. Symptom rules (2.5c) read these tables.

Usage (via scripts/run-job.sh py scan_metrics.py ...):
  scan_metrics.py [--namespace glue.demo] [--tables s0_small_appends,...]
                  [--config /opt/jobs/config/health.json]
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import gl_common as gl
import probes

PARTITION_METRICS_DDL = """
    scan_id STRING, scanned_at TIMESTAMP, table_name STRING, partition_key STRING,
    spec_ids STRING, data_files BIGINT, delete_files_pos BIGINT, delete_files_eq BIGINT,
    records BIGINT, delete_records BIGINT, data_bytes BIGINT, avg_file_bytes BIGINT,
    p10_file_bytes BIGINT, p50_file_bytes BIGINT, p90_file_bytes BIGINT,
    small_files BIGINT, oversized_files BIGINT, ideal_files BIGINT, excess_files BIGINT,
    files_old_spec BIGINT, files_current_sort BIGINT, last_updated_at TIMESTAMP,
    minutes_since_update DOUBLE, target_file_bytes BIGINT"""

TABLE_METRICS_DDL = """
    scan_id STRING, scanned_at TIMESTAMP, table_name STRING, load_error STRING,
    format_version BIGINT, partition_spec STRING, current_spec_id BIGINT,
    sort_order STRING, sort_order_defined BOOLEAN,
    partitions BIGINT, data_files BIGINT, delete_files BIGINT, records BIGINT,
    data_bytes BIGINT, avg_file_bytes BIGINT, read_amplification DOUBLE,
    partitions_with_excess BIGINT, skew_ratio DOUBLE, top1pct_share DOUBLE,
    undersized_partition_share DOUBLE,
    snapshots BIGINT, oldest_snapshot_age_h DOUBLE, commits_1h BIGINT, commits_24h BIGINT,
    avg_added_files_per_commit DOUBLE, avg_added_bytes_per_commit DOUBLE,
    avg_changed_partitions_per_commit DOUBLE,
    data_manifests BIGINT, avg_manifest_bytes DOUBLE, metadata_versions BIGINT,
    retained_bytes BIGINT,
    filter_columns STRING, pruning_json STRING, min_pruning_efficiency DOUBLE,
    distribution_mode STRING, write_delete_mode STRING, write_update_mode STRING,
    write_merge_mode STRING, manifest_merge_enabled STRING, properties_json STRING,
    target_file_bytes BIGINT"""


def coerce(value, data_type):
    """Match Python values to the column type (createDataFrame is strict)."""
    if value is None:
        return None
    name = data_type.typeName()
    if name == "double":
        return float(value)
    if name in ("long", "integer"):
        return int(value)
    if name == "boolean":
        return bool(value)
    if name == "string":
        return str(value)
    return value


def as_row(values, schema):
    return tuple(coerce(values.get(f.name), f.dataType) for f in schema)


def load_config(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def table_config(config, table):
    cfg = dict(config.get("defaults", {}))
    cfg.update(config.get("tables", {}).get(table, {}))
    return cfg


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--namespace", default="glue.demo")
    p.add_argument("--tables", default="", help="comma-separated table names in the namespace")
    p.add_argument("--config", default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                    "config", "health.json"))
    p.add_argument("--scan-id", default=None)
    a = p.parse_args()

    config = load_config(a.config)
    scan_id = a.scan_id or gl.new_run_id("scan")
    spark = SparkSession.builder.appName("gl25-scan-metrics").getOrCreate()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {gl.OPS_NAMESPACE}")
    pm_table = f"{gl.OPS_NAMESPACE}.partition_metrics"
    tm_table = f"{gl.OPS_NAMESPACE}.table_metrics"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {pm_table} ({PARTITION_METRICS_DDL}) USING iceberg")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {tm_table} ({TABLE_METRICS_DDL}) USING iceberg")
    pm_schema = spark.table(pm_table).schema
    tm_schema = spark.table(tm_table).schema

    wanted = {t.strip() for t in a.tables.split(",") if t.strip()}
    names = sorted(r.tableName for r in spark.sql(f"SHOW TABLES IN {a.namespace}").collect()
                   if not wanted or r.tableName in wanted)
    print(f"=== Scan {scan_id}: {len(names)} tables in {a.namespace} ===", flush=True)

    scanned_at = probes.now_utc()
    summary = []
    for name in names:
        table = f"{a.namespace}.{name}"
        cfg = table_config(config, table)
        info = probes.table_info(spark, table)
        try:
            pm = probes.partition_metrics(spark, table, info, cfg)
            pm_rows = pm.collect()
            out = (pm.withColumn("scan_id", F.lit(scan_id))
                     .withColumn("scanned_at", F.lit(scanned_at).cast("timestamp"))
                     .withColumn("table_name", F.lit(table)))
            out.select(*[F.col(f.name).cast(f.dataType) for f in pm_schema]).writeTo(pm_table).append()
            tm = probes.table_metrics(spark, table, info, cfg, pm_rows)
        except Exception as e:  # one broken table must not stop the scan
            tm = {"load_error": (info.get("error") or "") + f" | {type(e).__name__}: {e}"[:500]}
            print(f"  {table}: FAILED {tm['load_error']}", flush=True)
        tm.update(scan_id=scan_id, scanned_at=scanned_at, table_name=table)
        spark.createDataFrame([as_row(tm, tm_schema)], tm_schema).writeTo(tm_table).append()
        summary.append(tm)

    print("\n=== Table metrics (selected) ===", flush=True)
    cols = ["table_name", "partitions", "data_files", "read_amplification", "skew_ratio",
            "undersized_partition_share", "delete_files", "snapshots", "data_manifests",
            "avg_changed_partitions_per_commit", "min_pruning_efficiency",
            "sort_order_defined", "distribution_mode"]
    sub = spark.table(tm_table).select(*cols).schema
    spark.createDataFrame([as_row(s, sub) for s in summary], sub).show(100, truncate=False)

    print("=== Partitions with excess files (top 15) ===", flush=True)
    spark.sql(f"""
        SELECT table_name, partition_key, data_files, excess_files,
               delete_files_pos + delete_files_eq AS delete_files,
               round(minutes_since_update, 1) AS minutes_since_update
        FROM {pm_table} WHERE scan_id = '{scan_id}' AND (excess_files > 0 OR delete_files_pos > 0)
        ORDER BY excess_files DESC, delete_files DESC LIMIT 15
    """).show(truncate=False)
    print(f"Recorded under scan_id {scan_id}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
