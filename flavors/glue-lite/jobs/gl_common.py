"""Shared helpers for Glue-Lite jobs: run ids, ops tables, partition health.

Everything here reads Iceberg's own metadata tables (`<table>.files`,
`<table>.partitions`), which list files and their stats without scanning data.
"""
import uuid
from datetime import datetime, timezone

OPS_NAMESPACE = "glue.ops"

OPS_TABLES = {
    "table_health": """
        run_id STRING, measured_at TIMESTAMP, table_name STRING,
        partition_key STRING, data_files BIGINT, delete_files BIGINT,
        records BIGINT, data_bytes BIGINT, avg_file_bytes BIGINT,
        small_files BIGINT, ideal_files BIGINT, excess_files BIGINT,
        target_file_bytes BIGINT, last_updated_at TIMESTAMP,
        needs_compaction BOOLEAN""",
    "compaction_runs": """
        run_id STRING, started_at TIMESTAMP, table_name STRING,
        partition_key STRING, where_clause STRING, duration_s DOUBLE,
        files_before BIGINT, files_after BIGINT,
        bytes_before BIGINT, bytes_after BIGINT,
        rewritten_files BIGINT, added_files BIGINT,
        rewritten_bytes BIGINT, failed_files BIGINT""",
    "read_benchmarks": """
        run_id STRING, measured_at TIMESTAMP, table_name STRING,
        label STRING, day STRING, query_name STRING,
        files_in_partition BIGINT, runs BIGINT,
        median_ms DOUBLE, min_ms DOUBLE, max_ms DOUBLE""",
}


def new_run_id(prefix):
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{stamp}-{uuid.uuid4().hex[:6]}"


def ensure_ops_tables(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {OPS_NAMESPACE}")
    for name, cols in OPS_TABLES.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {OPS_NAMESPACE}.{name} ({cols}) USING iceberg")


def catalog_relative(table):
    """'glue.demo.events' -> 'demo.events' (what CALL glue.system.* expects)."""
    return table.split(".", 1)[1] if table.startswith("glue.") else table


def partition_health(spark, table, target_bytes, min_excess):
    """One row per partition: file counts and sizes, plus a compaction verdict.

    ideal_files  = how many target-sized files the partition's data would need
    excess_files = data_files - ideal_files (files compaction would remove)
    small_files  = data files under 75% of target (Iceberg's own default
                   'min-file-size-bytes' for rewrite_data_files)
    needs_compaction when excess_files >= min_excess, so a partition already
    holding one undersized file (all its data fits in one) isn't flagged.
    """
    small = int(target_bytes * 0.75)
    return spark.sql(f"""
        WITH f AS (
            SELECT to_json(partition) AS partition_key,
                   content, file_size_in_bytes, record_count
            FROM {table}.files
        ),
        agg AS (
            SELECT partition_key,
                   sum(CASE WHEN content = 0 THEN 1 ELSE 0 END)  AS data_files,
                   sum(CASE WHEN content <> 0 THEN 1 ELSE 0 END) AS delete_files,
                   sum(CASE WHEN content = 0 THEN record_count ELSE 0 END)       AS records,
                   sum(CASE WHEN content = 0 THEN file_size_in_bytes ELSE 0 END) AS data_bytes,
                   sum(CASE WHEN content = 0 AND file_size_in_bytes < {small}
                            THEN 1 ELSE 0 END)                                    AS small_files
            FROM f GROUP BY partition_key
        ),
        p AS (
            SELECT to_json(partition) AS partition_key, last_updated_at
            FROM {table}.partitions
        )
        SELECT agg.partition_key,
               CAST(data_files AS BIGINT)   AS data_files,
               CAST(delete_files AS BIGINT) AS delete_files,
               CAST(records AS BIGINT)      AS records,
               CAST(data_bytes AS BIGINT)   AS data_bytes,
               CAST(data_bytes / greatest(data_files, 1) AS BIGINT) AS avg_file_bytes,
               CAST(small_files AS BIGINT)  AS small_files,
               CAST(greatest(1, ceil(data_bytes / {target_bytes})) AS BIGINT) AS ideal_files,
               CAST(data_files - greatest(1, ceil(data_bytes / {target_bytes})) AS BIGINT) AS excess_files,
               CAST({target_bytes} AS BIGINT) AS target_file_bytes,
               p.last_updated_at,
               (data_files - greatest(1, ceil(data_bytes / {target_bytes}))) >= {min_excess} AS needs_compaction
        FROM agg LEFT JOIN p ON agg.partition_key = p.partition_key
        ORDER BY agg.partition_key
    """)
