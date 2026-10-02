"""GL1: build a day-partitioned Iceberg table where some days are healthy and
some are deliberately fragmented, the way frequent dbt incremental runs leave
them.

Every day gets the same number of rows, so a later read benchmark compares
like with like:
  healthy day     -> one commit, one file
  fragmented day  -> --commits separate commits (each a snapshot, like an
                     incremental run), each writing --files-per-commit files

Usage (via scripts/run-job.sh generate):
  generate_small_files.py --table glue.demo.events --start 2026-09-01 --days 7
      --rows-per-day 200000 --fragmented 2026-09-03,2026-09-05
      --commits 30 --files-per-commit 10 [--recreate]
"""
import argparse
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

EVENT_TYPES = ["signup", "page_view", "purchase", "logout"]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--table", default="glue.demo.events")
    p.add_argument("--start", default="2026-09-01")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--rows-per-day", type=int, default=200_000)
    p.add_argument("--fragmented", default="2026-09-03,2026-09-05",
                   help="comma-separated days to fragment")
    p.add_argument("--commits", type=int, default=30)
    p.add_argument("--files-per-commit", type=int, default=10)
    p.add_argument("--recreate", action="store_true",
                   help="drop and recreate the table first")
    return p.parse_args()


def day_rows(spark, day, start_id, n_rows):
    """n_rows synthetic events spread across one day, ids from start_id."""
    day_epoch = F.unix_timestamp(F.lit(f"{day.isoformat()} 00:00:00"))
    types = F.array(*[F.lit(t) for t in EVENT_TYPES])
    return (
        spark.range(start_id, start_id + n_rows)
        .select(
            F.concat(F.lit("evt-"), F.col("id").cast("string")).alias("event_id"),
            F.element_at(types, (F.col("id") % len(EVENT_TYPES) + 1).cast("int")).alias("event_type"),
            F.timestamp_seconds(day_epoch + F.col("id") % 86400).alias("occurred_at"),
            (F.col("id") % 5000).cast("int").alias("customer_id"),
            F.sha2(F.col("id").cast("string"), 256).alias("payload"),
        )
    )


def main():
    a = parse_args()
    spark = SparkSession.builder.appName("gl1-generate-small-files").getOrCreate()
    namespace = a.table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    if a.recreate:
        spark.sql(f"DROP TABLE IF EXISTS {a.table} PURGE")

    # distribution-mode=none: Spark writes exactly the files we ask for
    # (repartition count) instead of re-clustering by partition first.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {a.table} (
            event_id    STRING,
            event_type  STRING,
            occurred_at TIMESTAMP,
            customer_id INT,
            payload     STRING
        )
        USING iceberg
        PARTITIONED BY (days(occurred_at))
        TBLPROPERTIES (
            'write.distribution-mode' = 'none',
            'format-version' = '2'
        )
    """)

    fragmented = {d.strip() for d in a.fragmented.split(",") if d.strip()}
    start = date.fromisoformat(a.start)
    next_id = 0

    for i in range(a.days):
        day = start + timedelta(days=i)
        if day.isoformat() not in fragmented:
            print(f"{day}: healthy -> 1 commit, 1 file, {a.rows_per_day} rows", flush=True)
            day_rows(spark, day, next_id, a.rows_per_day).coalesce(1).writeTo(a.table).append()
            next_id += a.rows_per_day
            continue

        per_commit = max(1, a.rows_per_day // a.commits)
        print(f"{day}: fragmented -> {a.commits} commits x {a.files_per_commit} files, "
              f"{per_commit} rows per commit", flush=True)
        for c in range(a.commits):
            (day_rows(spark, day, next_id, per_commit)
             .repartition(a.files_per_commit)
             .writeTo(a.table).append())
            next_id += per_commit
            if (c + 1) % 10 == 0:
                print(f"  {day}: {c + 1}/{a.commits} commits", flush=True)

    print("\n=== Result: files per partition ===", flush=True)
    spark.sql(f"""
        SELECT partition.occurred_at_day AS day,
               record_count,
               file_count,
               round(total_data_file_size_in_bytes / file_count / 1024, 1) AS avg_file_kb
        FROM {a.table}.partitions
        ORDER BY day
    """).show(50, truncate=False)
    spark.sql(f"SELECT count(*) AS snapshots FROM {a.table}.snapshots").show()
    spark.stop()


if __name__ == "__main__":
    main()
