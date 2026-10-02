"""GL2.5a: build the scenario test tables the health scan must diagnose.

One table per scenario in glue.demo, dropped and rebuilt on every run, with
fixed seeds so file counts are identical run to run. Row counts are small:
detection reads metadata, so the file layout matters, not the volume.

Scale (see config/health.json): target file size 8 MB. About 26,000 rows of
this schema make 1 MB, so a "healthy day" of 100,000 rows is one ~3.8 MB file.

  s0_small_appends     many small commits into 2 of 7 days      -> SMALL_FILES
  s1_late_arrivals     late rows trickle into many older days    -> SCATTERED_SMALL_FILES
  s2_mor_deletes       merge-on-read DELETEs                     -> DELETE_BUILDUP
  s3_hot_partition     fragmented past day + still-written today -> SMALL_FILES, HOT_PARTITION
  s4_manifest_bloat    60 commits, manifest merging off          -> MANIFEST_BLOAT
  s5_skewed_partition  one day ~30x the others                   -> PARTITION_SKEW
  s6_over_partitioned  hours() on low volume: 720 tiny partitions -> OVER_PARTITIONED

s3 is always built last: it ages its past day beyond the hot window, then
writes "today", so a scan started right after sees exactly one hot partition.

Usage (via scripts/run-job.sh py build_test_tables.py ...):
  build_test_tables.py [--only s0,s4] [--hot-minutes 3]
"""
import argparse
import math
import random
import time
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

NS = "glue.demo"
EVENT_TYPES = ["signup", "page_view", "purchase", "logout"]
SCHEMA_SQL = """
    event_id    STRING,
    event_type  STRING,
    occurred_at TIMESTAMP,
    customer_id INT,
    payload     STRING
"""
# Written only while loading, then removed, so write-configuration findings
# appear only where a scenario keeps them on purpose.
LOAD_PROPS = {"write.distribution-mode": "none", "write.spark.fanout.enabled": "true"}


def events(spark, start_id, n, start_epoch, span_seconds):
    """n rows with ids from start_id, timestamps spread over [start, start+span)."""
    types = F.array(*[F.lit(t) for t in EVENT_TYPES])
    return spark.range(start_id, start_id + n).select(
        F.concat(F.lit("evt-"), F.col("id").cast("string")).alias("event_id"),
        F.element_at(types, (F.col("id") % len(EVENT_TYPES) + 1).cast("int")).alias("event_type"),
        F.timestamp_seconds(F.lit(start_epoch) + F.col("id") % span_seconds).alias("occurred_at"),
        (F.col("id") % 5000).cast("int").alias("customer_id"),
        F.sha2(F.col("id").cast("string"), 256).alias("payload"),
    )


def epoch(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


class Builder:
    def __init__(self, spark):
        self.spark = spark
        self.next_id = 0

    def take(self, n):
        start = self.next_id
        self.next_id += n
        return start

    def create(self, table, partition_expr, props):
        all_props = {**LOAD_PROPS, **props}
        tblprops = ", ".join(f"'{k}' = '{v}'" for k, v in all_props.items())
        self.spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
        self.spark.sql(f"""
            CREATE TABLE {table} ({SCHEMA_SQL})
            USING iceberg
            PARTITIONED BY ({partition_expr})
            TBLPROPERTIES ({tblprops})
        """)

    def finish(self, table, keep=()):
        """Remove load-only properties the scenario doesn't keep on purpose."""
        drop = [k for k in LOAD_PROPS if k not in keep]
        if drop:
            keys = ", ".join(f"'{k}'" for k in drop)
            self.spark.sql(f"ALTER TABLE {table} UNSET TBLPROPERTIES ({keys})")

    def day(self, table, d, rows, files=1):
        df = events(self.spark, self.take(rows), rows, epoch(d), 86400)
        (df.coalesce(1) if files == 1 else df.repartition(files)).writeTo(table).append()

    def many_days(self, table, days, rows_per_day):
        """One commit writing one file per day (fanout, single task)."""
        dfs = [events(self.spark, self.take(rows_per_day), rows_per_day, epoch(d), 86400) for d in days]
        df = dfs[0]
        for other in dfs[1:]:
            df = df.unionAll(other)
        df.coalesce(1).writeTo(table).append()

    def fragment(self, table, d, commits, files_per_commit, rows_per_commit):
        for _ in range(commits):
            self.day(table, d, rows_per_commit, files_per_commit)


def s0_small_appends(b, args):
    t = f"{NS}.s0_small_appends"
    b.create(t, "days(occurred_at)", {})
    start = date(2026, 9, 1)
    for i in range(7):
        d = start + timedelta(days=i)
        if d in (date(2026, 9, 3), date(2026, 9, 5)):
            b.fragment(t, d, commits=20, files_per_commit=10, rows_per_commit=5_000)
        else:
            b.day(t, d, 100_000)
    b.finish(t, keep=("write.distribution-mode",))  # the cause, kept on purpose
    return t


def s1_late_arrivals(b, args):
    t = f"{NS}.s1_late_arrivals"
    b.create(t, "days(occurred_at)", {})
    end = date(2026, 9, 30)
    days = [end - timedelta(days=k) for k in range(30)]   # k = 0 is the newest day
    b.many_days(t, days, 100_000)
    rng = random.Random(42)
    for _ in range(20):                                    # 20 "daily runs"
        touched = [days[k] for k in range(30) if rng.random() < math.exp(-k / 6)]
        b.many_days(t, touched, 500)                       # late rows: 1 small file per touched day
    b.finish(t, keep=("write.distribution-mode", "write.spark.fanout.enabled"))
    return t


def s2_mor_deletes(b, args):
    t = f"{NS}.s2_mor_deletes"
    b.create(t, "days(occurred_at)", {
        "format-version": "2",
        "write.delete.mode": "merge-on-read",
        "write.update.mode": "merge-on-read",
        "write.merge.mode": "merge-on-read",
    })
    b.many_days(t, [date(2026, 9, 1) + timedelta(days=i) for i in range(7)], 100_000)
    for r in range(10):                                    # each removes 1% of rows on every day
        b.spark.sql(f"DELETE FROM {t} WHERE customer_id % 100 = {r}")
    b.finish(t)
    return t


def s4_manifest_bloat(b, args):
    t = f"{NS}.s4_manifest_bloat"
    b.create(t, "days(occurred_at)", {"commit.manifest-merge.enabled": "false"})
    start = date(2026, 7, 1)
    for i in range(60):                                    # 60 commits, each a new healthy day
        b.day(t, start + timedelta(days=i), 80_000)
    b.finish(t)
    return t


def s5_skewed_partition(b, args):
    t = f"{NS}.s5_skewed_partition"
    b.create(t, "days(occurred_at)", {})
    big = date(2026, 9, 15)
    normal = [date(2026, 9, 1) + timedelta(days=i) for i in range(30) if date(2026, 9, 1) + timedelta(days=i) != big]
    b.many_days(t, normal, 100_000)
    b.day(t, big, 3_000_000, files=15)                     # ~30x: 15 well-sized files, no excess
    b.finish(t)
    return t


def s6_over_partitioned(b, args):
    t = f"{NS}.s6_over_partitioned"
    b.create(t, "hours(occurred_at)", {})
    start = epoch(date(2026, 9, 1))
    hours = 30 * 24
    rows_per_hour = 200
    n = hours * rows_per_hour
    first = b.take(n)
    # Row i lands in hour i // rows_per_hour: one tiny file per hour, one commit.
    df = (b.spark.range(first, first + n)
          .select(
              F.concat(F.lit("evt-"), F.col("id").cast("string")).alias("event_id"),
              F.element_at(F.array(*[F.lit(e) for e in EVENT_TYPES]),
                           (F.col("id") % len(EVENT_TYPES) + 1).cast("int")).alias("event_type"),
              F.timestamp_seconds(F.lit(start)
                                  + F.floor((F.col("id") - first) / rows_per_hour) * 3600
                                  + F.col("id") % 3600).alias("occurred_at"),
              (F.col("id") % 5000).cast("int").alias("customer_id"),
              F.sha2(F.col("id").cast("string"), 256).alias("payload")))
    df.coalesce(1).writeTo(t).append()
    b.finish(t)
    return t


def s3_hot_partition(b, args):
    t = f"{NS}.s3_hot_partition"
    b.create(t, "days(occurred_at)", {})
    today = datetime.now(timezone.utc).date()
    for k in range(7, 2, -1):                              # healthy past days
        b.day(t, today - timedelta(days=k), 100_000)
    b.fragment(t, today - timedelta(days=2), commits=20, files_per_commit=10, rows_per_commit=5_000)
    wait = args.hot_minutes * 60 + 30
    print(f"  s3: waiting {wait}s so the past day ages out of the {args.hot_minutes}-min hot window", flush=True)
    time.sleep(wait)
    b.fragment(t, today, commits=20, files_per_commit=10, rows_per_commit=5_000)
    b.finish(t, keep=("write.distribution-mode",))
    return t


BUILDERS = {
    "s0": s0_small_appends,
    "s1": s1_late_arrivals,
    "s2": s2_mor_deletes,
    "s4": s4_manifest_bloat,
    "s5": s5_skewed_partition,
    "s6": s6_over_partitioned,
    "s3": s3_hot_partition,      # keep last
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--only", default="", help="comma-separated scenario ids, e.g. s0,s4")
    p.add_argument("--hot-minutes", type=int, default=3,
                   help="hot window the scan will use; s3 ages its past day beyond it")
    args = p.parse_args()

    wanted = [s.strip() for s in args.only.split(",") if s.strip()] or list(BUILDERS)
    unknown = set(wanted) - set(BUILDERS)
    if unknown:
        raise SystemExit(f"unknown scenarios: {sorted(unknown)}")

    spark = SparkSession.builder.appName("gl25-build-test-tables").getOrCreate()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")
    b = Builder(spark)
    for sid in [s for s in BUILDERS if s in wanted]:       # builder order, s3 last
        t0 = time.perf_counter()
        print(f"=== {sid} ===", flush=True)
        table = BUILDERS[sid](b, args)
        print(f"  {table}: built in {time.perf_counter() - t0:.0f}s", flush=True)
        spark.sql(f"""
            SELECT count(*) AS partitions, sum(file_count) AS data_files,
                   round(sum(total_data_file_size_in_bytes) / 1048576, 1) AS data_mb
            FROM {table}.partitions
        """).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
