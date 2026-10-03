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
  s7_unpartitioned     no partition spec, 40 small appends       -> SMALL_FILES, SNAPSHOT_BUILDUP
  s8_identity_bucket   region + bucket(8, customer_id); eu fragmented -> SMALL_FILES (8 buckets)
  s9_hourly_small      hours(), 2 fragmented hours of 48         -> SMALL_FILES
  s10_equality_deletes unpartitioned v2 + 10 equality-delete commits -> DELETE_BUILDUP
  s11_string_keys      identity on awkward strings ("o'neil", "a/b") -> SMALL_FILES
  s12_cow_merge_churn  copy-on-write MERGEs of 50 rows across 30 days -> REWRITE_CHURN
  s13_oversized_file   one day written as a single ~20 MB file    -> OVERSIZED_FILES
  s14_spec_evolution   days() evolved to months() mid-life         -> MIXED_SPEC
  s15_metadata_retention previous-versions-max 5, no auto-delete   -> UNBOUNDED_RETENTION (+ orphans)
  s16_orphan_files     stray objects under the table location      -> ORPHAN_FILES

Every build gets a fresh location (<warehouse>/demo.db/<table>-<stamp>):
DROP ... PURGE only deletes files the old table still references, so reusing
the same path would leave the previous build's leftovers to show up as
orphans in the new one.

s3 is always built last: it ages its past day beyond the hot window, then
writes "today", so a scan started right after sees exactly one hot partition.
Without s3 in the run, the builder waits out the hot window at the end so
the new tables aren't scanned as hot.

These s7-s11 cover structures plan.py and the probes had not met on real
data: no partition spec, bucket transforms, hour partitions, equality deletes
(written through Iceberg's Java API; Spark itself only writes position
deletes) and string partition values that need quoting.

Usage (via scripts/run-job.sh py build_test_tables.py ...):
  build_test_tables.py [--only s0,s4] [--hot-minutes 3] [--no-settle]
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

    def create(self, table, partition_expr, props, extra_cols=""):
        all_props = {**LOAD_PROPS, **props}
        tblprops = ", ".join(f"'{k}' = '{v}'" for k, v in all_props.items())
        cols = SCHEMA_SQL + (f", {extra_cols}" if extra_cols else "")
        part = f"PARTITIONED BY ({partition_expr})" if partition_expr else ""
        warehouse = self.spark.conf.get("spark.sql.catalog.glue.warehouse").rstrip("/")
        ns, name = table.split(".")[1:]
        location = f"{warehouse}/{ns}.db/{name}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        self.spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
        self.spark.sql(f"""
            CREATE TABLE {table} ({cols})
            USING iceberg
            {part}
            LOCATION '{location}'
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


def s7_unpartitioned(b, args):
    t = f"{NS}.s7_unpartitioned"
    b.create(t, "", {})
    b.day(t, date(2026, 9, 1), 100_000)                    # one healthy file
    for _ in range(40):                                    # 40 small appends
        b.day(t, date(2026, 9, 2), 500)
    b.finish(t)
    return t


def _regional(b, rows, region, start_epoch, span):
    return events(b.spark, b.take(rows), rows, start_epoch, span).withColumn("region", F.lit(region))


def s8_identity_bucket(b, args):
    t = f"{NS}.s8_identity_bucket"
    b.create(t, "region, bucket(8, customer_id)", {}, extra_cols="region STRING")
    start = epoch(date(2026, 9, 1))
    base = _regional(b, 40_000, "us", start, 86400).unionAll(_regional(b, 40_000, "apac", start, 86400))
    base.coalesce(1).writeTo(t).append()                   # us, apac: one file per bucket
    for _ in range(20):                                    # eu: each commit adds a file to all 8 buckets
        _regional(b, 400, "eu", start, 86400).coalesce(1).writeTo(t).append()
    b.finish(t)
    return t


def s9_hourly_small(b, args):
    t = f"{NS}.s9_hourly_small"
    b.create(t, "hours(occurred_at)", {})
    start = epoch(date(2026, 9, 1))
    events(b.spark, b.take(48 * 500), 48 * 500, start, 48 * 3600).coalesce(1).writeTo(t).append()
    for h in (10, 30):                                     # two hours get 14 commits x 2 files
        for _ in range(14):
            events(b.spark, b.take(400), 400, start + h * 3600, 3600).repartition(2).writeTo(t).append()
    b.finish(t)
    return t


def s10_equality_deletes(b, args):
    t = f"{NS}.s10_equality_deletes"
    b.create(t, "", {"format-version": "2"})
    b.day(t, date(2026, 9, 1), 100_000)
    try:
        for k in range(10):                                # 10 commits, each deleting customer_id = k
            write_equality_delete(b.spark, t, "customer_id", k)
    except Exception as e:                                 # report and carry on with the other tables
        print(f"  s10: could not write equality deletes ({type(e).__name__}: {e}); "
              f"the table has none, so its scorecard check will fail", flush=True)
    b.finish(t)
    return t


def write_equality_delete(spark, table, column, value):
    """Commit one equality-delete file (column = value) through Iceberg's Java API.
    Spark's DELETE writes position deletes only; Flink upserts write these."""
    jvm, gw = spark._jvm, spark.sparkContext._gateway
    jt = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    schema = jt.schema()
    ids = gw.new_array(jvm.int, 1)
    ids[0] = schema.findField(column).fieldId()
    eq_schema = schema.select([column])
    factory = jvm.org.apache.iceberg.data.GenericAppenderFactory(schema, jt.spec(), ids, eq_schema, None)
    path = jt.locationProvider().newDataLocation(f"eq-delete-{column}-{value}-{int(time.time() * 1000)}.parquet")
    out = jvm.org.apache.iceberg.encryption.EncryptedFiles.plainAsEncryptedOutput(jt.io().newOutputFile(path))
    writer = factory.newEqDeleteWriter(out, jvm.org.apache.iceberg.FileFormat.PARQUET, None)
    rec = jvm.org.apache.iceberg.data.GenericRecord.create(eq_schema)
    rec.setField(column, value)
    writer.write(rec)
    writer.close()
    jt.newRowDelta().addDeletes(writer.toDeleteFile()).commit()


STRING_KEYS = ["north america", "o'neil", "a/b", "x=y"]


def s11_string_keys(b, args):
    t = f"{NS}.s11_string_keys"
    b.create(t, "region", {}, extra_cols="region STRING")
    start = epoch(date(2026, 9, 1))
    base = None
    for r in STRING_KEYS:                                  # one healthy file per key
        df = _regional(b, 20_000, r, start, 86400)
        base = df if base is None else base.unionAll(df)
    base.coalesce(1).writeTo(t).append()
    for r in ("o'neil", "a/b"):                            # two keys get 12 small appends
        for _ in range(12):
            _regional(b, 300, r, start, 86400).coalesce(1).writeTo(t).append()
    b.finish(t)
    return t


S12_DAYS, S12_ROWS = 30, 5_000


def s12_cow_merge_churn(b, args):
    """30 days, then 15 copy-on-write MERGEs that each update 50 random rows.
    Each merge rewrites every day file holding one of those rows (~80% of
    the table), so files stay healthy while the table is rewritten ~12x."""
    t = f"{NS}.s12_cow_merge_churn"
    b.create(t, "days(occurred_at)", {"format-version": "2", "write.merge.mode": "copy-on-write",
                                      "write.update.mode": "copy-on-write",
                                      "write.delete.mode": "copy-on-write"})
    first = b.next_id
    b.many_days(t, [date(2026, 9, 1) + timedelta(days=i) for i in range(S12_DAYS)], S12_ROWS)
    b.finish(t)                                            # merges run with default distribution
    rng = random.Random(12)
    for i in range(15):
        merge_random_rows(b.spark, t, first, S12_DAYS * S12_ROWS, 50, rng, f"cow-{i}")
    return t


def merge_random_rows(spark, table, first_id, n_ids, rows, rng, tag):
    """MERGE that updates `rows` random existing rows (by event_id) to a new payload."""
    ids = rng.sample(range(first_id, first_id + n_ids), rows)
    spark.createDataFrame([(f"evt-{i}", f"{tag}-{i}") for i in ids], "event_id string, payload string") \
        .createOrReplaceTempView("merge_src")
    spark.sql(f"""
        MERGE INTO {table} t USING merge_src s ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET t.payload = s.payload
    """)


def s13_oversized_file(b, args):
    t = f"{NS}.s13_oversized_file"
    b.create(t, "days(occurred_at)", {})
    for i in range(6):                                     # healthy days
        b.day(t, date(2026, 9, 1) + timedelta(days=i), 100_000)
    b.day(t, date(2026, 9, 7), 520_000)                    # ~20 MB in one file (> 180% of 8 MB)
    b.finish(t)
    return t


def s14_spec_evolution(b, args):
    t = f"{NS}.s14_spec_evolution"
    b.create(t, "days(occurred_at)", {})
    b.many_days(t, [date(2026, 9, 1) + timedelta(days=i) for i in range(10)], 100_000)
    b.spark.sql(f"ALTER TABLE {t} REPLACE PARTITION FIELD occurred_at_day WITH months(occurred_at)")
    for i in range(10, 15):                                # newer data lands in the monthly spec
        b.day(t, date(2026, 9, 1) + timedelta(days=i), 100_000)
    b.finish(t)
    return t


def s15_metadata_retention(b, args):
    t = f"{NS}.s15_metadata_retention"
    b.create(t, "days(occurred_at)", {"write.metadata.previous-versions-max": "5"})
    for i in range(12):                                    # 12 healthy commits; the log keeps 5
        b.day(t, date(2026, 9, 1) + timedelta(days=i), 100_000)
    b.finish(t)
    return t


def s16_orphan_files(b, args):
    t = f"{NS}.s16_orphan_files"
    b.create(t, "days(occurred_at)", {})
    b.many_days(t, [date(2026, 9, 1) + timedelta(days=i) for i in range(5)], 100_000)
    b.finish(t)
    jt = b.spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(b.spark._jsparkSession, t)
    for i in range(5):                                     # what a crashed writer leaves behind
        out = jt.io().newOutputFile(f"{str(jt.location()).rstrip('/')}/data/stray-{i}.parquet").create()
        out.write(bytearray(b"not referenced by any snapshot " * 32))
        out.close()
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
    "s7": s7_unpartitioned,
    "s8": s8_identity_bucket,
    "s9": s9_hourly_small,
    "s10": s10_equality_deletes,
    "s11": s11_string_keys,
    "s12": s12_cow_merge_churn,
    "s13": s13_oversized_file,
    "s14": s14_spec_evolution,
    "s15": s15_metadata_retention,
    "s16": s16_orphan_files,
    "s3": s3_hot_partition,      # keep last
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--only", default="", help="comma-separated scenario ids, e.g. s0,s4")
    p.add_argument("--hot-minutes", type=int, default=3,
                   help="hot window the scan will use; s3 ages its past day beyond it")
    p.add_argument("--no-settle", action="store_true",
                   help="without s3 in the run, don't wait out the hot window at the end")
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
    if "s3" not in wanted and not args.no_settle:
        wait = args.hot_minutes * 60 + 30
        print(f"=== waiting {wait}s so the new tables are past the {args.hot_minutes}-min hot window ===",
              flush=True)
        time.sleep(wait)
    spark.stop()


if __name__ == "__main__":
    main()
