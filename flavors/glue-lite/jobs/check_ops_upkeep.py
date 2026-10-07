"""Cluster check for the housekeeping pass under concurrent writers.

Works in a scratch namespace (glue.ops_check, dropped at the end):

  A  compacting a latest-value table while another thread appends new
     versions: the newest row of every key survives (row-level DELETE);
     the whole-table INSERT OVERWRITE it replaced is run once for contrast
  B  ops-table upkeep: many one-row appends -> rewrite_data_files,
     rewrite_manifests and expire_snapshots run and the counts drop

Usage (make gl-ops-check): check_ops_upkeep.py
"""
import os
import random
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession

import ops_maintenance as om
import state_store as ss
from ledger import LEDGER_STATE_DDL

NS = "glue.ops_check"
T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
ok = True


def check(name, cond, extra=""):
    global ok
    print(("PASS " if cond else "FAIL ") + name, extra, flush=True)
    ok &= bool(cond)


def ts(i):
    return (T0 + timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S")


def put(spark, uuid, i):
    spark.sql(f"INSERT INTO {NS}.ledger_state VALUES "          # columns as in LEDGER_STATE_DDL
              f"('{uuid}', NULL, TIMESTAMP '{ts(i)}', {i}, NULL, NULL, NULL, 'check', NULL)")


def newest(spark):
    return {r.table_uuid: r.m for r in spark.sql(
        f"SELECT table_uuid, max(last_ts_ms) AS m FROM {NS}.ledger_state GROUP BY table_uuid").collect()}


def part_a(spark):
    spark.sql(f"DROP TABLE IF EXISTS {NS}.ledger_state PURGE")
    spark.sql(f"CREATE TABLE {NS}.ledger_state ({LEDGER_STATE_DDL}) USING iceberg")
    uuids = [f"u{i}" for i in range(4)]
    i = 0
    for u in uuids:
        for _ in range(3):
            i += 1
            put(spark, u, i)

    # contrast: read the latest rows, another writer appends, then overwrite the table
    store = ss.IcebergStateStore(spark, NS)
    latest = store._load("ledger_state")
    i += 1
    put(spark, "u0", i)                             # the concurrent run's newer row
    from scan_metrics import as_row
    sch = spark.table(f"{NS}.ledger_state").schema
    spark.createDataFrame([as_row(r, sch) for r in latest], sch).createOrReplaceTempView("gl_check_old")
    spark.sql(f"INSERT OVERWRITE {NS}.ledger_state SELECT * FROM gl_check_old")
    lost = newest(spark).get("u0") != i
    print(f"  whole-table overwrite: the row appended in between {'was lost' if lost else 'survived'}", flush=True)

    # row-level compaction while a writer thread keeps appending
    for u in uuids:
        i += 1
        put(spark, u, i)
    expected = newest(spark)
    stop, errors, lock = threading.Event(), [], threading.Lock()
    counter = {"i": i + 1000}

    def writer():
        try:
            for _ in range(25):
                with lock:
                    counter["i"] += 1
                    n = counter["i"]
                u = random.choice(uuids)
                put(spark, u, n)
                with lock:
                    expected[u] = max(expected.get(u, 0), n)
                time.sleep(random.random() * 0.4)
        except Exception as e:
            errors.append(e)
        finally:
            stop.set()

    th = threading.Thread(target=writer)
    th.start()
    removed = rounds = 0
    while not stop.is_set() or rounds == 0:
        removed += ss.IcebergStateStore(spark, NS).compact("ledger_state")
        rounds += 1
    th.join()
    removed += ss.IcebergStateStore(spark, NS).compact("ledger_state")
    got = newest(spark)
    rows = spark.sql(f"SELECT count(*) AS n, count(DISTINCT table_uuid) AS k FROM {NS}.ledger_state").collect()[0]
    check("A writer thread finished", not errors, errors[:1])
    check(f"A compaction during appends ({rounds} rounds, {removed} rows removed): newest row of every key kept",
          got == expected, {u: (got.get(u), expected[u]) for u in uuids if got.get(u) != expected[u]})
    check("A after a last compaction one row per key", rows.n == rows.k == len(uuids), (rows.n, rows.k))


def part_b(spark):
    t = f"{NS}.small_log"
    spark.sql(f"DROP TABLE IF EXISTS {t} PURGE")
    spark.sql(f"CREATE TABLE {t} (ts TIMESTAMP, v BIGINT) USING iceberg PARTITIONED BY (days(ts))")
    for i in range(36):
        spark.sql(f"INSERT INTO {t} VALUES (TIMESTAMP '{ts(i * 3600)}', {i})")    # 2 day partitions
    cfg = {"ops_maintenance": {"min_excess_files": 10, "max_manifests": 10, "retain_last": 3,
                               "safe_age_hours": 0, "min_expire": 5}}
    before = om.table_facts(spark, t, int(time.time() * 1000), 3)
    rows = om.maintain(spark, cfg, "check", ops=NS)
    mine = [r for r in rows if r["table_name"] == t]
    after = om.table_facts(spark, t, int(time.time() * 1000), 3)
    acts = mine[0]["actions"].split(",") if mine else []
    check("B upkeep chose all four actions",
          set(acts) == {"set_properties", "rewrite_data_files", "rewrite_manifests", "expire_snapshots"},
          mine[0] if mine else rows)
    check("B data files dropped", after["data_files"] < before["data_files"],
          (before["data_files"], after["data_files"]))
    check("B snapshots expired", after["snapshots"] < before["snapshots"], (before["snapshots"], after["snapshots"]))
    check("B manifests dropped", after["manifests"] < before["manifests"], (before["manifests"], after["manifests"]))
    check("B properties set", after["properties"].get("write.metadata.delete-after-commit.enabled") == "true")
    check("B rows unchanged", spark.table(t).count() == 36)
    again = om.maintain(spark, cfg, "check-2", dry_run=True, ops=NS)
    check("B second pass: nothing left to do on that table",
          not [r for r in again if r["table_name"] == t and r["actions"]], again)


def main():
    spark = SparkSession.builder.appName("gl-ops-check").getOrCreate()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")
    try:
        part_a(spark)
        part_b(spark)
    finally:
        for r in spark.sql(f"SHOW TABLES IN {NS}").collect():
            spark.sql(f"DROP TABLE IF EXISTS {NS}.{r.tableName} PURGE")
        spark.sql(f"DROP NAMESPACE IF EXISTS {NS}")
    print("ALL PASS" if ok else "SOME FAILED", flush=True)
    spark.stop()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
