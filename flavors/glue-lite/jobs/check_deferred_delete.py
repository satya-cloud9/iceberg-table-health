"""Cluster check: snapshot expiry that keeps files, then deferred deletion.

Scratch namespace glue.dd_check (a user-like table and its own freed_files
state table; dropped at the end):

  1  expire_keep_files expires the old snapshots, deletes nothing, and records
     the freed files (the 6 small files a compaction replaced among them)
  2  the freed files still exist and are readable through the table's FileIO
  3  time travel to an expired snapshot fails
  4  the orphan listing counts them as waiting, not as orphans
  5  delete_due before the grace has passed deletes nothing
  6  after the grace, delete_due deletes them and drops their records
  7  the table reads the same rows throughout

Usage (make gl-deferred-check): check_deferred_delete.py
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession

import freed_files as ff
import probes
import state_store as ss

NS, T = "glue.dd_check", "glue.dd_check.events"
GRACE_S = 8
ok = True


def check(name, cond, extra=""):
    global ok
    print(("PASS " if cond else "FAIL ") + name, extra, flush=True)
    ok &= bool(cond)


def read_head(io, path):
    stream = io.newInputFile(path).newStream()
    try:
        return bytes(bytearray((stream.read() & 0xFF) for _ in range(4)))
    finally:
        stream.close()


def main():
    spark = SparkSession.builder.appName("gl-deferred-check").getOrCreate()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {T} PURGE")
        spark.sql(f"CREATE TABLE {T} (id BIGINT, v STRING) USING iceberg")
        for i in range(6):
            spark.sql(f"INSERT INTO {T} VALUES ({i}, 'row {i}')")
        old_sid = int(spark.sql(f"SELECT snapshot_id FROM {T}.snapshots ORDER BY committed_at DESC LIMIT 1")
                      .collect()[0].snapshot_id)
        old_files = sorted(r.file_path for r in spark.sql(f"SELECT file_path FROM {T}.files").collect())
        spark.sql("CALL glue.system.rewrite_data_files(table => 'dd_check.events', "
                  "options => map('min-input-files', '2'))").collect()
        rows = spark.table(T).count()
        uuid = probes.table_info(spark, T)["uuid"]

        ff.ensure_table(spark, NS)
        store = ss.IcebergStateStore(spark, NS)
        out = ff.expire_keep_files(spark, T, uuid, int(time.time() * 1000) + 1000, 1, GRACE_S / 3600,
                                   store, "check")
        print(f"  expire_keep_files: {out}", flush=True)
        recorded = ss.IcebergStateStore(spark, NS).range(ff.KIND, (uuid,))
        paths = {r["path"] for r in recorded}
        check("1 expiry committed (1 snapshot left), freed files recorded",
              out["snapshots_after"] == 1 and out["freed"] == len(recorded) > 0, (out["snapshots_after"], len(recorded)))
        check("1b the compacted small files are among them", set(old_files) <= paths, len(old_files))
        jt = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, T)
        io = jt.io()
        check("2 freed files still exist", all(io.newInputFile(p).exists() for p in paths))
        check("2b and are readable (a reader mid-scan keeps going)",
              all(read_head(io, p) == b"PAR1" for p in old_files))
        try:
            spark.read.option("snapshot-id", str(old_sid)).table(T).count()
            check("3 time travel to the expired snapshot fails", False)
        except Exception as e:
            check("3 time travel to the expired snapshot fails", True, f"{type(e).__name__}")
        om = probes.orphan_metrics(spark, T, {"orphan_min_age_minutes": 0}, pending=paths)
        bare = probes.orphan_metrics(spark, T, {"orphan_min_age_minutes": 0})
        check("4 orphan listing: waiting, not orphans", om["orphan_files"] == 0 and om["freed_files_listed"] >= len(old_files)
              and bare["orphan_files"] >= len(old_files),
              (om["orphan_files"], om["freed_files_listed"], bare["orphan_files"]))
        early = ff.delete_due(spark, T, uuid, store)
        check("5 before the grace: nothing deleted", early["deleted"] == 0 and early["waiting"] == len(paths)
              and all(io.newInputFile(p).exists() for p in paths), early)
        time.sleep(GRACE_S + 2)
        late = ff.delete_due(spark, T, uuid, store)
        check("6 after the grace: deleted and records dropped",
              late["deleted"] == len(paths) and late["failed"] == 0
              and not any(io.newInputFile(p).exists() for p in paths)
              and not ss.IcebergStateStore(spark, NS).range(ff.KIND, (uuid,)), late)
        check("7 table unchanged", spark.table(T).count() == rows == 6, rows)
    finally:
        for r in spark.sql(f"SHOW TABLES IN {NS}").collect():
            spark.sql(f"DROP TABLE IF EXISTS {NS}.{r.tableName} PURGE")
        spark.sql(f"DROP NAMESPACE IF EXISTS {NS}")
    print("ALL PASS" if ok else "SOME FAILED", flush=True)
    spark.stop()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
