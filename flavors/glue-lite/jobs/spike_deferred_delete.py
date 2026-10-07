"""Spike: expire snapshots without deleting files, list what the expiry freed,
delete those files later (Iceberg 1.10 on the homelab).

Scratch table glue.spike_dd.t (dropped at the end). Checks:
  1  ExpireSnapshotsSparkAction.expireFiles() commits the expiry, deletes
     nothing, and returns the freed files (path, type)
  2  the freed files still exist after the expiry
  3  time travel to an expired snapshot now fails ("cannot find snapshot")
  4  a reader already executing (its file list planned before the expiry,
     simulated by opening those files through the table's FileIO) still reads after it: the
     reader the grace period protects
  5  bulk delete through the table's FileIO removes them
  6  that same reader now fails: why the grace period matters
  (info) a time-travel DataFrame defined before the expiry: does it plan
     again on use (and fail), or keep the old metadata?
  7  the current table reads the same rows throughout

Run: make gl-image; bash flavors/glue-lite/scripts/run-job.sh py spike_deferred_delete.py
"""
import sys
import time

from pyspark.sql import DataFrame, SparkSession

NS, T = "glue.spike_dd", "glue.spike_dd.t"
ok = True


def check(name, cond, extra=""):
    global ok
    print(("PASS " if cond else "FAIL ") + name, extra, flush=True)
    ok &= bool(cond)


def read_head(io, path):
    """The first 4 bytes of a file through Iceberg's FileIO (b'PAR1' for Parquet)."""
    stream = io.newInputFile(path).newStream()
    try:
        return bytes(bytearray((stream.read() & 0xFF) for _ in range(4)))
    finally:
        stream.close()


def main():
    spark = SparkSession.builder.appName("gl-spike-deferred-delete").getOrCreate()
    jvm = spark._jvm
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")
    spark.sql(f"DROP TABLE IF EXISTS {T} PURGE")
    try:
        spark.sql(f"CREATE TABLE {T} (id BIGINT, v STRING) USING iceberg")
        for i in range(6):                                   # 6 small files, 6 snapshots
            spark.sql(f"INSERT INTO {T} VALUES ({i}, 'row {i}')")
        old_sid = int(spark.sql(f"SELECT snapshot_id FROM {T}.snapshots ORDER BY committed_at DESC LIMIT 1")
                      .collect()[0].snapshot_id)
        old_files = [r.file_path for r in spark.sql(f"SELECT file_path FROM {T}.files").collect()]
        spark.sql(f"CALL glue.system.rewrite_data_files(table => 'spike_dd.t', "
                  f"options => map('min-input-files', '2'))").collect()
        rows_before = spark.table(T).count()

        # a reader that resolved the old snapshot before the expiry
        old_reader = spark.read.option("snapshot-id", str(old_sid)).table(T)
        check("4a old-snapshot reader works before expiry", old_reader.count() == 6)

        jt = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, T)
        jt.refresh()
        now_ms = int(time.time() * 1000) + 1000
        action = (jvm.org.apache.iceberg.spark.actions.SparkActions.get(spark._jsparkSession)
                  .expireSnapshots(jt).expireOlderThan(now_ms).retainLast(1))
        freed = DataFrame(action.expireFiles().toDF(), spark).collect()
        by_type = {}
        for r in freed:
            by_type[r.type] = by_type.get(r.type, 0) + 1
        print(f"  freed: {by_type}", flush=True)
        paths = [r.path for r in freed]
        jt.refresh()
        snaps_left = int(jt.operations().current().snapshots().size())
        check("1 expiry committed (1 snapshot left), freed files returned", snaps_left == 1 and paths, by_type)
        check("1b the 6 small data files are among them",
              set(old_files) <= {r.path for r in freed if r.type in ("Data", "DATA", "data")} or
              set(old_files) <= set(paths), len(old_files))
        io = jt.io()
        check("2 freed files still exist after expiry", all(io.newInputFile(p).exists() for p in paths))
        try:
            spark.read.option("snapshot-id", str(old_sid)).table(T).count()
            check("3 time travel to the expired snapshot fails", False)
        except Exception as e:
            check("3 time travel to the expired snapshot fails", True, f"{type(e).__name__}: {str(e)[:120]}")
        try:
            n = old_reader.count()
            print(f"  (info) time-travel DataFrame defined before expiry: still reads ({n} rows)", flush=True)
        except Exception as e:
            print(f"  (info) time-travel DataFrame defined before expiry: plans again and fails "
                  f"({type(e).__name__}: {str(e)[:120]})", flush=True)
        try:
            heads = [read_head(io, p) for p in old_files]
            check("4 a reader executing on the old files still reads after expiry",
                  all(h == b"PAR1" for h in heads), heads[:2])
        except Exception as e:
            check("4 a reader executing on the old files still reads after expiry", False,
                  f"{type(e).__name__}: {str(e)[:160]}")

        bulk = jvm.org.apache.iceberg.io.SupportsBulkOperations
        lst = jvm.java.util.ArrayList()
        for p in paths:
            lst.add(p)
        check("5a table FileIO supports bulk delete", bulk._java_lang_class.isInstance(io), io.getClass().getName())
        io.deleteFiles(lst)
        check("5 freed files gone after bulk delete", not any(io.newInputFile(p).exists() for p in paths))
        try:
            [read_head(io, p) for p in old_files]
            check("6 that reader fails once the files are deleted", False, "still read")
        except Exception as e:
            check("6 that reader fails once the files are deleted", True, f"{type(e).__name__}: {str(e)[:120]}")
        check("7 current table unchanged", spark.table(T).count() == rows_before == 6, rows_before)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {T} PURGE")
        spark.sql(f"DROP NAMESPACE IF EXISTS {NS}")
    print("ALL PASS" if ok else "SOME FAILED", flush=True)
    spark.stop()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
