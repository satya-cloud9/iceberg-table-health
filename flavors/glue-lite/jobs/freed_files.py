"""Deferred deletion of the files a snapshot expiry frees (user tables).

Snapshot expiry and file deletion are two steps:

  expire_keep_files  ExpireSnapshotsSparkAction.expireFiles() commits the expiry
                     with file cleanup off and returns the files that just became
                     unreferenced (data, delete, manifest, manifest-list and
                     statistics files). Each is recorded in the state store (kind
                     freed_file) with unreferenced_ms = now and due_ms = now + grace.
  delete_due         deletes a table's recorded files whose due time has passed,
                     in bulk through the table's FileIO, then drops their records.

The grace (expiry.py resolve_policy) is the longest a reader can run: a
declared reader timeout + margin, or file_grace_hours (12) when there is none.
Counting from the expiry, not from the commit that replaced the files, covers a
query that time-travels to a snapshot still retained until then.

A crash between the expiry commit and the record leaves those files
unrecorded; the orphan-file sweep (an age of days) is the backstop. While a
table has freed files waiting, orphan removal is held: a freed file may be
months old, so a creation-time age would not protect it.
"""
import time

KIND = "freed_file"
DDL = """
    table_uuid STRING, table_name STRING, path STRING, file_type STRING,
    unreferenced_ms BIGINT, due_ms BIGINT, grace_h DOUBLE, run_id STRING"""


def ensure_table(spark, ops):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {ops}.freed_files ({DDL}) USING iceberg")


def now_ms():
    return int(time.time() * 1000)


def records(freed, uuid, table, at_ms, grace_h, run_id):
    """freed: [(path, type)] from expireFiles -> state rows (one per path)."""
    due = int(at_ms + float(grace_h) * 3600 * 1000)
    seen, out = set(), []
    for path, ftype in freed:
        if path in seen:
            continue
        seen.add(path)
        out.append({"table_uuid": uuid, "table_name": table, "path": path, "file_type": ftype,
                    "unreferenced_ms": int(at_ms), "due_ms": due, "grace_h": float(grace_h),
                    "run_id": run_id})
    return out


def due(rows, at_ms):
    """Recorded files whose grace has passed."""
    return [r for r in rows if r.get("due_ms") is not None and int(r["due_ms"]) <= at_ms]


def summary(rows, at_ms):
    """-> {waiting, due, next_due_ms} for a table's recorded files."""
    d = due(rows, at_ms)
    later = [int(r["due_ms"]) for r in rows if r not in d and r.get("due_ms") is not None]
    return {"waiting": len(rows) - len(d), "due": len(d), "next_due_ms": min(later) if later else None}


def _table(spark, table):
    jt = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    jt.refresh()
    return jt


def expire_keep_files(spark, table, uuid, older_than_ms, retain_last, grace_h, store, run_id, at_ms=None):
    """Expire snapshots older than older_than_ms (keeping the newest retain_last
    and anything a ref holds), delete nothing, record what was freed.
    -> {"freed", "by_type", "due_ms", "snapshots_before", "snapshots_after"}."""
    from pyspark.sql import DataFrame
    jvm = spark._jvm
    jt = _table(spark, table)
    before = int(jt.operations().current().snapshots().size())
    action = (jvm.org.apache.iceberg.spark.actions.SparkActions.get(spark._jsparkSession)
              .expireSnapshots(jt).expireOlderThan(int(older_than_ms)).retainLast(int(retain_last)))
    rows = DataFrame(action.expireFiles().toDF(), spark).collect()
    at = at_ms or now_ms()
    recs = records([(r.path, r.type) for r in rows], uuid, table, at, grace_h, run_id)
    for r in recs:
        store.put(KIND, r)
    store.flush()
    by_type = {}
    for r in recs:
        by_type[r["file_type"]] = by_type.get(r["file_type"], 0) + 1
    after = int(_table(spark, table).operations().current().snapshots().size())
    return {"freed": len(recs), "by_type": by_type, "due_ms": recs[0]["due_ms"] if recs else None,
            "snapshots_before": before, "snapshots_after": after}


def delete_due(spark, table, uuid, store, at_ms=None, chunk=1000):
    """Delete the table's recorded files whose grace has passed, then drop their
    records. A file already gone counts as deleted. -> {"deleted", "failed", "waiting"}."""
    jvm = spark._jvm
    at = at_ms or now_ms()
    rows = store.range(KIND, (uuid,))
    todo = due(rows, at)
    if not todo:
        return {"deleted": 0, "failed": 0, "waiting": len(rows)}
    io = _table(spark, table).io()
    bulk = jvm.org.apache.iceberg.io.SupportsBulkOperations._java_lang_class.isInstance(io)
    done, failed = [], 0
    for i in range(0, len(todo), chunk):
        part = todo[i:i + chunk]
        try:
            if bulk:
                lst = jvm.java.util.ArrayList()
                for r in part:
                    lst.add(r["path"])
                io.deleteFiles(lst)
            else:
                for r in part:
                    io.deleteFile(r["path"])
            done.extend(part)
        except Exception:
            # partial failure: keep the records of files that still exist, retry next run
            for r in part:
                try:
                    if io.newInputFile(r["path"]).exists():
                        failed += 1
                    else:
                        done.append(r)
                except Exception:
                    failed += 1
    for r in done:
        store.delete(KIND, (uuid, r["path"]))
    store.flush()
    return {"deleted": len(done), "failed": failed, "waiting": len(rows) - len(todo)}
