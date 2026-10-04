"""GL2.5n: incremental metadata scan, family 2 - partition activity.

For every snapshot the scan hasn't processed yet, read only the manifests that
snapshot wrote (Iceberg's Snapshot.addedDataFiles / removedDataFiles /
addedDeleteFiles / removedDeleteFiles) and keep, per partition:

  glue.ops.partition_activity  one row per (snapshot, partition): files and bytes
                               added / removed, lateness of the batch (time-based
                               partitions)
  glue.ops.partition_state     per (table, partition): last writer write, last
                               compaction, how often a compacted partition was
                               written again ("reopened")
  glue.ops.lateness_hist       lateness counts per table, day and bucket
  glue.ops.activity_state      per table: the last snapshot processed

What this replaces: the full all_entries read (every retained snapshot's
manifests) that the hot window needs, and that lateness would need.

Shadow checks per table (glue.ops.incremental_check, family partition_activity):
  activity     (snapshot, partition, content): files and bytes added / removed,
               ledger vs all_entries, for every retained snapshot processed
  last_write   last writer write per live partition, ledger vs all_entries
               (a partition whose last write left the retained history is
               only known to the ledger: counted as agreeing)
  lateness_p95 histogram bucket vs the exact value from the activity rows

Pure logic (keys, lateness, aggregation, state, comparisons) is unit-tested
without Spark or py4j.
"""
import base64
import calendar
import json
import math
from datetime import date, datetime, timedelta, timezone

ACTIVITY_DDL = """
    table_uuid STRING, table_name STRING, snapshot_id BIGINT, committed_at TIMESTAMP, ts_ms BIGINT,
    operation STRING, is_writer BOOLEAN, partition_key STRING,
    data_files_added BIGINT, data_bytes_added BIGINT, records_added BIGINT,
    delete_files_added BIGINT, delete_bytes_added BIGINT,
    data_files_removed BIGINT, data_bytes_removed BIGINT, delete_files_removed BIGINT,
    lateness_h DOUBLE, scan_id STRING, label STRING"""

PARTITION_STATE_DDL = """
    table_uuid STRING, partition_key STRING, last_write_ms BIGINT, last_compaction_ms BIGINT,
    reopen_count BIGINT, updated_at TIMESTAMP, scan_id STRING, last_write_label STRING"""

LATENESS_HIST_DDL = """
    table_uuid STRING, day DATE, bucket_max_h DOUBLE, batches BIGINT, scan_id STRING"""

ACTIVITY_STATE_DDL = """
    table_uuid STRING, table_name STRING, updated_at TIMESTAMP, last_ts_ms BIGINT,
    last_snapshot_id BIGINT, event STRING, scan_id STRING"""

# Upper edges in hours after the partition's time range ended; None = longer.
LATE_BUCKETS = [1, 6, 24, 48, 72, 168, 720, None]
TIME_TRANSFORMS = ("year", "month", "day", "hour")
EPOCH = date(1970, 1, 1)


# ---------------------------------------------------------------- partition keys

def json_value(type_id, value, utc=True):
    """An Iceberg partition value as Spark's to_json writes it (session time zone UTC)."""
    if value is None:
        return None
    if type_id == "DATE":
        return (EPOCH + timedelta(days=int(value))).isoformat()
    if type_id in ("TIMESTAMP", "TIMESTAMP_NANO"):
        us = int(value) // (1000 if type_id == "TIMESTAMP_NANO" else 1)
        dt = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=us)
        s = dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}"
        return s + "Z" if utc else s
    if type_id in ("INTEGER", "LONG"):
        return int(value)
    if type_id in ("FLOAT", "DOUBLE"):
        return float(value)
    if type_id == "BOOLEAN":
        return bool(value)
    if type_id == "DECIMAL":
        return float(str(value)) if "." in str(value) else int(str(value))
    if type_id in ("BINARY", "FIXED"):
        return base64.b64encode(bytes(value)).decode() if not isinstance(value, str) else value
    return str(value)


def partition_key(fields):
    """fields: [(field_id, name, type_id, value, utc)] of the file's spec.
    -> the same string as to_json(partition) in the full path: fields in field
    id order, nulls left out (Spark's to_json drops null fields), no spaces;
    '{}' for an unpartitioned spec."""
    parts = []
    for fid, name, type_id, value, utc in sorted(fields, key=lambda f: f[0]):
        v = json_value(type_id, value, utc)
        if v is None:
            continue
        # decimals keep their scale (Spark writes 12.30, not 12.3)
        text = str(value) if type_id == "DECIMAL" else json.dumps(v, ensure_ascii=False)
        parts.append(json.dumps(name, ensure_ascii=False) + ":" + text)
    return "{" + ",".join(parts) + "}"


def partition_end_ms(transform, source_type, value):
    """End of the time range a partition value covers (ms since epoch), or None
    when the partition isn't time-based."""
    if value is None:
        return None
    v = int(value)
    if transform == "day" or (transform == "identity" and source_type == "DATE"):
        return (v + 1) * 86400000
    if transform == "hour":
        return (v + 1) * 3600000
    if transform == "month":
        y, m = 1970 + (v + 1) // 12, (v + 1) % 12 + 1
        return calendar.timegm((y, m, 1, 0, 0, 0)) * 1000
    if transform == "year":
        return calendar.timegm((1970 + v + 1, 1, 1, 0, 0, 0)) * 1000
    return None


def late_bucket(hours):
    for edge in LATE_BUCKETS:
        if edge is None or hours <= edge:
            return edge
    return None


# ---------------------------------------------------------------- aggregation

def aggregate(snapshot, files):
    """snapshot: {snapshot_id, ts_ms, operation}; files: [{partition_key, change
    ('added'|'removed'), content (0 data, 1 pos, 2 eq), bytes, records, end_ms}].
    -> one activity row per partition the snapshot touched."""
    rows = {}
    for f in files:
        r = rows.setdefault(f["partition_key"], {
            "snapshot_id": snapshot["snapshot_id"], "ts_ms": snapshot["ts_ms"],
            "operation": snapshot["operation"], "is_writer": snapshot["operation"] != "replace",
            "partition_key": f["partition_key"], "data_files_added": 0, "data_bytes_added": 0,
            "records_added": 0, "delete_files_added": 0, "delete_bytes_added": 0,
            "data_files_removed": 0, "data_bytes_removed": 0, "delete_files_removed": 0,
            "lateness_h": None, "_end": None})
        data = f["content"] == 0
        if f["change"] == "added":
            if data:
                r["data_files_added"] += 1
                r["data_bytes_added"] += int(f["bytes"] or 0)
                r["records_added"] += int(f["records"] or 0)
                if f.get("end_ms") is not None:
                    r["_end"] = f["end_ms"]
            else:
                r["delete_files_added"] += 1
                r["delete_bytes_added"] += int(f["bytes"] or 0)
        else:
            if data:
                r["data_files_removed"] += 1
                r["data_bytes_removed"] += int(f["bytes"] or 0)
            else:
                r["delete_files_removed"] += 1
    out = []
    for r in rows.values():
        end = r.pop("_end")
        if r["is_writer"] and r["data_files_added"] and end is not None:
            r["lateness_h"] = round(max(0.0, (r["ts_ms"] - end) / 3600000.0), 4)
        out.append(r)
    return sorted(out, key=lambda r: r["partition_key"])


def wrote(r):
    """A writer write into the partition: files added by a non-replace commit."""
    return r["is_writer"] and (r["data_files_added"] or r["delete_files_added"])


# Batch labels (GL2.5o+). Engines don't mark reloads, so the two "possible_"
# labels are guesses from the commit's shape; writer attribution can confirm them later.
APPEND_LABELS = ("on_time", "late")          # the only batches lateness and reopens are measured on


def is_possible_full_refresh(rows, totals, live_partitions, th):
    """A commit that replaced (nearly) the whole table: partition coverage,
    file replacement and byte replacement all above their thresholds.
    rows: the commit's activity rows; totals: the snapshot summary numbers
    (total_data_files, added_data_files, deleted_data_files, total_files_size,
    added_files_size, removed_files_size); live_partitions: partitions with files."""
    def before(total, added, removed):
        if total is None:
            return None
        return float(total) - float(added or 0) + float(removed or 0)
    files_before = before(totals.get("total_data_files"), totals.get("added_data_files"),
                          totals.get("deleted_data_files"))
    bytes_before = before(totals.get("total_files_size"), totals.get("added_files_size"),
                          totals.get("removed_files_size"))
    if not files_before or not bytes_before:
        return False
    touched = sum(1 for r in rows if r["data_files_removed"] or r["data_files_added"])
    coverage = touched / max(float(live_partitions or 0), touched, 1)
    file_repl = float(totals.get("deleted_data_files") or 0) / files_before
    byte_repl = float(totals.get("removed_files_size") or 0) / bytes_before
    return (coverage >= th.get("refresh_partition_coverage", 0.9)
            and file_repl >= th.get("refresh_file_replacement", 0.8)
            and byte_repl >= th.get("refresh_byte_replacement", 0.8))


def _backfill_limit(baseline_p99, baseline_n, th):
    """Lateness above which a large append counts as a possible backfill."""
    if (baseline_n or 0) >= th.get("min_batches", th.get("min_samples", 20)):
        # p99 beyond the last bucket: nothing is unusually late
        return float("inf") if baseline_p99 is None else float(baseline_p99)
    return float(th.get("cold_start_backfill_hours", 168))


def label_rows(snapshot, rows, live_partitions, baseline_p99, baseline_n, median_partition_bytes, th):
    """Label each (snapshot, partition) batch in place; -> True when the commit is a
    possible full refresh.
      compaction             a 'replace' commit
      possible_full_refresh  the commit replaced (nearly) the whole table
      rewrite                files removed in that partition (copy-on-write, overwrite)
                             or delete files only (merge-on-read row changes)
      possible_backfill      append-only, at least a median partition's bytes, and
                             later than the table's 99th-percentile lateness (as it
                             stood before this scan, with min_batches samples) or,
                             while the table has no such baseline yet (cold start),
                             later than cold_start_backfill_hours
      on_time / late         append-only, by whether the partition's range had ended
      append                 append-only into a partition with no time range"""
    if snapshot["operation"] == "replace":
        for r in rows:
            r["label"] = "compaction"
        return False
    refresh = is_possible_full_refresh(rows, snapshot, live_partitions, th)
    for r in rows:
        if refresh:
            r["label"] = "possible_full_refresh"
        elif r["data_files_removed"] or r["delete_files_removed"] or not r["data_files_added"]:
            r["label"] = "rewrite"
        elif r["lateness_h"] is None:
            r["label"] = "append"
        elif r["lateness_h"] == 0:
            r["label"] = "on_time"
        elif (median_partition_bytes and r["data_bytes_added"] >= median_partition_bytes
              and r["lateness_h"] > _backfill_limit(baseline_p99, baseline_n, th)):
            r["label"] = "possible_backfill"
        else:
            r["label"] = "late"
    return refresh


def update_state(state, rows):
    """state: {partition_key: {last_write_ms, last_compaction_ms, reopen_count, last_write_label}}
    rows: activity rows, any order. -> (new state for touched partitions, reopens).
    Every writer write moves last_write (the hot window needs it); only an
    append-only write after a compaction counts as a reopen (late data), not a
    rewrite, a possible backfill or a possible full refresh."""
    new, reopens = {}, 0
    for r in sorted(rows, key=lambda r: (r["ts_ms"], r["snapshot_id"])):
        pk = r["partition_key"]
        s = dict(new.get(pk) or state.get(pk) or
                 {"last_write_ms": None, "last_compaction_ms": None, "reopen_count": 0})
        s.setdefault("reopen_count", 0)
        if wrote(r):
            label = r.get("label")
            if ((label is None or label in APPEND_LABELS)
                    and s["last_compaction_ms"] is not None and r["ts_ms"] > s["last_compaction_ms"]
                    and (s["last_write_ms"] is None or s["last_write_ms"] <= s["last_compaction_ms"])):
                s["reopen_count"] = (s["reopen_count"] or 0) + 1     # first late write after a compaction
                reopens += 1
            s["last_write_ms"] = max(s["last_write_ms"] or 0, r["ts_ms"])
            s["last_write_label"] = label
        elif r["operation"] == "replace" and r["data_files_added"]:
            s["last_compaction_ms"] = max(s["last_compaction_ms"] or 0, r["ts_ms"])
        new[pk] = s
    return new, reopens


def exact_percentile(values, q):
    vals = sorted(v for v in values if v is not None)
    return vals[max(1, math.ceil(q * len(vals))) - 1] if vals else None


def hist_percentile(counts, q):
    total = sum(counts.values())
    if not total:
        return None, 0
    k, run = max(1, math.ceil(q * total)), 0
    for edge in LATE_BUCKETS:
        run += counts.get(edge, 0)
        if run >= k:
            return edge, total
    return None, total


# ---------------------------------------------------------------- comparisons

def compare_activity(ref, led, processed_ids):
    """ref / led: {(snapshot_id, partition_key): (data_added, data_bytes_added,
    delete_added, data_removed, delete_removed)}; only snapshots the ledger
    processed are compared. -> (agree, n_compared, notes)."""
    keys = {k for k in ref if k[0] in processed_ids} | {k for k in led if k[0] in processed_ids}
    zero = (0, 0, 0, 0, 0)
    bad = [f"{k[0]} {k[1]}: full {ref.get(k, zero)} vs ledger {led.get(k, zero)}"
           for k in sorted(keys, key=str) if ref.get(k, zero) != led.get(k, zero)]
    return not bad, len(keys), bad


def compare_last_write(ref, led, live, oldest_retained_ms):
    """ref / led: {partition_key: last writer write ms}; live: partitions with
    files now. -> (agree, n_compared, notes)."""
    bad = []
    for pk in sorted(live):
        r, l = ref.get(pk), led.get(pk)
        if r == l:
            continue
        if r is None and l is not None and oldest_retained_ms is not None and l < oldest_retained_ms:
            continue                     # written before every retained snapshot: only the ledger knows
        bad.append(f"{pk}: full {r} vs ledger {l}")
    return not bad, len(live), bad


# ---------------------------------------------------------------- Spark / py4j side

def _py(v):
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    try:
        return str(v.toString())          # CharSequence / Utf8, BigDecimal, ...
    except Exception:
        return str(v)


def read_activity(spark, table, snapshot_ids):
    """Files each snapshot added or removed, read only from the manifests that
    snapshot wrote. -> {snapshot_id: [file dicts for aggregate()]}."""
    jvm = spark._jvm
    jt = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    io, specs, schema = jt.io(), jt.specs(), jt.schema()
    obj = jvm.java.lang.Class.forName("java.lang.Object")
    meta = {}

    def spec_meta(spec_id):
        if spec_id not in meta:
            spec = specs.get(spec_id)
            ptype = spec.partitionType()
            out = []
            for pos, pf in enumerate(spec.fields().toArray()):
                ft = ptype.fields().get(pos).type()
                tid = str(ft.typeId().toString())
                utc = True
                if tid.startswith("TIMESTAMP"):
                    try:
                        utc = bool(ft.shouldAdjustToUTC())
                    except Exception:
                        pass
                src = str(schema.findType(pf.sourceId()).typeId().toString())
                out.append((pos, int(pf.fieldId()), str(pf.name()), tid, utc,
                            str(pf.transform().toString()), src))
            meta[spec_id] = out
        return meta[spec_id]

    res = {}
    for sid in snapshot_ids:
        snap = jt.snapshot(int(sid))
        files = []
        if snap is not None:
            for change, iterable in (("added", snap.addedDataFiles(io)), ("removed", snap.removedDataFiles(io)),
                                     ("added", snap.addedDeleteFiles(io)), ("removed", snap.removedDeleteFiles(io))):
                it = iterable.iterator()
                while it.hasNext():
                    f = it.next()
                    part, vals, end = f.partition(), [], None
                    for pos, fid, name, tid, utc, tr, src in spec_meta(int(f.specId())):
                        v = _py(part.get(pos, obj))
                        vals.append((fid, name, tid, v, utc))
                        if end is None and (tr in TIME_TRANSFORMS or (tr == "identity" and src == "DATE")):
                            end = partition_end_ms(tr, src, v)
                    files.append({"partition_key": partition_key(vals), "change": change,
                                  "content": int(f.content().id()), "bytes": int(f.fileSizeInBytes()),
                                  "records": int(f.recordCount()), "end_ms": end})
        res[sid] = files
    return res


def full_reference(spark, table, partitioned):
    """The full path's answer, from all_entries (every retained snapshot's
    manifests): activity per (snapshot, partition) and last writer write per
    partition. Shadow only."""
    pk = "to_json(e.data_file.partition)" if partitioned else "'{}'"
    act = {}
    for r in spark.sql(f"""
            SELECT e.snapshot_id, {pk} AS partition_key,
                   sum(CASE WHEN e.status = 1 AND e.data_file.content = 0 THEN 1 ELSE 0 END) AS da,
                   sum(CASE WHEN e.status = 1 AND e.data_file.content = 0
                            THEN e.data_file.file_size_in_bytes ELSE 0 END)                  AS dba,
                   sum(CASE WHEN e.status = 1 AND e.data_file.content <> 0 THEN 1 ELSE 0 END) AS xa,
                   sum(CASE WHEN e.status = 2 AND e.data_file.content = 0 THEN 1 ELSE 0 END) AS dr,
                   sum(CASE WHEN e.status = 2 AND e.data_file.content <> 0 THEN 1 ELSE 0 END) AS xr
            FROM {table}.all_entries e WHERE e.status IN (1, 2) GROUP BY 1, 2""").collect():
        act[(int(r.snapshot_id), r.partition_key)] = (int(r.da), int(r.dba), int(r.xa), int(r.dr), int(r.xr))
    lw = {r.partition_key: int(r.ms) for r in spark.sql(f"""
            SELECT {pk} AS partition_key, unix_millis(max(s.committed_at)) AS ms
            FROM {table}.all_entries e JOIN {table}.snapshots s ON e.snapshot_id = s.snapshot_id
            WHERE e.status = 1 AND s.operation <> 'replace' GROUP BY 1""").collect()}
    return act, lw


def activity_tuple(r):
    return (int(r["data_files_added"]), int(r["data_bytes_added"]), int(r["delete_files_added"]),
            int(r["data_files_removed"]), int(r["delete_files_removed"]))
