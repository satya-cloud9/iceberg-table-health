"""GL2.5b: run the metric inventory over every table in a namespace.

Logs one row per table to glue.ops.table_metrics and, for each table measured
this scan, one row per partition to glue.ops.partition_metrics, all under one
scan_id, then prints a summary. Reads only Iceberg metadata.

What the next run needs is state, not log (scan_state.py): table_state (each
table's latest facts and trend), partition_facts (each partition's latest
facts, rewritten only when it changed) and action_state (written by the plan).
A reused table's partitions come from partition_facts and are not logged again.
Detect and the scorecard get this scan's rows in memory (RESULTS) when they run
in the same job, and read them back (scan_results) otherwise. Logs are flushed
before state, so a crash between the two repeats work instead of skipping it.

Usage (via scripts/run-job.sh py scan_metrics.py ...):
  scan_metrics.py [--namespace glue.demo] [--tables s0_small_appends,...]
                  [--config /opt/jobs/config/health.json]
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession

import gl_common as gl
import ledger as ledger_mod
import scan_state
import state_store as ss
import gltrace as tr
import probes

PARTITION_METRICS_DDL = """
    scan_id STRING, scanned_at TIMESTAMP, table_name STRING, partition_key STRING,
    spec_ids STRING, data_files BIGINT, delete_files_pos BIGINT, delete_files_eq BIGINT,
    records BIGINT, delete_records BIGINT, data_bytes BIGINT, avg_file_bytes BIGINT,
    p10_file_bytes BIGINT, p50_file_bytes BIGINT, p90_file_bytes BIGINT,
    small_files BIGINT, oversized_files BIGINT, ideal_files BIGINT, excess_files BIGINT,
    files_old_spec BIGINT, files_current_sort BIGINT, last_updated_at TIMESTAMP,
    minutes_since_update DOUBLE, target_file_bytes BIGINT, rewrite_bytes BIGINT,
    fragments BIGINT, undersized_segments BIGINT, fragment_bytes BIGINT, oldest_fragment_ms BIGINT"""

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
    target_file_bytes BIGINT, target_source STRING, table_uuid STRING,
    partition_fields_json STRING,
    overwrite_commits_recent BIGINT, avg_overwrite_rewrite_share DOUBLE,
    overwrite_commits_24h BIGINT, rewritten_bytes_24h BIGINT, table_turnover_24h DOUBLE,
    orphan_files BIGINT, orphan_bytes BIGINT, listed_objects BIGINT, orphan_sample STRING,
    orphan_error STRING, excess_files_total BIGINT,
    metadata_location STRING, scan_mode STRING, scan_seconds DOUBLE,
    minutes_since_writer_commit DOUBLE, orphan_scanned_at TIMESTAMP, retained_scanned_at TIMESTAMP,
    commit_gap_p95_min DOUBLE, commit_gaps_window BIGINT, ledger_new_snapshots BIGINT, ledger_event STRING,
    activity_new_snapshots BIGINT, activity_event STRING, lateness_p95_h DOUBLE, lateness_batches_window BIGINT,
    reopened_partitions BIGINT, hot_partitions_ledger BIGINT,
    lateness_p99_h DOUBLE, hot_window_min DOUBLE, hot_window_source STRING,
    possible_full_refreshes_30d BIGINT, possible_backfill_batches_30d BIGINT, last_full_refresh_ms BIGINT,
    full_refresh_avg_bytes BIGINT, retained_full_copies BIGINT, pre_refresh_snapshot_ms BIGINT,
    lateness_p99_batches BIGINT, lateness_lookback_days BIGINT, hot_gap_p95_min DOUBLE, hot_gaps_used BIGINT,
    idle_gaps_ignored BIGINT, gap_lookback_days BIGINT, metadata_json_bytes BIGINT,
    retained_metadata_bytes BIGINT, policy_age_h DOUBLE, policy_min_keep BIGINT, policy_source STRING,
    write_category STRING, writer_commits_24h BIGINT, expirable_snapshots BIGINT,
    oldest_expirable_age_h DOUBLE, refs_json STRING, stale_refs_json STRING, stale_refs BIGINT,
    retained_bytes_ledger BIGINT, retained_ledger_note STRING, metadata_json_files BIGINT,
    policy_file_grace_h DOUBLE, policy_grace_source STRING, freed_files_waiting BIGINT,
    writer_commits_24h_seen BIGINT, writer_gap_median_min DOUBLE, writer_bytes_per_commit_24h DOUBLE,
    writer_files_per_commit_24h DOUBLE, history_gaps_window BIGINT, history_lost_commits BIGINT,
    last_history_gap_ms BIGINT, history_unseen_minutes DOUBLE, minutes_since_previous_scan DOUBLE,
    retained_detail_json STRING"""


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


_SCHEMAS = {}


def normalize(spark, kind, values):
    """A record as it reads back from the log: every declared column present, types
    coerced, timestamps naive UTC (what Spark returns in a UTC session)."""
    if kind not in _SCHEMAS:
        from pyspark.sql.types import _parse_datatype_string
        _SCHEMAS[kind] = _parse_datatype_string(ss.LOG_KINDS[kind]["ddl"])
    out = {}
    for f, v in zip(_SCHEMAS[kind], as_row(values, _SCHEMAS[kind])):
        if hasattr(v, "tzinfo") and v.tzinfo is not None:
            v = scan_state.from_ms(scan_state.to_ms(v))
        out[f.name] = v
    for k in ("scanned_ms",):
        if k in values:
            out[k] = values[k]
    return out


def scan_results(spark, scan_id, store=None, config=None, log=None):
    """(table rows, {table: partition rows}) of a scan: from this job's memory when
    it ran here, else read back (the scan's logged rows through the log sink;
    partition rows of reused tables, which are not logged again, from partition_facts)."""
    if scan_id in RESULTS:
        r = RESULTS[scan_id]
        return r["tms"], r["parts"]
    log = log or ss.make_log_sink(spark, config)
    tms = [dict(r, scanned_ms=scan_state.to_ms(r["scanned_at"])) for r in log.rows("table_metrics", eq={"scan_id": scan_id})]
    parts = {}
    for r in log.rows("partition_metrics", eq={"scan_id": scan_id}):
        parts.setdefault(r["table_name"], []).append(r)
    store = store or ss.make_state_store(spark, config)
    for tm in tms:
        if tm["table_name"] not in parts and str(tm.get("scan_mode") or "").startswith("reused") and tm.get("table_uuid"):
            parts[tm["table_name"]] = [dict(r, scan_id=scan_id, table_name=tm["table_name"])
                                       for r in scan_state.partition_rows(
                                           store.range("partition_facts", (tm["table_uuid"],)), tm["scanned_ms"])]
    return tms, parts


def as_row(values, schema):
    return tuple(coerce(values.get(f.name), f.dataType) for f in schema)


REUSE_DROP = {"rn", "elapsed_min", "orphan_age_h", "retained_age_h", "_scanned_ms", "_uuid"}

# this job's scan results, handed to detect and the scorecard in memory (gl_scan
# runs all three in one job); a standalone detect or scorecard reads them back
RESULTS = {}

COVERAGE_DDL = """
    run_id STRING, scan_id STRING, target STRING, group_name STRING, shard STRING,
    table_name STRING, status STRING, note STRING, at TIMESTAMP"""

ss.declare_log("table_metrics", TABLE_METRICS_DDL, "scanned_at")
ss.declare_log("partition_metrics", PARTITION_METRICS_DDL, "scanned_at")
ss.declare_log("coverage", COVERAGE_DDL, "at")


def acted_since_orphan_scan(p, uuid, ages):
    """True when an action ran after the table's last orphan listing."""
    if not p or uuid not in ages or p.get("orphan_age_h") is None:
        return False
    return ages[uuid] < float(p["orphan_age_h"]) * 60.0


def due(p, age_key, every_hours, changed=True):
    """Run an interval-gated probe? Always on first sight; otherwise when the
    interval has passed (0 = whenever the table changed)."""
    if not p or p.get(age_key) is None:
        return True
    every = float(every_hours)
    if every == 0:
        return changed
    return p[age_key] >= every


def carry_orphans(tm, p):
    if p:
        for k in ("orphan_files", "orphan_bytes", "listed_objects", "orphan_sample", "orphan_error", "metadata_json_files",
                  "orphan_scanned_at"):
            tm.setdefault(k, p.get(k))


def median_bytes(pm_rows):
    sizes = sorted(int((r.get("data_bytes") if isinstance(r, dict) else r.data_bytes) or 0) for r in pm_rows)
    return sizes[len(sizes) // 2] if sizes else None


def reuse(spark, table, cfg, p, prev_rows, scanned_at, snapshot_source="full"):
    """Unchanged table (same metadata.json): copy the last scan's results and
    refresh only what moves with the clock - partition ages, and the
    snapshot-based metrics (from metadata.json, cheap)."""
    # from the last scan to this table's own scan time (not to the start of the run)
    elapsed = (float(scan_state.to_ms(scanned_at) - int(p["_scanned_ms"])) / 60000.0
               if p.get("_scanned_ms") is not None else float(p.get("elapsed_min") or 0))
    rows = []
    for r in prev_rows:
        r = dict(r)
        if r.get("minutes_since_update") is not None:
            r["minutes_since_update"] = float(r["minutes_since_update"]) + elapsed
        rows.append(r)
    tm = {k: v for k, v in p.items() if k not in REUSE_DROP}
    if snapshot_source == "full":       # "ledger": the snapshot ledger fills these in (mode "on")
        tm.update(probes.snapshot_metrics(spark, table, cfg, p.get("data_bytes") or 0))
    if snapshot_source == "full":
        # exact, not last scan's value + elapsed: that goes stale while the scan
        # works through the tables (minutes, by the end of a scan); one read of
        # metadata.json, like snapshot_metrics above
        tm["minutes_since_writer_commit"] = probes.writer_minutes(spark, table)
    else:
        wm = p.get("minutes_since_writer_commit")     # the ledger replaces it (mode "on")
        tm["minutes_since_writer_commit"] = None if wm is None else float(wm) + elapsed
    tm["scan_mode"] = "reused"
    tr.log("reuse", "previous partition rows copied; ages moved on by elapsed_min", elapsed_min=elapsed,
           partitions=len(rows))
    return tm, rows


def run_scan(spark, namespace, config, tables=(), scan_id=None, priority=(), report=True, full=False,
             coord=None, run=None, housekeeping=True):
    """Measure every table in the namespace (or just `tables`); return the scan_id.

    `priority` tables are measured first (gl-scan puts s3 first so its hot
    partition is still inside the hot window when it is measured).

    Tables whose metadata.json location is the same as at their last scan are
    reused (see reuse()); `full=True` measures everything from scratch.

    coord (coordinator.py): each table's lease is taken before it is measured and
    released after (the same lease plan.py holds for a whole run, so a scan and
    an optimize run never work on one table at once); a table another run holds is skipped and recorded so in
    coverage. run: {run_id, target, group, shard} for the coverage log.
    housekeeping: whether this run does the store's retention pass.
    """
    scan_id = scan_id or gl.new_run_id("scan")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {gl.OPS_NAMESPACE}")   # the log sink creates its tables

    wanted = set(tables)
    names = sorted(r.tableName for r in spark.sql(f"SHOW TABLES IN {namespace}").collect()
                   if not wanted or r.tableName in wanted)
    first = [n for n in priority if n in names]
    names = first + [n for n in names if n not in first]
    print(f"=== Scan {scan_id}: {len(names)} tables in {namespace} ===", flush=True)

    t_prev = time.perf_counter()
    window_h = float((config.get("incremental") or {}).get("previous_scan_window_hours", 168))
    inc = config.get("incremental") or {}
    # what the previous run left: each table's latest facts and partition facts and
    # the latest advisor action, from the state store (scan_state.py), not the logs
    store = ss.make_state_store(spark, config)
    scan_state.ensure_tables(spark, gl.OPS_NAMESPACE, store)
    now_ms = int(time.time() * 1000)
    prev = scan_state.load_previous(store, namespace, now_ms, window_h)
    # the run's tables we already know (new ones load on their own when first seen)
    known = sorted({p.get("table_uuid") for n, p in prev.items()
                    if p.get("table_uuid") and n.rsplit(".", 1)[-1] in set(names)})
    if known:
        store.preload("partition_facts", uuids=known, max_rows=int(inc.get("store_preload_max_rows", 2000000)))
    scan_state.seed_actions(store, ss.make_log_sink(spark, config))   # once, when action_state is empty
    ages = scan_state.action_ages(store, now_ms)
    t_prev = time.perf_counter() - t_prev
    t_pre = time.perf_counter()
    # logs (table_metrics, partition_metrics, incremental_check) are buffered and
    # written once at the end, before the state (a crash then means redo, not loss)
    log = ss.make_log_sink(spark, config)
    led = ledger_mod.Ledger(spark, config, scan_id, store=store, log=log, uuids=known if wanted else None)
    t_pre = time.perf_counter() - t_pre
    snap_source = "ledger" if led.mode == "on" else "full"
    parts_out = {}
    summary, modes, t_start = [], {"full": 0, "reused": 0}, time.perf_counter()
    run = run or {}
    coverage = []

    def covered(table, status, note=None):
        coverage.append({"run_id": run.get("run_id", scan_id), "scan_id": scan_id, "target": run.get("target"),
                         "group_name": run.get("group"), "shard": run.get("shard"), "table_name": table,
                         "status": status, "note": note, "at": probes.now_utc()})

    for name in names:
        table = f"{namespace}.{name}"
        if coord is not None and not coord.claim_table(table, "run"):
            print(f"  skip   {table}  (claimed by another run)", flush=True)
            covered(table, "claimed_elsewhere")
            modes["skipped"] = modes.get("skipped", 0) + 1
            continue
        cfg = gl.table_config(config, table)
        scanned_at = probes.now_utc()
        t0 = time.perf_counter()
        tr.begin(table)
        info = probes.table_info(spark, table)
        cfg["target_file_bytes"], cfg["target_source"] = gl.resolve_target(
            config, table, info.get("properties"))
        p = prev.get(table)
        tr.log("table_info", spec=info.get("spec"), partitioned=info.get("partitioned"),
               sort_order=info.get("sort_order"), format_version=info.get("format_version"),
               uuid=info.get("uuid"), metadata_json_bytes=info.get("metadata_json_bytes"),
               target_file_bytes=cfg["target_file_bytes"], target_source=cfg["target_source"],
               error=info.get("error"))
        tr.log("previous_scan", "none (first sight)" if not p else "",
               **({} if not p else {"scan_id": p.get("scan_id"), "elapsed_min": p.get("elapsed_min"),
                                    "same_metadata_json": p.get("metadata_location") == info.get("metadata_location"),
                                    "retained_age_h": p.get("retained_age_h"), "orphan_age_h": p.get("orphan_age_h")}))
        try:
            if (not full and p and info.get("metadata_location")
                    and p["metadata_location"] == info["metadata_location"]):
                tr.log("path", "2B reused: metadata.json unchanged since the last scan",
                       snapshot_metrics_from=snap_source)
                # unchanged partitions are not logged again: their facts live in partition_facts
                stored = store.range("partition_facts", (p["_uuid"],)) if p.get("_uuid") else []
                tm, pm_rows = reuse(spark, table, cfg, p, scan_state.partition_rows(stored, p.get("_scanned_ms")),
                                    scanned_at, snap_source)
            else:
                tr.log("path", "2A full measure: " + ("--full" if full else "first scan of this table" if not p
                                                      else "metadata.json changed"))
                wm = probes.writer_minutes(spark, table)
                pm = probes.partition_metrics(spark, table, info, cfg, wm)
                pm_rows = pm.collect()
                tr.rows("partition", pm_rows, ["partition_key", "data_files", "data_bytes", "small_files",
                                               "ideal_files", "excess_files", "oversized_files", "delete_files_pos",
                                               "delete_files_eq", "delete_records", "records", "minutes_since_update"])
                log.append("partition_metrics", [dict(r.asDict(), scan_id=scan_id, scanned_at=scanned_at,
                                                      table_name=table) for r in pm_rows])
                rmode = str(cfg.get("retained_bytes", "full"))
                # "ledger": the summary formula fills it in; the full query stays as the
                # rationed fallback for tables where the formula is unknown (refs other
                # than main, history off the lineage, no size summaries) or not yet seen
                fallback = rmode == "ledger" and (not p or p.get("retained_bytes_ledger") is None)
                is_due = rmode != "off" and due(p, "retained_age_h", cfg.get("retained_bytes_every_hours", 24))
                retained_due = is_due and (rmode == "full" or fallback)
                meta_due = is_due and not retained_due          # ledger mode: old manifests only
                tr.log("retained", "measure now (all_files, all_manifests)" if retained_due else
                       "measure old manifests now (all_manifests); data bytes from the ledger's summary formula"
                       if meta_due else "carried from the last scan" if rmode != "off" else "off",
                       every_hours=cfg.get("retained_bytes_every_hours", 24), mode=rmode,
                       ledger_fallback=fallback)
                tm = probes.table_metrics(spark, table, info, cfg, pm_rows, retained=retained_due,
                                          retained_meta=meta_due)
                if retained_due or meta_due:
                    tm["retained_scanned_at"] = scanned_at
                    if retained_due:
                        tm["_retained_fresh"] = True      # the ledger compares its formula with it
                elif rmode != "off" and p:
                    tm["retained_scanned_at"] = p.get("retained_scanned_at")
                    tm["retained_metadata_bytes"] = p.get("retained_metadata_bytes")
                    if rmode == "full" or fallback:   # ledger mode, formula known: the ledger fills it in
                        tm["retained_bytes"] = p.get("retained_bytes")
                tm["minutes_since_writer_commit"] = wm
                tm["scan_mode"] = "full"
                carry_orphans(tm, p)
            if tm.get("scan_mode") == "reused":
                tr.rows("partition", pm_rows, ["partition_key", "data_files", "excess_files", "delete_files_pos",
                                               "minutes_since_update"])
            acted = acted_since_orphan_scan(p, info.get("uuid"), ages)
            orphan_due = cfg.get("orphan_scan", False) and (acted or due(
                p, "orphan_age_h", cfg.get("orphan_scan_every_hours", 24), changed=tm["scan_mode"] == "full"))
            tr.log("orphans", "list the location now" if orphan_due else "carried from the last listing",
                   orphan_scan=cfg.get("orphan_scan", False), action_since_listing=acted,
                   every_hours=cfg.get("orphan_scan_every_hours", 24))
            if orphan_due:
                if acted and tm["scan_mode"] == "reused":
                    tm["scan_mode"] = "reused+orphans"
                try:
                    pending = [r["path"] for r in led.store.range("freed_file", (info.get("uuid"),))] \
                        if info.get("uuid") else []
                    om = probes.orphan_metrics(spark, table, cfg, pending=pending)
                    om.pop("freed_files_listed", None)
                    tm.update(om, orphan_error=None, orphan_scanned_at=scanned_at)
                except Exception as oe:          # listing problems must not lose the table's metrics
                    tm["orphan_error"] = f"{type(oe).__name__}: {oe}"[:500]
            try:
                led.process(table, info.get("uuid"), tm, cfg,
                            full_fn=lambda: dict(probes.snapshot_metrics(spark, table, cfg, tm.get("data_bytes") or 0),
                                                 minutes_since_writer_commit=probes.writer_minutes(spark, table)),
                            live_keys=[r["partition_key"] if isinstance(r, dict) else r.partition_key
                                       for r in pm_rows],
                            partitioned=info.get("partitioned", True),
                            median_partition_bytes=median_bytes(pm_rows))
            except Exception as le:          # the ledger must never cost the table its metrics
                tm["ledger_event"] = f"error: {type(le).__name__}: {le}"[:300]
                print(f"  {table}: ledger skipped ({tm['ledger_event']})", flush=True)
        except Exception as e:  # one broken table must not stop the scan
            tm = {"load_error": (info.get("error") or "") + f" | {type(e).__name__}: {e}"[:500],
                  "scan_mode": "failed"}
            print(f"  {table}: FAILED {tm['load_error']}", flush=True)
        tm.update(scan_id=scan_id, scanned_at=scanned_at, table_name=table,
                  table_uuid=info.get("uuid"), target_source=cfg["target_source"],
                  metadata_location=info.get("metadata_location"),
                  metadata_json_bytes=info.get("metadata_json_bytes"),
                  scan_seconds=round(time.perf_counter() - t0, 2))
        tr.log("table_row", "written to ops.table_metrics", **{k: tm.get(k) for k in (
            "scan_mode", "partitions", "data_files", "data_bytes", "excess_files_total", "delete_files",
            "snapshots", "oldest_snapshot_age_h", "commits_24h", "data_manifests", "metadata_versions",
            "retained_bytes", "retained_metadata_bytes", "metadata_json_bytes", "overwrite_commits_recent",
            "avg_overwrite_rewrite_share", "table_turnover_24h", "minutes_since_writer_commit", "orphan_files",
            "ledger_event", "activity_event", "hot_window_min", "load_error")})
        tr.begin(None)
        if p and p.get("elapsed_min") is not None:     # how often this table is scanned (HISTORY_LOST)
            tm["minutes_since_previous_scan"] = round(float(p["elapsed_min"]), 2)
        log.append("table_metrics", [tm])
        uuid = info.get("uuid")
        if tm.get("scan_mode") != "failed":
            parts_out[table] = [dict(r if isinstance(r, dict) else r.asDict(), scan_id=scan_id,
                                     scanned_at=scanned_at, table_name=table) for r in pm_rows]
            row = scan_state.table_row(tm, store.get("table_state", (uuid,)) if uuid else None, window_h,
                                       probes.now_utc())
            if row:
                store.put("table_state", row)
            if uuid and tm.get("scan_mode") == "full":
                sms = scan_state.to_ms(scanned_at)
                prev_at = scan_state.partition_rows(store.range("partition_facts", (uuid,)), sms)
                puts, gone = scan_state.partition_changes(uuid, table, pm_rows, prev_at, sms)
                for r in puts:
                    store.put("partition_facts", r)
                for key in gone:
                    store.delete("partition_facts", key)
        summary.append(tm)
        modes[tm.get("scan_mode", "full")] = modes.get(tm.get("scan_mode", "full"), 0) + 1
        covered(table, "failed" if tm.get("scan_mode") == "failed" else "done", tm.get("scan_mode"))
        if coord is not None:
            coord.release_table(table, "run")
        print(f"  {tm.get('scan_mode', 'full'):6} {table}  {tm['scan_seconds']:.1f}s", flush=True)
    t_tables = time.perf_counter() - t_start
    print(f"=== {len(names)} tables in {t_tables:.1f}s: "
          + ", ".join(f"{k} {v}" for k, v in modes.items() if v) + " ===", flush=True)
    t_w = time.perf_counter()
    if coverage:
        log.append("coverage", coverage)
    log.flush()                              # logs first ...
    led.report(housekeeping=housekeeping)   # ... then the ledger's state (and its own logs)
    store.flush()                            # table_state / partition_facts (also with the ledger off)
    if housekeeping:                         # latest kinds append a row per change: keep one per table
        try:
            for kind in ("table_state", "action_state"):
                store.compact(kind)
        except Exception as e:
            print(f"  (scan state cleanup skipped: {type(e).__name__})", flush=True)
    RESULTS[scan_id] = {"tms": [normalize(spark, "table_metrics", dict(tm, scanned_ms=scan_state.to_ms(tm["scanned_at"])))
                                for tm in summary],
                        "parts": {t: [normalize(spark, "partition_metrics", r) for r in rows] for t, rows in parts_out.items()}}
    t_w = time.perf_counter() - t_w
    st = led.store.stats
    print(f"=== Timing: previous scan {t_prev:.1f}s, state preload {t_pre:.1f}s, tables {t_tables:.1f}s, writes + report {t_w:.1f}s; "
          f"state store: {st['queries']} queries, {st['table_loads']} table loads, {st['ranges']} range / "
          f"{st['gets']} get calls served"
          + (f"; not preloaded (too big): {st['preload_skipped']}" if st["preload_skipped"] else "") + " ===",
          flush=True)

    if report:
        print("\n=== Table metrics (selected) ===", flush=True)
        cols = ["table_name", "partitions", "data_files", "read_amplification", "skew_ratio",
                "undersized_partition_share", "delete_files", "snapshots", "data_manifests",
                "avg_changed_partitions_per_commit", "min_pruning_efficiency",
                "sort_order_defined", "distribution_mode"]
        from pyspark.sql.types import StructType, _parse_datatype_string
        full = _parse_datatype_string(TABLE_METRICS_DDL)
        sub = StructType([full[c] for c in cols])
        spark.createDataFrame([as_row(s, sub) for s in summary], sub).show(100, truncate=False)

        print("=== Partitions with excess files (top 15) ===", flush=True)
        top = sorted((r for rows in RESULTS[scan_id]["parts"].values() for r in rows
                      if (r.get("excess_files") or 0) > 0 or (r.get("delete_files_pos") or 0) > 0),
                     key=lambda r: (-(r.get("excess_files") or 0),
                                    -((r.get("delete_files_pos") or 0) + (r.get("delete_files_eq") or 0))))[:15]
        for r in top:
            m = r.get("minutes_since_update")
            print(f"  {r['table_name']:40} {str(r['partition_key'])[:40]:40} files {r.get('data_files')} "
                  f"excess {r.get('excess_files')} deletes "
                  f"{(r.get('delete_files_pos') or 0) + (r.get('delete_files_eq') or 0)} "
                  f"minutes {None if m is None else round(float(m), 1)}", flush=True)
    print(f"Metrics recorded under scan_id {scan_id}", flush=True)
    return scan_id


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--namespace", default="glue.demo")
    p.add_argument("--tables", default="", help="comma-separated table names in the namespace")
    p.add_argument("--config", default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                    "config", "health.json"))
    p.add_argument("--scan-id", default=None)
    p.add_argument("--full", action="store_true", help="measure every table, even unchanged ones")
    a = p.parse_args()

    spark = SparkSession.builder.appName("gl25-scan-metrics").getOrCreate()
    run_scan(spark, a.namespace, gl.load_config(a.config),
             tables=[t.strip() for t in a.tables.split(",") if t.strip()], scan_id=a.scan_id, full=a.full)
    spark.stop()


if __name__ == "__main__":
    main()
