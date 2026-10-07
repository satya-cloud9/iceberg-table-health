"""GL2.5m: incremental metadata scan, family 1 - the snapshot ledger.

Every scan reads each table's metadata.json anyway (to load the table). The
ledger keeps what it learns from it, so later scans only process snapshots
they haven't seen:

  glue.ops.snapshot_log      one row per snapshot ever seen (outlives expire_snapshots),
                             with the gap to the previous writer commit, computed once
  glue.ops.ledger_state      per table UUID: the watermark (last snapshot ingested,
                             last writer commit, current snapshot at that scan)
  glue.ops.commit_gap_hist   gap counts per table, day and bucket; percentiles sum
                             buckets instead of sorting history
  glue.ops.incremental_check shadow comparison: full value vs ledger value per metric

Per table and scan:
  1. read the snapshots from the loaded table (Java API, in memory: no extra I/O)
  2. lineage check: is the current snapshot a descendant of the one current at the
     last scan? (no -> rollback / replaced table: noted; the ledger keeps every
     snapshot by id, so family 1 stays correct, but family 2+ will fall back to full)
  3. gap check: does the oldest new snapshot's parent exist in the ledger? (no ->
     snapshots expired before the ledger saw them: the first new gap is unknown)
  4. ingest only snapshots newer than the watermark; writer commits (anything but
     'replace') get their gap; gaps go into today's histogram bucket
  5. compute the snapshot metrics (M1-M4, W2, writer age) from the ledger

Mode (config "incremental.snapshot_ledger"): off | shadow | on. In shadow the full
path stays authoritative and both are compared; in "on" the ledger values are
used, with a random share of tables (spot_check_share) and any table not
compared for reconcile_every_days still compared.

Pure logic (plan_ingest, ledger_metrics, bucket helpers) has no Spark or py4j
dependency so it is unit-tested locally.

Storage (migration step 1): every read and write of the ledger's state goes
through a StateStore (state_store.py: kinds ledger_state, commit, gap_hist,
activity_state, commit_partition, partition_state, late_hist), and the shadow
comparisons through a LogSink (incremental_check). The Iceberg store reads each
kind once per scan, so the ledger no longer runs SQL per table.
"""
import json
import math
import random
from datetime import datetime, timedelta, timezone

import activity as act
import expiry as xp
import gltrace as tr
import state_store as ss
import windows as win

SNAPSHOT_LOG_DDL = """
    table_uuid STRING, table_name STRING, snapshot_id BIGINT, parent_id BIGINT,
    committed_at TIMESTAMP, ts_ms BIGINT, operation STRING, is_writer BOOLEAN,
    added_data_files BIGINT, deleted_data_files BIGINT, added_delete_files BIGINT,
    added_files_size BIGINT, removed_files_size BIGINT, changed_partitions BIGINT,
    total_data_files BIGINT, total_files_size BIGINT,
    gap_min DOUBLE, gap_note STRING, scan_id STRING"""

LEDGER_STATE_DDL = """
    table_uuid STRING, table_name STRING, updated_at TIMESTAMP, last_ts_ms BIGINT,
    last_snapshot_id BIGINT, last_writer_ts_ms BIGINT, current_snapshot_id BIGINT,
    event STRING, scan_id STRING, last_checked_ms BIGINT"""

GAP_HIST_DDL = """
    table_uuid STRING, day DATE, bucket_max_min DOUBLE, gaps BIGINT, scan_id STRING"""

CHECK_DDL = """
    scan_id STRING, checked_at TIMESTAMP, table_name STRING, table_uuid STRING,
    family STRING, metric STRING, full_value STRING, ledger_value STRING,
    agree BOOLEAN, note STRING"""
ss.declare_log("incremental_check", CHECK_DDL, "checked_at", "days(checked_at)")

# Upper edges in minutes; None = longer than the last edge.
GAP_BUCKETS = [1, 2, 5, 10, 15, 30, 60, 120, 360, 720, 1440, 2880, 10080, None]

SUMMARY_KEYS = {
    "added_data_files": "added-data-files", "deleted_data_files": "deleted-data-files",
    "added_delete_files": "added-delete-files", "added_files_size": "added-files-size",
    "removed_files_size": "removed-files-size", "changed_partitions": "changed-partition-count",
    "total_data_files": "total-data-files", "total_files_size": "total-files-size",
}

# Metrics both paths produce, compared in shadow.
COMPARED = ["snapshots", "oldest_snapshot_age_h", "commits_1h", "commits_24h",
            "avg_added_files_per_commit", "avg_added_bytes_per_commit",
            "avg_changed_partitions_per_commit", "overwrite_commits_recent",
            "avg_overwrite_rewrite_share", "overwrite_commits_24h", "rewritten_bytes_24h",
            "table_turnover_24h", "metadata_versions", "minutes_since_writer_commit"]


# ---------------------------------------------------------------- pure logic

def bucket_of(gap_min):
    for edge in GAP_BUCKETS:
        if edge is None or gap_min <= edge:
            return edge
    return None


def exact_percentile(values, q):
    """Nearest-rank percentile (the definition the histogram uses too)."""
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    return vals[max(1, math.ceil(q * len(vals))) - 1]


def hist_percentile(counts, q):
    """counts: {bucket_edge: n}. -> (bucket upper edge, n); edge None = longer
    than the last edge; (None, 0) when empty."""
    total = sum(counts.values())
    if not total:
        return None, 0
    k = max(1, math.ceil(q * total))
    run = 0
    for edge in GAP_BUCKETS:
        run += counts.get(edge, 0)
        if run >= k:
            return edge, total
    return None, total


def bucket_lower(edge):
    """The lower edge of a bucket (exclusive)."""
    i = GAP_BUCKETS.index(edge)
    return 0 if i == 0 else GAP_BUCKETS[i - 1]


def ancestors(snaps_by_id, start_id):
    """Snapshot ids from start back through parents that are still in metadata."""
    out, sid = [], start_id
    while sid is not None and sid in snaps_by_id:
        out.append(sid)
        sid = snaps_by_id[sid]["parent_id"]
    return out, sid      # sid = first parent not in metadata (None = reached the root)


def plan_ingest(snaps, state, current_id, in_ledger):
    """Which snapshots to ingest and with which gaps.

    snaps:     every snapshot in metadata: dicts with snapshot_id, parent_id, ts_ms,
               operation and the SUMMARY_KEYS fields
    state:     the table's ledger_state row (dict) or None
    in_ledger: callable(snapshot_id) -> bool, consulted only when a parent is not
               in metadata (rare)
    -> (rows to ingest, oldest first; event string; new state dict)
    """
    by_id = {s["snapshot_id"]: s for s in snaps}
    events = []
    if not state:
        new = sorted(snaps, key=lambda s: (s["ts_ms"], s["snapshot_id"]))
        prev_writer = None
        events.append("bootstrap")
    else:
        new = sorted((s for s in snaps
                      if s["ts_ms"] > state["last_ts_ms"]
                      or (s["ts_ms"] == state["last_ts_ms"]
                          and s["snapshot_id"] != state["last_snapshot_id"]
                          and not in_ledger(s["snapshot_id"]))),
                     key=lambda s: (s["ts_ms"], s["snapshot_id"]))
        prev_writer = state.get("last_writer_ts_ms")
        # lineage: was the previously current snapshot an ancestor of the current one?
        # (only provable when the walk back reaches the root; if it stops at an
        # expired parent, lineage is unknown and the gap check below speaks)
        if current_id is not None and state.get("current_snapshot_id") is not None:
            chain, stop = ancestors(by_id, current_id)
            if state["current_snapshot_id"] not in chain and stop is None:
                events.append("lineage-break")
        # gap: the oldest new snapshot's parent must be known
        if new:
            parent = new[0]["parent_id"]
            if parent is not None and parent not in by_id and parent != state["last_snapshot_id"] \
                    and not in_ledger(parent):
                events.append("ledger-gap")
                prev_writer = None          # the gap before the first new writer commit is unknown

    rows = []
    for s in new:
        writer = s["operation"] != "replace"
        gap, note = None, None
        if writer:
            if prev_writer is not None:
                gap = round((s["ts_ms"] - prev_writer) / 60000.0, 4)
            else:
                note = "first-seen" if "bootstrap" in events else "unknown-after-gap"
            prev_writer = s["ts_ms"]
        rows.append(dict(s, is_writer=writer, gap_min=gap, gap_note=note))

    if rows:
        last = rows[-1]
        new_state = {"last_ts_ms": last["ts_ms"], "last_snapshot_id": last["snapshot_id"],
                     "last_writer_ts_ms": prev_writer, "current_snapshot_id": current_id}
    else:
        new_state = dict(state or {}, current_snapshot_id=current_id)
    return rows, ",".join(events) or ("new" if rows else "unchanged"), new_state


def _avg(vals, nd):
    vals = [float(v) for v in vals if v is not None]
    return round(sum(vals) / len(vals), nd) if vals else None


def ledger_metrics(retained, ledger_rows, now_ms, recent, data_bytes, metadata_versions):
    """The snapshot metrics, computed like probes.snapshot_metrics but from
    ledger rows. `retained` = snapshots in metadata now (in memory; gives the
    count and the oldest age); everything else comes from `ledger_rows`
    restricted to retained ids, so a missing or wrong ledger row shows up as a
    disagreement in shadow."""
    ids = {s["snapshot_id"] for s in retained}
    rows = sorted((r for r in ledger_rows if r["snapshot_id"] in ids),
                  key=lambda r: (r["ts_ms"], r["snapshot_id"]), reverse=True)
    m = {"snapshots": len(retained)}
    m["oldest_snapshot_age_h"] = (round((now_ms - min(s["ts_ms"] for s in retained)) / 3600000.0, 2)
                                  if retained else None)
    m["commits_1h"] = sum(1 for r in rows if r["ts_ms"] >= now_ms - 3600000)
    m["commits_24h"] = sum(1 for r in rows if r["ts_ms"] >= now_ms - 86400000)
    last_n = rows[:recent]
    m["avg_added_files_per_commit"] = _avg([r.get("added_data_files") for r in last_n], 2)
    m["avg_added_bytes_per_commit"] = _avg([r.get("added_files_size") for r in last_n], 0)
    m["avg_changed_partitions_per_commit"] = _avg([r.get("changed_partitions") for r in last_n], 2)
    ow = [r for r in last_n if r["operation"] == "overwrite" and (r.get("deleted_data_files") or 0) > 0]
    shares = []
    for r in ow:
        if r.get("total_data_files") is None:
            continue
        d, a = float(r.get("deleted_data_files") or 0), float(r.get("added_data_files") or 0)
        shares.append(d / max(float(r["total_data_files"]) - a + d, 1))
    m["overwrite_commits_recent"] = len(ow)
    m["avg_overwrite_rewrite_share"] = round(sum(shares) / len(shares), 3) if shares else None
    recent24 = [r for r in ow if r["ts_ms"] >= now_ms - 86400000]
    m["overwrite_commits_24h"] = len(recent24)
    m["rewritten_bytes_24h"] = int(sum(float(r.get("removed_files_size") or 0) for r in recent24))
    m["table_turnover_24h"] = round(m["rewritten_bytes_24h"] / data_bytes, 2) if data_bytes else None
    m["metadata_versions"] = metadata_versions
    writers = [r["ts_ms"] for r in rows if r["is_writer"]]
    m["minutes_since_writer_commit"] = round((now_ms - max(writers)) / 60000.0, 4) if writers else None
    return m


def reference_gaps(retained):
    """Exact gaps from metadata: each writer commit vs the previous writer commit
    still in metadata (what a lag() over .snapshots gives)."""
    out, prev = {}, None
    for s in sorted(retained, key=lambda s: (s["ts_ms"], s["snapshot_id"])):
        if s["operation"] == "replace":
            continue
        out[s["snapshot_id"]] = None if prev is None else round((s["ts_ms"] - prev) / 60000.0, 4)
        prev = s["ts_ms"]
    return out


def same(a, b, metric):
    if a is None or b is None:
        return a is None and b is None
    if metric in ("minutes_since_writer_commit", "oldest_snapshot_age_h"):
        return abs(float(a) - float(b)) <= 0.5     # minutes / hours: both read 'now' a moment apart
    if isinstance(a, float) or isinstance(b, float):
        return abs(float(a) - float(b)) <= max(1e-6, 1e-6 * abs(float(a)))
    return a == b


def compare(full, led, gaps_stored, gaps_ref, pct):
    """-> list of (metric, full_value, ledger_value, agree, note)."""
    out = []
    for k in COMPARED:
        if k in full:
            out.append((k, full.get(k), led.get(k), same(full.get(k), led.get(k), k), ""))
    bad = []
    for sid, ref in gaps_ref.items():
        st = gaps_stored.get(sid, "missing")
        if st == "missing":
            bad.append(f"{sid}: not in ledger")
        elif st[0] is None and ref is None:
            continue
        elif st[0] is None and st[1] == "unknown-after-gap":
            continue                        # expected: the predecessor expired unseen
        elif ref is None and st[0] is not None:
            continue                        # ledger knows a predecessor metadata no longer has
        elif not same(ref, st[0], "gap"):
            bad.append(f"{sid}: full {ref} vs ledger {st[0]}")
    out.append(("commit_gaps", f"{len(gaps_ref)} retained writer commits", f"{len(bad)} differ",
                not bad, "; ".join(bad[:5])))
    exact, edge, n = pct
    if n:
        ok = exact is not None and (exact <= edge if edge is not None else exact > GAP_BUCKETS[-2]) \
             and exact > (bucket_lower(edge) if edge is not None else GAP_BUCKETS[-2]) - 1e-9
        out.append(("commit_gap_p95_min", exact, f"<= {edge}" if edge is not None else "> 10080",
                    ok, f"{n} gaps in the window"))
    return out


# ---------------------------------------------------------------- Spark / py4j side

def mode_of(config, family="snapshot_ledger"):
    m = (config.get("incremental") or {}).get(family, "off")
    return m if m in ("off", "shadow", "on") else "off"


def read_snapshots(spark, table):
    """All snapshots in the loaded table's metadata (no I/O beyond the load),
    the current snapshot id and the metadata.json version count."""
    jt = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    snaps = []
    it = jt.snapshots().iterator()
    while it.hasNext():
        s = it.next()
        summ = s.summary()
        row = {"snapshot_id": int(s.snapshotId()),
               "parent_id": None if s.parentId() is None else int(s.parentId()),
               "ts_ms": int(s.timestampMillis()), "operation": str(s.operation())}
        for col, key in SUMMARY_KEYS.items():
            v = summ.get(key)
            row[col] = None if v is None else int(str(v))
        snaps.append(row)
    cur = jt.currentSnapshot()
    try:
        versions = int(jt.operations().current().previousFiles().size()) + 1
    except Exception:
        versions = None
    return snaps, (None if cur is None else int(cur.snapshotId())), versions


def read_refs(spark, table):
    """Branches and tags from the loaded table's metadata (no I/O beyond the load):
    [{name, type, snapshot_id, max_ref_age_ms, min_snapshots_to_keep, max_snapshot_age_ms}],
    or None when they can't be read."""
    try:
        jt = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
        refs = jt.refs()
        out = []
        for name in refs.keySet().toArray():
            r = refs.get(name)

            def opt(v):
                return None if v is None else int(v)
            out.append({"name": str(name), "type": str(r.type().toString()).lower(),
                        "snapshot_id": int(r.snapshotId()), "max_ref_age_ms": opt(r.maxRefAgeMs()),
                        "min_snapshots_to_keep": opt(r.minSnapshotsToKeep()),
                        "max_snapshot_age_ms": opt(r.maxSnapshotAgeMs())})
        return out
    except Exception:
        return None


def ensure_tables(spark, ops):
    import gl_common as gl
    specs = {"snapshot_log": (SNAPSHOT_LOG_DDL, "PARTITIONED BY (days(committed_at))"),
             "ledger_state": (LEDGER_STATE_DDL, ""),
             "commit_gap_hist": (GAP_HIST_DDL, ""),
             "incremental_check": (CHECK_DDL, "PARTITIONED BY (days(checked_at))")}
    for name, (ddl, part) in specs.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {ops}.{name} ({ddl}) USING iceberg {part}")
        gl.ensure_columns(spark, f"{ops}.{name}", ddl)


def ensure_activity_tables(spark, ops):
    import gl_common as gl
    specs = {"partition_activity": (act.ACTIVITY_DDL, "PARTITIONED BY (days(committed_at))"),
             "partition_state": (act.PARTITION_STATE_DDL, ""),
             "lateness_hist": (act.LATENESS_HIST_DDL, ""),
             "activity_state": (act.ACTIVITY_STATE_DDL, ""),
             "incremental_check": (CHECK_DDL, "PARTITIONED BY (days(checked_at))")}
    for name, (ddl, part) in specs.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {ops}.{name} ({ddl}) USING iceberg {part}")
        gl.ensure_columns(spark, f"{ops}.{name}", ddl)


class Ledger:
    """One per scan: reads and writes state through the store, logs through the sink."""

    def __init__(self, spark, config, scan_id, store=None, log=None, uuids=None):
        import gl_common as gl
        self.spark, self.config, self.scan_id = spark, config, scan_id
        self.ops = gl.OPS_NAMESPACE
        self.store = store or ss.IcebergStateStore(spark, self.ops)
        self.log = log or ss.IcebergLogSink(spark, self.ops)
        self.mode = mode_of(config)
        inc = config.get("incremental") or {}
        self.spot_share = float(inc.get("spot_check_share", 0.05))
        self.reconcile_days = float(inc.get("reconcile_every_days", 7))
        self.hist_days = int(inc.get("histogram_days", 30))
        # adaptive lookback (family 3 and the possible-backfill baseline)
        self.min_batches = int(inc.get("min_batches", inc.get("min_samples", 20)))
        self.min_gaps = int(inc.get("min_gaps", inc.get("min_samples", 20)))
        self.max_days = (max(int(inc.get("lookback_max_days", 365)), self.hist_days)
                         if inc.get("adaptive_lookback", True) else self.hist_days)
        self.retention_days = int(inc.get("retention_days", 365))
        self.results = []          # per table: (table, agree, n_checks, disagreements, ingested, event)
        self.mode3 = mode_of(config, "learned_windows")
        self.mode2 = mode_of(config, "partition_activity")
        if self.mode2 == "on":     # family 2 only reports in this patch; "on" behaves like shadow
            self.mode2 = "shadow"
        self.results2 = []
        self.results_retained = []
        self.pstate_cache = {}
        # What is read up front: the run's tables (uuids, when known), commits back
        # to the percentile window (older retained snapshots load per table as
        # needed), histograms back to the adaptive lookback; nothing in bulk past
        # store_preload_max_rows (tables then load one by one).
        now = datetime.now(timezone.utc)
        self.window_days = max(self.hist_days, 30)
        preload_days = int(inc.get("store_preload_days", self.window_days + 1))
        commits_from = int((now - timedelta(days=preload_days)).timestamp() * 1000)
        hist_from = now.date() - timedelta(days=self.max_days)
        cap = int(inc.get("store_preload_max_rows", 2000000))
        self.pstate_keep_days = float(inc.get("partition_state_keep_days", 7))
        uu = sorted(uuids) if uuids else None
        if self.mode2 != "off":
            ensure_activity_tables(spark, self.ops)
            for kind, start in (("activity_state", None), ("partition_state", None),
                                ("commit_partition", commits_from), ("late_hist", hist_from)):
                self.store.preload(kind, start=start, uuids=uu, max_rows=cap)
        if self.mode != "off":
            ensure_tables(spark, self.ops)
            for kind, start in (("ledger_state", None), ("commit", commits_from), ("gap_hist", hist_from)):
                self.store.preload(kind, start=start, uuids=uu, max_rows=cap)
            # files earlier expiries freed and still waiting out their grace (freed_files.py):
            # the orphan listing must not count them, and the scan reports how many wait
            self.store.preload("freed_file", uuids=uu, max_rows=cap)

    # -- per table ---------------------------------------------------------
    def process(self, table, uuid, tm, cfg, full_fn=None, live_keys=None, partitioned=True,
                median_partition_bytes=None):
        """Family 1 (snapshot ledger) and family 2 (partition activity) for one table."""
        if not uuid or (self.mode == "off" and self.mode2 == "off"):
            return
        snaps, current, versions = read_snapshots(self.spark, table)
        self._from_ms = self._read_from(snaps)
        self._expiry(table, uuid, tm, cfg, snaps, current)
        if self.mode != "off":
            self._family1(table, uuid, tm, cfg, full_fn, snaps, current, versions)
        if self.mode2 != "off":
            self._family2(table, uuid, tm, cfg, snaps, live_keys or [], partitioned, median_partition_bytes)
        if self.mode3 != "off" and self.mode != "off" and self.mode2 != "off":
            self._windows(uuid, tm, cfg)

    def _read_from(self, snaps):
        """How far back this table's commit and activity reads go: its oldest
        retained snapshot, the percentile window (from midnight of its first day)
        and the 30-day label window, whichever is earliest."""
        now = datetime.now(timezone.utc)
        cut = self._cutoff_day()
        cut_ms = int(datetime(cut.year, cut.month, cut.day, tzinfo=timezone.utc).timestamp() * 1000)
        since30 = int((now - timedelta(days=30)).timestamp() * 1000)
        return min([cut_ms, since30] + [s["ts_ms"] for s in snaps])

    def _expiry(self, table, uuid, tm, cfg, snaps, current):
        """GL2.6e: the retention policy and what it would expire, refs, and retained
        bytes from snapshot summaries (compared with the full query when that ran)."""
        refs = read_refs(self.spark, table)
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        try:
            props = json.loads(tm.get("properties_json") or "{}")
        except ValueError:
            props = {}
        policy = xp.resolve_policy(props, cfg, snaps, now_ms)
        tm.update(xp.expiry_facts(snaps, refs, current, now_ms, policy, cfg))
        tm["freed_files_waiting"] = len(self.store.range("freed_file", (uuid,))) if uuid else None
        led_bytes, note = xp.retained_from_summaries(snaps, refs, current)
        tm["retained_bytes_ledger"], tm["retained_ledger_note"] = led_bytes, note
        mode = str(cfg.get("retained_bytes", "full"))
        tr.log("expiry", f"policy {policy['age_h']:g} h / keep {policy['min_keep']} ({policy['source']})",
               category=policy["category"], writer_commits_24h=policy["writer_commits_24h"],
               expirable=tm["expirable_snapshots"], oldest_expirable_h=tm["oldest_expirable_age_h"],
               refs=tm["refs_json"], stale_refs=tm["stale_refs"])
        tr.log("retained_ledger", note, ledger_bytes=led_bytes, full_bytes=tm.get("retained_bytes"),
               full_measured_now=bool(tm.get("_retained_fresh")), mode=mode)
        if tm.get("_retained_fresh") and tm.get("retained_bytes") is not None:
            full = int(tm["retained_bytes"])
            ok = led_bytes is None or abs(led_bytes - full) <= max(1024, full * 0.001)
            self.log.append("incremental_check", [{
                "scan_id": self.scan_id, "checked_at": datetime.now(timezone.utc), "table_name": table,
                "table_uuid": uuid, "family": "retained_ledger", "metric": "retained_bytes",
                "full_value": str(full), "ledger_value": None if led_bytes is None else str(led_bytes),
                "agree": ok, "note": note}])
            self.results_retained.append((table, ok, full, led_bytes, note))
        if mode == "ledger" and led_bytes is not None:
            tm["retained_bytes"] = led_bytes
        elif mode == "off":
            tm["retained_bytes"] = None

    def _windows(self, uuid, tm, cfg):
        """Family 3 inputs, recorded per table for detect_symptoms."""
        inc = self.config.get("incremental") or {}
        p99, n_late, late_days = self._late_adaptive(uuid, 0.99)
        tm["lateness_p99_h"] = p99
        tm["lateness_p99_batches"] = n_late
        tm["lateness_lookback_days"] = late_days
        gaps, n_gaps, gap_days = self._gaps_adaptive(uuid)
        kept, idle = win.drop_idle(gaps, GAP_BUCKETS, float(inc.get("idle_gap_factor", 10)))
        hot_edge, n_hot = hist_percentile(kept, 0.95)
        tm["hot_gap_p95_min"] = hot_edge
        tm["hot_gaps_used"] = n_hot
        tm["idle_gaps_ignored"] = idle
        tm["gap_lookback_days"] = gap_days
        tm["hot_window_min"], tm["hot_window_source"] = win.learned_hot(
            hot_edge, n_hot, float(cfg.get("hot_partition_minutes", 15)), float(inc.get("hot_cap_minutes", 1440)),
            self.min_gaps, float(inc.get("hot_gap_factor", 2.0)))
        tm["settle_window_h"], tm["settle_window_source"] = win.learned_settle(
            p99, n_late, float(inc.get("settle_cap_hours", 168)), self.min_batches)
        tr.log("family3", "learned windows (inputs for detect)", gap_counts=gaps, idle_gaps_dropped=idle,
               gap_p95_bucket_min=hot_edge, gaps_used=n_hot, gap_lookback_days=gap_days,
               hot_window_min=tm["hot_window_min"], hot_source=tm["hot_window_source"],
               lateness_p99_bucket_h=p99, late_batches=n_late, late_lookback_days=late_days,
               settle_window_h=tm["settle_window_h"], settle_source=tm["settle_window_source"])

    def _day_hist(self, uuid, kind, bucket_col, n_col):
        """{day: {bucket_edge: n}} for one table, back to max_days (exclusive)."""
        today = datetime.now(timezone.utc).date()
        since = today - timedelta(days=self.max_days)
        out = {}
        for r in self.store.range(kind, (uuid,), start=since + timedelta(days=1)):
            day = out.setdefault(r["day"], {})
            b = r[bucket_col]
            day[b] = day.get(b, 0) + int(r[n_col] or 0)
        return out, today

    def _late_adaptive(self, uuid, q):
        """-> (percentile edge, batches, days looked back)."""
        days, today = self._day_hist(uuid, "late_hist", "bucket_max_h", "batches")
        counts, n, used = win.adaptive_window(days, today, self.hist_days, self.max_days, self.min_batches)
        edge, _ = act.hist_percentile(counts, q)
        return edge, n, used

    def _gaps_adaptive(self, uuid):
        """-> (gap counts, gaps, days looked back)."""
        days, today = self._day_hist(uuid, "gap_hist", "bucket_max_min", "gaps")
        return win.adaptive_window(days, today, self.hist_days, self.max_days, self.min_gaps)

    def _family1(self, table, uuid, tm, cfg, full_fn, snaps, current, versions):
        """Ingest new snapshots, compute ledger metrics, compare (shadow / spot
        check / reconcile), and in "on" mode put the ledger values into tm."""
        state = self.store.get("ledger_state", (uuid,))

        def in_ledger(sid):
            return any(r["snapshot_id"] == sid for r in self.store.range("commit", (uuid,)))

        rows, event, new_state = plan_ingest(snaps, state, current, in_ledger)
        tr.log("family1", "snapshot ledger: " + ("first sight, every retained snapshot ingested" if state is None
                                                 else f"{len(rows)} snapshot(s) newer than the watermark"),
               mode=self.mode, event=event, retained_snapshots=len(snaps),
               watermark_ts_ms=None if not state else state.get("last_ts_ms"))
        tr.rows("family1.ingest", rows, ["snapshot_id", "operation", "is_writer", "gap_min", "gap_note",
                                         "added_data_files", "deleted_data_files", "added_files_size"])
        now = datetime.now(timezone.utc)
        now_ms = int(now.timestamp() * 1000)
        # compared with the full path this scan? (the last comparison's time is in ledger_state)
        last_checked = (state or {}).get("last_checked_ms")
        age_d = 1e9 if last_checked is None else (now_ms - int(last_checked)) / 86400000.0
        check = (self.mode == "shadow" or random.random() < self.spot_share or age_d >= self.reconcile_days)
        counts = {}
        for r in rows:
            if r["gap_min"] is not None:
                day = ss.day_of(r["ts_ms"])
                counts[(day, bucket_of(r["gap_min"]))] = counts.get((day, bucket_of(r["gap_min"])), 0) + 1
        with self.store.transaction():          # new commits, counters, then the watermark
            for r in rows:
                self.store.put("commit", dict(
                    r, table_uuid=uuid, table_name=table, scan_id=self.scan_id,
                    committed_at=datetime.fromtimestamp(r["ts_ms"] / 1000.0, timezone.utc)))
            for (d, b), n in counts.items():
                self.store.increment("gap_hist", {"table_uuid": uuid, "day": d, "bucket_max_min": b,
                                                  "gaps": n, "scan_id": self.scan_id}, "gaps")
            if rows or state is None or event != "unchanged" or check:
                self.store.put("ledger_state", dict(new_state, table_uuid=uuid, table_name=table,
                                                    updated_at=now, event=event, scan_id=self.scan_id,
                                                    last_checked_ms=now_ms if check else last_checked))

        # ledger rows needed for the metrics: the newest `recent` retained snapshots
        # plus every retained snapshot of the last 24 h (bounded by recent activity)
        recent = int(cfg["recent_commits"])
        by_ts = sorted(snaps, key=lambda s: (s["ts_ms"], s["snapshot_id"]), reverse=True)
        need = ({s["snapshot_id"] for s in by_ts[:recent]}
                | {s["snapshot_id"] for s in snaps if s["ts_ms"] >= now_ms - 86400000})
        last_writer = next((s for s in by_ts if s["operation"] != "replace"), None)
        if last_writer:
            need.add(last_writer["snapshot_id"])
        lrows = self._log_rows(uuid, need, "*")
        led = ledger_metrics(snaps, lrows, now_ms, recent, tm.get("data_bytes") or 0, versions)
        p95_edge, n_gaps = self._hist_p95(uuid)
        tm["commit_gap_p95_min"] = p95_edge
        tm["commit_gaps_window"] = n_gaps
        tm["ledger_new_snapshots"] = len(rows)
        tm["ledger_event"] = event

        disagreements, n_checks, full = [], 0, tm
        if check:
            ref = reference_gaps(snaps)
            stored = {r["snapshot_id"]: (r["gap_min"], r["gap_note"])
                      for r in self._log_rows(uuid, ref, "snapshot_id, gap_min, gap_note")}
            exact = self._exact_p95(uuid)
            full = tm
            if self.mode == "on" and full_fn is not None:   # spot check: tm holds no full values
                full = full_fn()
            res = compare(full, led, stored, ref, (exact, p95_edge, n_gaps))
            why = "shadow" if self.mode == "shadow" else "spot-check/reconcile"
            self.log.append("incremental_check", [{
                "scan_id": self.scan_id, "checked_at": now, "table_name": table, "table_uuid": uuid,
                "family": "snapshot_ledger", "metric": m, "full_value": None if f is None else str(f),
                "ledger_value": None if l is None else str(l), "agree": ok,
                "note": (note + f" [{why}; {event}]").strip()} for m, f, l, ok, note in res])
            n_checks = len(res)
            disagreements = [(m, f, l, note) for m, f, l, ok, note in res if not ok]
        if self.mode == "on":
            if disagreements:      # don't trust the ledger for this table this scan
                tm.update({k: v for k, v in full.items() if k in COMPARED})
                tm["ledger_event"] = event + ",fallback-full"
            else:
                tm.update({k: v for k, v in led.items() if k in COMPARED})
        tr.log("family1.check", ("compared with the full path: " + ("agree" if not disagreements else
                                  f"{len(disagreements)} disagree") if check else "not compared this scan"),
               why=("shadow" if self.mode == "shadow" else "spot check / reconcile") if check else None,
               used=("full path (fallback)" if disagreements else "ledger") if self.mode == "on" else "full path",
               disagreements=[d[:3] for d in disagreements][:5] or None,
               **{k: led.get(k) for k in ("snapshots", "commits_24h", "overwrite_commits_recent",
                                          "table_turnover_24h", "minutes_since_writer_commit")})
        self.results.append((table, not disagreements, n_checks, disagreements, len(rows), event))

    # -- family 2 ----------------------------------------------------------
    def _family2(self, table, uuid, tm, cfg, snaps, live_keys, partitioned, median_partition_bytes=None):
        spark = self.spark
        now = datetime.now(timezone.utc)
        now_ms = int(now.timestamp() * 1000)
        st = self.store.get("activity_state", (uuid,))
        if st is None:
            new_snaps, event = sorted(snaps, key=lambda x: (x["ts_ms"], x["snapshot_id"])), "bootstrap"
        else:
            new_snaps = sorted((x for x in snaps if x["ts_ms"] > st["last_ts_ms"]
                                or (x["ts_ms"] == st["last_ts_ms"] and x["snapshot_id"] != st["last_snapshot_id"])),
                               key=lambda x: (x["ts_ms"], x["snapshot_id"]))
            event = "new" if new_snaps else "unchanged"

        rows = []
        if new_snaps:
            files = act.read_activity(spark, table, [x["snapshot_id"] for x in new_snaps])
            # the table's lateness as it stood before this scan: the baseline for possible backfills
            base_p99, base_n, _ = self._late_adaptive(uuid, 0.99)
            inc = self.config.get("incremental") or {}
            th = dict(cfg.get("thresholds", {}), min_batches=self.min_batches,
                      cold_start_backfill_hours=float(inc.get("cold_start_backfill_hours", 168)))
            for x in new_snaps:
                commit_rows = act.aggregate(x, files.get(x["snapshot_id"], []))
                earlier = st is not None or any(s["ts_ms"] < x["ts_ms"] for s in snaps)
                act.label_rows(x, commit_rows, len(live_keys), base_p99, base_n, median_partition_bytes, th,
                               had_snapshots=earlier)
                rows += commit_rows
        tr.log("family2", "partition activity: " + (f"{len(new_snaps)} new snapshot(s), manifests they wrote read"
                                                    if new_snaps else "no new snapshots"),
               mode=self.mode2, event=event)
        tr.rows("family2.activity", rows, ["snapshot_id", "operation", "partition_key", "label", "data_files_added",
                                          "data_files_removed", "delete_files_added", "data_bytes_removed",
                                          "lateness_h"])
        # the latest possible full refresh ever: kept in activity_state so no read
        # goes back further than the window (seeded once from the stored history)
        last_refresh = st.get("last_full_refresh_ms") if st else None
        seed = st is not None and not st.get("refresh_seeded")
        if seed:
            last_refresh = max((r["ts_ms"] for r in self.store.range("commit_partition", (uuid,))
                                if r.get("label") == "possible_full_refresh"), default=None)
        new_refresh = max((r["ts_ms"] for r in rows if r.get("label") == "possible_full_refresh"), default=None)
        if new_refresh is not None:
            last_refresh = new_refresh if last_refresh is None else max(last_refresh, new_refresh)
        state = self._partition_state(uuid)
        changed, reopens = act.update_state(state, rows)
        for pk, v in changed.items():
            tr.log("family2.state", pk, **v)
        state.update(changed)
        counts = {}
        for r in rows:
            if r["lateness_h"] is not None and r.get("label") in act.APPEND_LABELS:   # only real late data
                key = (ss.day_of(r["ts_ms"]), act.late_bucket(r["lateness_h"]))
                counts[key] = counts.get(key, 0) + 1
        with self.store.transaction():          # activity, counters, partition state, then the watermark
            for r in rows:
                self.store.put("commit_partition", dict(
                    r, table_uuid=uuid, table_name=table, scan_id=self.scan_id,
                    committed_at=datetime.fromtimestamp(r["ts_ms"] / 1000.0, timezone.utc)))
            for (d, b), n in counts.items():
                self.store.increment("late_hist", {"table_uuid": uuid, "day": d, "bucket_max_h": b,
                                                   "batches": n, "scan_id": self.scan_id}, "batches")
            for pk, v in changed.items():
                self.store.put("partition_state", dict(v, table_uuid=uuid, partition_key=pk, updated_at=now,
                                                       scan_id=self.scan_id))
            pruned = self._prune_partition_state(uuid, state, live_keys, now_ms)
            if new_snaps or seed:
                last = new_snaps[-1] if new_snaps else st
                self.store.put("activity_state", {"table_uuid": uuid, "table_name": table, "updated_at": now,
                                                  "last_ts_ms": last["ts_ms"] if new_snaps else last.get("last_ts_ms"),
                                                  "last_snapshot_id": last["snapshot_id"] if new_snaps
                                                  else last.get("last_snapshot_id"),
                                                  "event": event, "scan_id": self.scan_id,
                                                  "last_full_refresh_ms": last_refresh, "refresh_seeded": True})

        hot = float(cfg.get("hot_partition_minutes", 15))
        live = set(live_keys)
        led_lw = {pk: v["last_write_ms"] for pk, v in state.items() if v.get("last_write_ms") is not None}
        late_edge, n_late = self._late_p95(uuid)
        tm["activity_new_snapshots"] = len(new_snaps)
        tm["activity_event"] = event
        tm["lateness_p95_h"] = late_edge
        tm["lateness_batches_window"] = n_late
        tm.update(self._label_stats(uuid, snaps, last_refresh))
        if pruned:
            tr.log("family2.state", f"{pruned} partition(s) no longer in the table dropped from partition_state")
        tm["reopened_partitions"] = sum(1 for pk in live if (state.get(pk) or {}).get("reopen_count"))
        tm["hot_partitions_ledger"] = sum(1 for pk in live if pk in led_lw and now_ms - led_lw[pk] < hot * 60000)

        # shadow comparison against all_entries
        ref_act, ref_lw = act.full_reference(spark, table, partitioned)
        retained = {x["snapshot_id"] for x in snaps}
        led_act = {(r["snapshot_id"], r["partition_key"]): act.activity_tuple(r)
                   for r in self._activity_rows(uuid, retained)}
        oldest = min((x["ts_ms"] for x in snaps), default=None)
        ok_a, n_a, bad_a = act.compare_activity(ref_act, led_act, retained)
        ok_w, n_w, bad_w = act.compare_last_write(ref_lw, led_lw, live, oldest)
        exact = self._late_exact(uuid)
        res = [("activity", f"{n_a} (snapshot, partition) pairs", f"{len(bad_a)} differ", ok_a, "; ".join(bad_a[:5])),
               ("last_write", f"{n_w} live partitions", f"{len(bad_w)} differ", ok_w, "; ".join(bad_w[:5]))]
        if n_late:
            lo = act.LATE_BUCKETS[act.LATE_BUCKETS.index(late_edge) - 1] if late_edge in act.LATE_BUCKETS[1:] else 0
            if late_edge is None:
                ok_l = exact is not None and exact > act.LATE_BUCKETS[-2]
            else:
                ok_l = exact is not None and lo - 1e-9 < exact <= late_edge
            res.append(("lateness_p95_h", exact, f"<= {late_edge}" if late_edge is not None else "> 720",
                        ok_l, f"{n_late} batches in the window"))
        self.log.append("incremental_check", [{
            "scan_id": self.scan_id, "checked_at": now, "table_name": table, "table_uuid": uuid,
            "family": "partition_activity", "metric": m, "full_value": None if f is None else str(f),
            "ledger_value": None if l is None else str(l), "agree": ok,
            "note": (note + f" [shadow; {event}]").strip()} for m, f, l, ok, note in res])
        dis = [(m, f, l, note) for m, f, l, ok, note in res if not ok]
        self.results2.append((table, not dis, len(res), dis, len(new_snaps), event, reopens))

    def _partition_state(self, uuid):
        """The table's partition state, as a dict the caller updates in place."""
        if uuid not in self.pstate_cache:
            self.pstate_cache[uuid] = {r["partition_key"]: {"last_write_ms": r["last_write_ms"],
                                                            "last_compaction_ms": r["last_compaction_ms"],
                                                            "reopen_count": r["reopen_count"] or 0}
                                       for r in self.store.range("partition_state", (uuid,))}
        return self.pstate_cache[uuid]

    def _prune_partition_state(self, uuid, state, live_keys, now_ms):
        """Forget partitions that are no longer in the table and saw no write or
        compaction for partition_state_keep_days (a partition only dropped for a
        moment, e.g. a reload, keeps its history). -> how many were dropped."""
        if not live_keys:
            return 0                      # no partition list this scan: never prune on that
        live = set(live_keys)
        keep_ms = self.pstate_keep_days * 86400000
        gone = [pk for pk, v in state.items() if pk not in live
                and now_ms - max(v.get("last_write_ms") or 0, v.get("last_compaction_ms") or 0) > keep_ms]
        for pk in gone:
            self.store.delete("partition_state", (uuid, pk))
            del state[pk]
        return len(gone)

    def _activity_rows(self, uuid, ids):
        ids = set(ids)
        return [r for r in self.store.range("commit_partition", (uuid,), start=self._from_ms)
                if r["snapshot_id"] in ids] if ids else []

    def _late_p95(self, uuid):
        return self._late_pct(uuid, 0.95)

    def _hist_counts(self, kind, uuid, bucket_col, n_col):
        counts = {}
        for r in self.store.range(kind, (uuid,), start=self._cutoff_day()):
            counts[r[bucket_col]] = counts.get(r[bucket_col], 0) + int(r[n_col] or 0)
        return counts

    def _late_pct(self, uuid, q):
        return act.hist_percentile(self._hist_counts("late_hist", uuid, "bucket_max_h", "batches"), q)

    def _late_exact(self, uuid):
        cutoff = self._cutoff_day()
        # rows written before labels existed (label NULL) were counted into the histogram too
        vals = [r["lateness_h"] for r in self.store.range("commit_partition", (uuid,), start=self._from_ms)
                if r["lateness_h"] is not None and (r.get("label") is None or r["label"] in act.APPEND_LABELS)
                and ss.day_of(r["ts_ms"]) >= cutoff]
        return act.exact_percentile(vals, 0.95)

    def _label_stats(self, uuid, snaps, last_refresh_ms=None):
        """Per table: possible full refreshes and backfills (last 30 days), the
        retained full copies and the snapshot just before the latest refresh.
        last_refresh_ms: the table's latest refresh ever (activity_state), for
        when none falls in the window."""
        since = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp() * 1000)
        rows = [r for r in self.store.range("commit_partition", (uuid,), start=self._from_ms)
                if r.get("label") in ("possible_full_refresh", "possible_backfill")]
        seen, uniq = set(), []
        for r in rows:
            k = (r["snapshot_id"], r.get("partition_key"), r["label"])
            if k not in seen:
                seen.add(k)
                uniq.append(r)
        refresh_ids = {r["snapshot_id"] for r in uniq if r["label"] == "possible_full_refresh"}
        recent = {r["snapshot_id"]: r["ts_ms"] for r in uniq
                  if r["label"] == "possible_full_refresh" and r["ts_ms"] >= since}
        refresh_bytes = sum(int(r["data_bytes_added"] or 0) for r in uniq
                            if r["label"] == "possible_full_refresh" and r["ts_ms"] >= since)
        by_id = {x["snapshot_id"]: x for x in snaps}
        retained = sorted((by_id[i] for i in refresh_ids if i in by_id), key=lambda x: x["ts_ms"])
        latest = retained[-1] if retained else None
        parent = by_id.get(latest["parent_id"]) if latest else None
        return {"possible_full_refreshes_30d": len(recent),
                "possible_backfill_batches_30d": sum(1 for r in uniq
                                                     if r["label"] == "possible_backfill" and r["ts_ms"] >= since),
                "last_full_refresh_ms": max(recent.values()) if recent else
                    max([r["ts_ms"] for r in uniq if r["label"] == "possible_full_refresh"]
                        + ([last_refresh_ms] if last_refresh_ms is not None else []), default=None),
                "full_refresh_avg_bytes": int(refresh_bytes / len(recent)) if recent else None,
                "retained_full_copies": len(retained),
                "pre_refresh_snapshot_ms": parent["ts_ms"] if parent else None}

    def _log_rows(self, uuid, ids, cols=None):
        ids = set(ids)
        return [r for r in self.store.range("commit", (uuid,), start=self._from_ms)
                if r["snapshot_id"] in ids] if ids else []

    def _cutoff_day(self):
        return (datetime.now(timezone.utc) - timedelta(days=self.hist_days)).date()

    def _hist_p95(self, uuid):
        return hist_percentile(self._hist_counts("gap_hist", uuid, "bucket_max_min", "gaps"), 0.95)

    def _exact_p95(self, uuid):
        cutoff = self._cutoff_day()
        vals = [r["gap_min"] for r in self.store.range("commit", (uuid,), start=self._from_ms)
                if r["gap_min"] is not None and ss.day_of(r["ts_ms"]) >= cutoff]
        return exact_percentile(vals, 0.95)

    def flush(self):
        """Write the scan's state and log records (one append per ops table, one
        MERGE for partition_state)."""
        if self.mode == "off" and self.mode2 == "off":
            return
        self.log.flush()          # logs first, then state: a crash between means redo, not loss
        self.store.flush()

    # -- end of scan -------------------------------------------------------
    def report(self, housekeeping=True):
        """Flush, print the comparisons, and (when this run holds the
        housekeeping claim) do the retention pass."""
        if self.mode == "off" and self.mode2 == "off":
            return
        self.flush()
        if self.mode2 != "off":
            ok2 = sum(1 for r in self.results2 if r[1])
            snaps2 = sum(r[4] for r in self.results2)
            reop = sum(r[6] for r in self.results2)
            boot = sum(1 for r in self.results2 if r[5] == "bootstrap")
            print(f"\n=== Incremental check ({self.mode2}, partition activity): {ok2}/{len(self.results2)} tables "
                  f"agree; {snaps2} snapshots read{'; bootstrap ' + str(boot) if boot else ''}"
                  f"{'; ' + str(reop) + ' partitions reopened' if reop else ''} ===", flush=True)
            for table, agree, n, dis, _, event, _r in self.results2:
                for m, f, l, note in dis:
                    print(f"  {table.rsplit('.', 1)[-1]:26} {m}: full={f} ledger={l}  {note} [{event}]", flush=True)
        if self.results_retained:
            ok_r = sum(1 for r in self.results_retained if r[1])
            print(f"\n=== Retained bytes, summary formula vs full query: {ok_r}/{len(self.results_retained)} "
                  f"tables agree ===", flush=True)
            for table, ok, full, led, note in self.results_retained:
                if not ok or led is None:
                    print(f"  {table.rsplit('.', 1)[-1]:26} full={full} ledger={led}  {note}", flush=True)
        if self.mode == "off":
            return
        checked = [r for r in self.results if r[2]]
        ok = sum(1 for r in checked if r[1])
        ingested = sum(r[4] for r in self.results)
        events = {}
        for r in self.results:
            for e in r[5].split(","):
                if e not in ("unchanged", "new"):
                    events[e] = events.get(e, 0) + 1
        ev = ", ".join(f"{k} {v}" for k, v in sorted(events.items()))
        print(f"\n=== Incremental check ({self.mode}, snapshot ledger): {ok}/{len(checked)} tables agree; "
              f"{ingested} new snapshots ingested{'; ' + ev if ev else ''} ===", flush=True)
        for table, agree, n, dis, _, event in self.results:
            for m, f, l, note in dis:
                print(f"  {table.rsplit('.', 1)[-1]:26} {m}: full={f} ledger={l}  {note} [{event}]", flush=True)
        if not housekeeping:
            return
        try:   # keep the stores bounded (DynamoDB: a TTL attribute instead)
            self.log.expire("incremental_check", 30)
            keep = max(self.hist_days * 3, 90, self.max_days + 1)
            self.store.expire("gap_hist", keep)
            if self.mode2 != "off":
                self.store.expire("late_hist", keep)
            self.store.expire("commit", self.retention_days)
            if self.mode2 != "off":
                self.store.expire("commit_partition", self.retention_days)
            for kind in ("ledger_state", "activity_state"):
                self.store.compact(kind)
        except Exception as e:
            print(f"  (ledger retention cleanup skipped: {type(e).__name__})", flush=True)
