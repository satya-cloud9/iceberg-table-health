"""Scan state: what the next run needs from this one, kept in the state store
instead of read back from the logs.

  table_state      (latest, per table)   the table's latest facts (the table_metrics
                                         row as written), when it was scanned, and a
                                         short trend for the trend rules
  partition_facts  (merge, per partition) each partition's latest facts; rewritten
                                         only when the partition changed
  action_state     (latest, per table)   the latest advisor action and the recent ones
                                         (written by the plan)

The scan reads these to decide what to reuse and what is due, detect reads the
trend and recent actions, the plan reads each table's latest facts. The logs
(table_metrics, partition_metrics, symptoms, actions) stay a history for people
and reports; no code reads yesterday's log rows on the scan path.

Partition rows of a reused table are rebuilt from partition_facts: each stored
row keeps its own scanned_ms, so its age moves on correctly although unchanged
rows are not rewritten.
"""
import json
from datetime import datetime, timezone

TABLE_STATE_DDL = """
    table_uuid STRING, table_name STRING, updated_at TIMESTAMP, scan_id STRING, scanned_ms BIGINT,
    metadata_location STRING, facts_json STRING, trend_json STRING"""
PARTITION_FACTS_DDL = """
    table_uuid STRING, table_name STRING, partition_key STRING, scanned_ms BIGINT, facts_json STRING"""
ACTION_STATE_DDL = """
    table_uuid STRING, table_name STRING, updated_at TIMESTAMP, last_action_ms BIGINT, actions_json STRING,
    last_ok_json STRING"""

TREND_COLS = ("excess_files_total", "delete_files", "data_manifests")
TREND_KEEP = 30
RECENT_ACTIONS = 20
PART_SKIP = {"scan_id", "scanned_at", "table_name"}      # carried by the row itself


def ensure_tables(spark, ops, store=None):
    """The Iceberg tables of these kinds (nothing to do for another backend)."""
    import gl_common as gl
    if store is not None and not getattr(store, "iceberg", True):
        return
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ops}")
    for name, ddl in (("table_state", TABLE_STATE_DDL), ("partition_facts", PARTITION_FACTS_DDL),
                      ("action_state", ACTION_STATE_DDL)):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {ops}.{name} ({ddl}) USING iceberg")
        gl.ensure_columns(spark, f"{ops}.{name}", ddl)


def to_ms(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, str):
        from state_store import parse_ts
        v = parse_ts(v)
    if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return int(v.timestamp() * 1000)


def from_ms(ms):
    """Naive UTC datetime, as Spark returns TIMESTAMP values in a UTC session."""
    return None if ms is None else datetime.fromtimestamp(ms / 1000, timezone.utc).replace(tzinfo=None)


def timestamp_cols(ddl):
    return {p.split()[0] for p in ddl.split(",") if len(p.split()) >= 2 and p.split()[1].upper() == "TIMESTAMP"}


def _dumps(d):
    def enc(v):
        if isinstance(v, datetime):
            return {"$ms": to_ms(v)}
        return str(v)
    return json.dumps(d, default=enc, sort_keys=True)


def _loads(s, ts_cols=()):
    d = json.loads(s or "{}")
    for k, v in list(d.items()):
        if isinstance(v, dict) and "$ms" in v:
            d[k] = from_ms(v["$ms"])
    return d


def _as_dict(r):
    return dict(r) if isinstance(r, dict) else r.asDict()


# --- table state -----------------------------------------------------------
def table_row(tm, prev_state, window_hours, now):
    """The table_state row for a scanned table (None for a failed one)."""
    if tm.get("load_error") or not tm.get("table_uuid") or not tm.get("metadata_location"):
        return None
    scanned_ms = to_ms(tm["scanned_at"])
    facts = {k: v for k, v in tm.items() if not str(k).startswith("_")}
    trend = json.loads((prev_state or {}).get("trend_json") or "[]")
    trend = [t for t in trend if t.get("ms") is not None and t["ms"] < scanned_ms]
    trend.append(dict({c: tm.get(c) for c in TREND_COLS}, ms=scanned_ms))
    since = scanned_ms - float(window_hours) * 3600000
    trend = [t for t in trend if t["ms"] >= since][-TREND_KEEP:]
    return {"table_uuid": tm["table_uuid"], "table_name": tm["table_name"], "updated_at": now,
            "scan_id": tm["scan_id"], "scanned_ms": scanned_ms, "metadata_location": tm["metadata_location"],
            "facts_json": _dumps(facts), "trend_json": json.dumps(trend)}


def previous(state_row, now_ms):
    """A table_state row -> the previous-scan dict the scan used to read from the
    log: the facts, plus elapsed_min, orphan_age_h and retained_age_h."""
    p = _loads(state_row.get("facts_json"))
    scanned_ms = int(state_row["scanned_ms"])
    p.update(scan_id=state_row.get("scan_id"), metadata_location=state_row.get("metadata_location"),
             elapsed_min=(now_ms - scanned_ms) / 60000.0, _scanned_ms=scanned_ms,
             _uuid=state_row.get("table_uuid"))
    for key, col in (("orphan_age_h", "orphan_scanned_at"), ("retained_age_h", "retained_scanned_at")):
        ms = to_ms(p.get(col))
        p[key] = None if ms is None else (now_ms - ms) / 3600000.0
    return p


def load_previous(store, namespace, now_ms, window_hours=None):
    """{table name: previous-scan dict} for the namespace's tables in table_state.
    A table last scanned longer than window_hours ago has none (measured in full)."""
    out = {}
    since = None if window_hours is None else now_ms - float(window_hours) * 3600000
    for key, row in store.items("table_state"):
        name = row.get("table_name") or ""
        if not name.startswith(namespace + "."):
            continue
        if since is not None and int(row.get("scanned_ms") or 0) < since:
            continue
        out[name] = previous(row, now_ms)
    return out


def latest_facts(store, now_ms=None, window_hours=None):
    """{table name: facts dict} for every table in table_state (the plan's input),
    optionally only those scanned within window_hours."""
    out = {}
    since = None if window_hours is None else (now_ms or 0) - float(window_hours) * 3600000
    for key, row in store.items("table_state"):
        if since is not None and int(row.get("scanned_ms") or 0) < since:
            continue
        out[row["table_name"]] = dict(_loads(row.get("facts_json")), scan_id=row.get("scan_id"),
                                      scanned_ms=row.get("scanned_ms"))
    return out


def trend(state_row, upto_ms=None):
    """The trend rows detect's trend rules read (oldest first, up to and including
    the scan at upto_ms): table_uuid, scanned_at, scanned_ms and TREND_COLS."""
    out = []
    for t in json.loads((state_row or {}).get("trend_json") or "[]"):
        if upto_ms is not None and t["ms"] > upto_ms:
            continue
        out.append(dict({c: t.get(c) for c in TREND_COLS}, table_uuid=(state_row or {}).get("table_uuid"),
                        scanned_ms=t["ms"], scanned_at=from_ms(t["ms"])))
    return sorted(out, key=lambda r: r["scanned_ms"])


# --- partition facts ---------------------------------------------------------
def partition_rows(fact_rows, table_scanned_ms):
    """Stored partition facts -> partition rows as of the table's last scan
    (minutes_since_update moved on from each row's own scanned_ms)."""
    rows = []
    for f in fact_rows:
        r = _loads(f.get("facts_json"))
        r["partition_key"] = f["partition_key"]
        m = r.get("minutes_since_update")
        if m is not None and f.get("scanned_ms") is not None and table_scanned_ms is not None:
            r["minutes_since_update"] = float(m) + (int(table_scanned_ms) - int(f["scanned_ms"])) / 60000.0
        rows.append(r)
    return rows


def _same(a, b):
    """Two partition rows equal apart from the clock-driven minutes_since_update."""
    ka = {k: v for k, v in a.items() if k not in PART_SKIP and k != "minutes_since_update"}
    kb = {k: v for k, v in b.items() if k not in PART_SKIP and k != "minutes_since_update"}
    if _dumps(ka) != _dumps(kb):
        return False
    ma, mb = a.get("minutes_since_update"), b.get("minutes_since_update")
    if ma is None or mb is None:
        return ma is None and mb is None
    return abs(float(ma) - float(mb)) < 1.0          # same last write, aged to the same moment


def partition_changes(uuid, table, new_rows, prev_rows_at_scan, scanned_ms):
    """-> (puts, deleted keys). new_rows: this scan's rows; prev_rows_at_scan: the
    stored facts aged to this scan (partition_rows(stored, scanned_ms))."""
    prev = {r["partition_key"]: r for r in prev_rows_at_scan}
    puts, seen = [], set()
    for r in new_rows:
        r = _as_dict(r)
        key = r["partition_key"]
        seen.add(key)
        if key in prev and _same(prev[key], r):
            continue
        facts = {k: v for k, v in r.items() if k not in PART_SKIP and k != "partition_key"}
        puts.append({"table_uuid": uuid, "table_name": table, "partition_key": key,
                     "scanned_ms": int(scanned_ms), "facts_json": _dumps(facts)})
    gone = [(uuid, k) for k in prev if k not in seen]
    return puts, gone


# --- action state ------------------------------------------------------------
def action_row(prev_state, uuid, table, actions, now, window_hours=168):
    """action_state after recording `actions` (dicts with started_at, kind, status,
    result_json): the latest time and the recent ones."""
    recent = json.loads((prev_state or {}).get("actions_json") or "[]")
    for a in actions:
        recent.append({"ms": to_ms(a["started_at"]), "kind": a.get("kind"), "status": a.get("status"),
                       "result_json": (a.get("result_json") or "")[:2000]})
    recent.sort(key=lambda a: a["ms"])
    since = to_ms(now) - float(window_hours) * 3600000
    recent = [a for a in recent if a["ms"] >= since][-RECENT_ACTIONS:]
    last = max([a["ms"] for a in recent] + [int((prev_state or {}).get("last_action_ms") or 0)])
    last_ok = json.loads((prev_state or {}).get("last_ok_json") or "null")
    # "fixed": per symptom, the latest successful action that addressed it, kept
    # for good (not only the recent window), so the scorecard can tell a fix for a
    # table's own problem from housekeeping. "complete" = built from the whole
    # history (a row written before this field has only what came after it, until
    # backfill_fixed fills it from the actions log).
    fixed = dict((last_ok or {}).get("fixed") or {})
    complete = bool((last_ok or {}).get("complete")) if last_ok else True
    for a in actions:
        if a.get("status") != "ok":
            continue
        ms = to_ms(a["started_at"])
        if last_ok is None or ms >= last_ok["ms"]:
            last_ok = {"ms": ms, "kind": a.get("kind")}
        for sym in addressed(a):
            if sym not in fixed or ms >= fixed[sym][0]:
                fixed[sym] = [ms, a.get("kind")]
    if last_ok is not None:
        last_ok = dict(last_ok, fixed=fixed, complete=complete)
    return {"table_uuid": uuid, "table_name": table, "updated_at": now, "last_action_ms": last or None,
            "actions_json": json.dumps(recent), "last_ok_json": json.dumps(last_ok) if last_ok else None}


NOT_A_FIX = {"FREED_FILES"}      # housekeeping steps: they fix nothing a table was built with


def addressed(action):
    """The symptoms an action addressed (its symptoms column), housekeeping left out."""
    raw = action.get("symptoms") or ""
    syms = raw if isinstance(raw, (list, tuple)) else str(raw).split(",")
    return sorted({x.strip() for x in syms if x and x.strip() and x.strip() not in NOT_A_FIX})


def fixes(state_row):
    """(per-symptom {symptom: (ms, kind)}, complete) from action_state; ({}, True) when no action."""
    v = json.loads((state_row or {}).get("last_ok_json") or "null")
    if not v:
        return {}, True
    return {k: (int(x[0]), x[1]) for k, x in (v.get("fixed") or {}).items()}, bool(v.get("complete"))


def backfill_fixed(store, log):
    """Fill the per-symptom fix history of action_state rows written before it
    existed, from the actions log, once per row. -> rows filled."""
    todo = {k[0]: r for k, r in store.items("action_state") if r.get("last_ok_json") and not fixes(r)[1]}
    if not todo:
        return 0
    try:
        import plan          # noqa: F401  declares the actions log kind
        rows = log.rows("actions")
    except Exception as e:
        print(f"  (fix history not filled: {type(e).__name__}: {str(e)[:120]})", flush=True)
        return 0
    hist = {}
    for a in sorted((a for a in rows if a.get("table_uuid") in todo and a.get("started_at")),
                    key=lambda a: to_ms(a["started_at"])):
        if a.get("status") != "ok":
            continue
        for sym in addressed(a):
            hist.setdefault(a["table_uuid"], {})[sym] = [to_ms(a["started_at"]), a.get("kind")]
    for uuid, r in todo.items():
        v = json.loads(r["last_ok_json"])
        fixed = dict(hist.get(uuid, {}))
        for sym, x in (v.get("fixed") or {}).items():       # anything recorded since is newer
            if sym not in fixed or x[0] >= fixed[sym][0]:
                fixed[sym] = x
        store.put("action_state", dict(r, last_ok_json=json.dumps(dict(v, fixed=fixed, complete=True))))
    store.flush()
    print(f"  action_state: fix history filled from the actions log for {len(todo)} tables", flush=True)
    return len(todo)


def record_actions(store, records, now):
    """Fold action records (the rows logged to actions) into action_state, one
    row per table. Records without a table UUID are skipped."""
    by_uuid = {}
    for r in records:
        if r.get("table_uuid"):
            by_uuid.setdefault(r["table_uuid"], []).append(r)
    for uuid, acts in by_uuid.items():
        store.put("action_state", action_row(store.get("action_state", (uuid,)), uuid,
                                             acts[-1].get("table_name"), acts, now))
    return len(by_uuid)


def last_ok(state_row):
    """(kind, ms) of the table's latest successful action, or None."""
    v = json.loads((state_row or {}).get("last_ok_json") or "null")
    return (v["kind"], v["ms"]) if v else None


def recent_actions(state_row):
    """The action rows detect's rules read: started_at, kind, status, result_json."""
    return [{"started_at": from_ms(a["ms"]), "kind": a.get("kind"), "status": a.get("status"),
             "result_json": a.get("result_json")}
            for a in json.loads((state_row or {}).get("actions_json") or "[]")]


def seed_actions(store, log, now=None):
    """Fill an empty action_state from the actions log, once: actions taken before
    action_state existed (or before a backend switch) would otherwise be missing,
    so the scorecard would score fixed tables as freshly built. -> tables seeded."""
    if store.items("action_state"):
        return 0
    try:
        import plan          # noqa: F401  declares the actions log kind
        rows = log.rows("actions")
    except Exception as e:
        print(f"  (action history not seeded: {type(e).__name__}: {str(e)[:120]})", flush=True)
        return 0
    rows = [r for r in rows if r.get("table_uuid") and r.get("started_at")]
    if not rows:
        return 0
    rows.sort(key=lambda r: to_ms(r["started_at"]))
    n = record_actions(store, rows, now or datetime.now(timezone.utc))
    store.flush()
    print(f"  action_state seeded from the actions log: {len(rows)} actions on {n} tables", flush=True)
    return n


def action_ages(store, now_ms):
    """{table uuid: minutes since the latest advisor action}."""
    return {k[0]: (now_ms - int(r["last_action_ms"])) / 60000.0
            for k, r in store.items("action_state") if r.get("last_action_ms")}
