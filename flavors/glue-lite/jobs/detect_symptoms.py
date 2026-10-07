"""GL2.5c: apply the symptom rules to a metric scan and record the findings.

Reads one scan (default: the latest) from glue.ops.table_metrics and
glue.ops.partition_metrics, runs symptom_rules.evaluate() per table with that
table's config, appends the findings to glue.ops.symptoms under the same
scan_id, and prints them ranked.

Usage (via scripts/run-job.sh py detect_symptoms.py ...):
  detect_symptoms.py [--scan-id scan-...] [--config /opt/jobs/config/health.json]
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession

import gl_common as gl
import gltrace as tr
import holds as hl
import probes
import state_store as ss
import symptom_rules
import windows as win
from ledger import CHECK_DDL, mode_of

SYMPTOMS_DDL = """
    scan_id STRING, detected_at TIMESTAMP, table_name STRING, partition_key STRING,
    symptom STRING, category STRING, level STRING, action STRING, automatic BOOLEAN,
    score DOUBLE, severity STRING, remedy STRING, evidence_level STRING,
    evidence_json STRING, rule_version STRING"""
ss.declare_log("symptoms", SYMPTOMS_DDL, "detected_at")

ACTION_ORDER = {"auto": 0, "defer": 1, "approval": 2, "advisory": 3, "needs-evidence": 4, "acknowledged": 5}


def coerce(value, data_type):
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


def latest_scan_id(spark):
    r = spark.sql(f"SELECT max_by(scan_id, scanned_at) AS s FROM {gl.OPS_NAMESPACE}.table_metrics").collect()
    return r[0].s if r else None


def load_history(spark, tms, scan_id, window_hours=168):
    """Per table UUID: earlier scans (oldest first, up to the current one) and actions,
    from the last window_hours only (trend rules use the last few scans; the logs
    grow every run, so the read is bounded).
    Bounded by the scan's own timestamp in SQL: a Python datetime written back as a
    literal would shift by the driver's local zone."""
    uuids = sorted({t["table_uuid"] for t in tms if t.get("table_uuid")})
    history, actions = {}, {}
    if not uuids:
        return history, actions
    ids = ", ".join(f"'{u}'" for u in uuids)
    cols = {f.name for f in spark.table(f"{gl.OPS_NAMESPACE}.table_metrics").schema}
    want = [c for c in ("excess_files_total", "delete_files", "data_manifests") if c in cols]
    for r in spark.sql(f"""
            SELECT table_uuid, scanned_at, unix_millis(scanned_at) AS scanned_ms, {', '.join(want)}
            FROM {gl.OPS_NAMESPACE}.table_metrics
            WHERE table_uuid IN ({ids}) AND scanned_at <= (
                SELECT max(scanned_at) FROM {gl.OPS_NAMESPACE}.table_metrics WHERE scan_id = '{scan_id}')
              AND scanned_at > current_timestamp() - INTERVAL {int(window_hours)} HOURS
            ORDER BY scanned_at""").collect():
        history.setdefault(r.table_uuid, []).append(r.asDict())
    if spark.catalog.tableExists(f"{gl.OPS_NAMESPACE}.actions"):
        for r in spark.sql(f"""
                SELECT table_uuid, started_at, kind, status, result_json
                FROM {gl.OPS_NAMESPACE}.actions WHERE table_uuid IN ({ids})
                  AND started_at > current_timestamp() - INTERVAL {int(window_hours)} HOURS
                ORDER BY started_at""").collect():
            actions.setdefault(r.table_uuid, []).append(r.asDict())
    return history, actions


def _uuids(tms):
    return sorted({t["table_uuid"] for t in tms if t.get("table_uuid")})


def load_partition_state(store, tms):
    """Family 2's per-partition state, by table UUID (empty when family 2 never ran)."""
    out = {}
    for u in _uuids(tms):
        for r in store.range("partition_state", (u,)):
            out.setdefault(u, {})[r["partition_key"]] = {"last_write_ms": r.get("last_write_ms"),
                                                        "last_write_label": r.get("last_write_label")}
    return out


def _mb(n):
    return "n/a" if n is None else f"{n / 1048576:.1f} MiB" if n >= 104858 else f"{n / 1024:.0f} KiB"


def load_compactions(store, tms, since_ms=None):
    """Compaction times per partition (family 2's activity rows), by table UUID.
    since_ms: only compactions a rule can still count (the settle window)."""
    out = {}
    for u in _uuids(tms):
        for r in store.range("commit_partition", (u,), start=since_ms):
            if r.get("operation") == "replace" and (r.get("data_files_added") or 0) > 0:
                out.setdefault(u, {}).setdefault(r["partition_key"], []).append(int(r["ts_ms"]))
    for parts in out.values():
        for pk in parts:
            parts[pk].sort()
    return out


def load_late_hists(store, tms, default_days=30):
    """Lateness bucket counts by table UUID, over each table's own lookback
    (lateness_lookback_days from the scan: adaptive, see windows.py)."""
    days = {t.get("table_uuid"): int(t.get("lateness_lookback_days") or default_days) for t in tms}
    today = datetime.now(timezone.utc).date()
    out = {}
    for u in _uuids(tms):
        for r in store.range("late_hist", (u,), start=today - timedelta(days=max(days.values()))):
            if (today - r["day"]).days >= days.get(u, default_days):
                continue
            t = out.setdefault(u, {})
            t[r["bucket_max_h"]] = t.get(r["bucket_max_h"], 0) + int(r["batches"] or 0)
    return out


def load_holds_activity(store, tms, scan_ms, window_h, hot_cap_min):
    """Family 2's activity summed per partition for the revised holds (holds.py),
    by table UUID. Reads back max(churn window, hot cap) from the scan time."""
    churn_since = scan_ms - int(window_h * 3600000)
    since = min(churn_since, scan_ms - int(hot_cap_min * 60000))
    out = {}
    for u in _uuids(tms):
        a = hl.activity_from_rows(store.range("commit_partition", (u,), start=since), since, churn_since)
        if a:
            out[u] = a
    return out


CHECK_COLS = ["scan_id", "checked_at", "table_name", "table_uuid", "family", "metric", "full_value",
              "ledger_value", "agree", "note"]


def record_holds_changes(log, scan_id, at, mode, changes, report=True,
                         family="partition_holds", title="Partition holds", baseline="today's holds"):
    """What a variant of the rules changes, per table (log incremental_check;
    agree = no change). Families: partition_holds (GL2.6b), new_findings (GL2.6c),
    expiry_policy (GL2.6e)."""
    log.append("incremental_check", [dict(zip(CHECK_COLS, [
        scan_id, at, tm["table_name"], tm.get("table_uuid"), family, "findings",
        baseline, note, not d, "; ".join(d)[:2000]])) for tm, d, note in changes])
    changed = [c for c in changes if c[1]]
    print(f"\n=== {title} ({mode}): {len(changed)}/{len(changes)} tables would change"
          f"{'' if mode == 'shadow' else ' (applied)'} ===", flush=True)
    if report:
        for tm, d, note in changed:
            print(f"  {tm['table_name'].rsplit('.', 1)[-1]:26} {note}", flush=True)
            for line in d[:8]:
                print(f"  {'':26}   {line}", flush=True)


def record_window_changes(log, scan_id, at, mode, changes, report=True):
    """What the learned windows change, per table (log incremental_check,
    family learned_windows; agree = no change)."""
    rows = []
    for tm, d in changes:
        windows = (f"hot {tm.get('hot_window_min')} min ({tm.get('hot_window_source')}); "
                   f"settle {tm.get('settle_window_h')} h ({tm.get('settle_window_source')})")
        rows.append(dict(zip(CHECK_COLS, [scan_id, at, tm["table_name"], tm.get("table_uuid"), "learned_windows",
                                          "findings", "configured windows", windows, not d, "; ".join(d)[:2000]])))
    log.append("incremental_check", rows)
    changed = [(tm, d) for tm, d in changes if d]
    print(f"\n=== Learned windows ({mode}): {len(changed)}/{len(changes)} tables would change"
          f"{'' if mode == 'shadow' else ' (applied)'} ===", flush=True)
    if report:
        for tm, d in changes:
            if not d and tm.get("hot_window_source", "").startswith("config"):
                continue
            name = tm["table_name"].rsplit(".", 1)[-1]
            idle = f", {tm.get('idle_gaps_ignored')} idle gaps ignored" if tm.get("idle_gaps_ignored") else ""
            print(f"  {name:26} hot {tm.get('hot_window_min'):g} min [{tm.get('hot_window_source')}; "
                  f"{tm.get('hot_gaps_used')} gaps / {tm.get('gap_lookback_days')} d{idle}], "
                  f"settle {tm.get('settle_window_h')} h [{tm.get('settle_window_source')}; "
                  f"{tm.get('lateness_p99_batches')} batches / {tm.get('lateness_lookback_days')} d]", flush=True)
            for line in d[:8]:
                print(f"  {'':26}   {line}", flush=True)


def run_detect(spark, scan_id, config, report=True):
    """Apply the rules to one scan, append to glue.ops.symptoms, return the findings."""
    tm_table = f"{gl.OPS_NAMESPACE}.table_metrics"
    pm_table = f"{gl.OPS_NAMESPACE}.partition_metrics"
    sy_table = f"{gl.OPS_NAMESPACE}.symptoms"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {sy_table} ({SYMPTOMS_DDL}) USING iceberg")

    tms = [r.asDict() for r in spark.sql(f"SELECT *, unix_millis(scanned_at) AS scanned_ms FROM {tm_table} "
                                         f"WHERE scan_id = '{scan_id}'").collect()]
    parts = {}
    for r in spark.sql(f"SELECT * FROM {pm_table} WHERE scan_id = '{scan_id}'").collect():
        parts.setdefault(r.table_name, []).append(r.asDict())

    history, actions = load_history(spark, tms, scan_id, float(
        (config.get("incremental") or {}).get("previous_scan_window_hours", 168)))
    detected_at = probes.now_utc()
    findings = []
    store = ss.IcebergStateStore(spark, gl.OPS_NAMESPACE)
    log = ss.make_log_sink(spark, config)
    mode3 = mode_of(config, "learned_windows")
    mode4 = mode_of(config, "partition_holds")
    inc = config.get("incremental") or {}
    scan_ms = max((t["scanned_ms"] for t in tms), default=0)
    # activity is read back only as far as a rule looks: the settle window (compactions
    # since a partition ended), the churn window and the hot cap (holds)
    act_from = scan_ms - int(max(float(inc.get("settle_cap_hours", 168)) * 3600000,
                                 float(inc.get("churn_window_h", 24)) * 3600000,
                                 float(inc.get("hot_cap_minutes", 1440)) * 60000))
    if mode3 != "off" or mode4 != "off":       # one read per kind, then per-table lookups in memory
        uu = _uuids(tms)
        cap = int(inc.get("store_preload_max_rows", 2000000))
        store.preload("partition_state", uuids=uu, max_rows=cap)
        store.preload("commit_partition", start=act_from, uuids=uu, max_rows=cap)
        if mode3 != "off":
            days = max([int(t.get("lateness_lookback_days") or 30) for t in tms] or [30])
            store.preload("late_hist", start=datetime.now(timezone.utc).date() - timedelta(days=days),
                          uuids=uu, max_rows=cap)
    pstate = load_partition_state(store, tms) if mode3 != "off" else {}
    compactions, late_hists = ((load_compactions(store, tms, act_from), load_late_hists(store, tms))
                               if mode3 != "off" else ({}, {}))
    changes = []
    if mode4 != "off" and not pstate:
        pstate = load_partition_state(store, tms)
    hold_act = (load_holds_activity(store, tms, max((t["scanned_ms"] for t in tms), default=0),
                                    float(inc.get("churn_window_h", 24)), float(inc.get("hot_cap_minutes", 1440)))
                if mode4 != "off" and tms else {})
    hold_changes = []
    mode5 = mode_of(config, "new_findings")
    new_changes = []
    mode6 = mode_of(config, "expiry_policy")
    exp_changes = []
    for tm in sorted(tms, key=lambda t: t["table_name"]):
        table, uuid = tm["table_name"], tm.get("table_uuid")
        cfg = gl.table_config(config, table)
        tr.begin(table)
        tr.log("detect", "inputs", partitions=len(parts.get(table, [])), history_scans=len(history.get(uuid, [])),
               actions=len(actions.get(uuid, [])), hot_partition_minutes=cfg.get("hot_partition_minutes"),
               families=f"learned_windows={mode3} partition_holds={mode4} new_findings={mode5} "
                        f"expiry_policy={mode6}")
        tr.variant("today")
        current = symptom_rules.evaluate(table, tm, parts.get(table, []), cfg,
                                         history=history.get(uuid, []), actions=actions.get(uuid, []))
        mine, rows_used = current, parts.get(table, [])
        if mode3 != "off" and uuid in pstate and tm.get("hot_window_min") is not None:
            # GL2.5o: the same rules with learned windows and the ledger's per-partition age
            inc = config.get("incremental") or {}
            cfg2 = dict(cfg, hot_partition_minutes=tm["hot_window_min"], settle_hours=tm.get("settle_window_h"),
                        learned_windows=True, min_batches=int(inc.get("min_batches", inc.get("min_samples", 20))))
            rows2 = win.learned_rows(parts.get(table, []), pstate[uuid],
                                     json.loads(tm.get("partition_fields_json") or "[]"), tm["scanned_ms"],
                                     compactions.get(uuid, {}), late_hists.get(uuid, {}))
            tr.variant("learned_windows")
            tr.log("detect", "learned windows", hot_window_min=tm["hot_window_min"],
                   settle_window_h=tm.get("settle_window_h"))
            tr.rows("detect.learned_rows", rows2, ["partition_key", "minutes_since_update", "hours_since_end",
                                                   "compactions_since_end", "p_more_late", "last_write_label"])
            learned = symptom_rules.evaluate(table, tm, rows2, cfg2,
                                             history=history.get(uuid, []), actions=actions.get(uuid, []))
            d = win.diff(current, learned)
            tr.log("detect.diff", "learned_windows: " + ("; ".join(d) if d else "no change"))
            changes.append((tm, d))
            if mode3 == "on":
                mine, cfg, rows_used = learned, cfg2, rows2
        if mode4 != "off" and uuid in pstate:
            # GL2.6b: the same rules as the findings above, with the revised holds
            fields = json.loads(tm.get("partition_fields_json") or "[]")
            rows4 = hl.holds_rows(rows_used, pstate[uuid], hold_act.get(uuid, {}), fields, tm["scanned_ms"])
            tr.variant("partition_holds")
            tr.rows("detect.holds_rows", rows4, ["partition_key", "minutes_since_update", "hours_since_end",
                                                 "minutes_since_conflict", "removed_bytes_window",
                                                 "rewrite_commits_window", "rewrite_rate"])
            revised = symptom_rules.evaluate(table, tm, rows4, dict(cfg, revised_holds=True),
                                             history=history.get(uuid, []), actions=actions.get(uuid, []))
            d4 = hl.diff_actions(mine, revised)
            tr.log("detect.diff", "partition_holds: " + ("; ".join(d4) if d4 else "no change"))
            churny = [r for r in rows4 if (r.get("rewrite_rate") or 0) >= cfg["thresholds"].get("churn_partition_rate", 1.0)]
            note = (f"hot {float(cfg.get('hot_partition_minutes', 15)):g} min; "
                    f"{len(churny)} partition(s) at rewrite rate >= "
                    f"{cfg['thresholds'].get('churn_partition_rate', 1.0):g} / {inc.get('churn_window_h', 24)} h")
            hold_changes.append((tm, d4, note))
            if mode4 == "on":
                mine, cfg, rows_used = revised, dict(cfg, revised_holds=True), rows4
        if mode5 != "off":
            # GL2.6c: the same rules plus DELETE_FILE_SPRAWL, RETAINED_STORAGE, METADATA_BLOAT
            tr.variant("new_findings")
            added = symptom_rules.evaluate(table, tm, rows_used, dict(cfg, new_findings=True),
                                           history=history.get(uuid, []), actions=actions.get(uuid, []))
            d5 = hl.diff_actions(mine, added)
            tr.log("detect.diff", "new_findings: " + ("; ".join(d5) if d5 else "no change"))
            note = (f"old snapshots keep {_mb(tm.get('retained_bytes'))} (live {_mb(tm.get('data_bytes'))}); "
                    f"metadata.json {_mb(tm.get('metadata_json_bytes'))}; "
                    f"old manifests {_mb(tm.get('retained_metadata_bytes'))}")
            new_changes.append((tm, d5, note))
            if mode5 == "on":
                mine, cfg = added, dict(cfg, new_findings=True)
        if mode6 != "off" and tm.get("policy_age_h") is not None:
            # GL2.6e: SNAPSHOT_BUILDUP by retention policy (expiry.py), STALE_REF
            tr.variant("expiry_policy")
            pol = symptom_rules.evaluate(table, tm, rows_used, dict(cfg, expiry_policy=True),
                                         history=history.get(uuid, []), actions=actions.get(uuid, []))
            d6 = hl.diff_actions(mine, pol)
            tr.log("detect.diff", "expiry_policy: " + ("; ".join(d6) if d6 else "no change"))
            note = (f"{tm.get('write_category')}: {tm.get('policy_age_h'):g} h / keep {tm.get('policy_min_keep')} "
                    f"[{tm.get('policy_source')}]; {tm.get('snapshots')} snapshots, "
                    f"{tm.get('expirable_snapshots')} expirable; refs {tm.get('refs_json')}")
            exp_changes.append((tm, d6, note))
            if mode6 == "on":
                mine = pol
        tr.variant("")
        tr.log("detect", f"recorded: {len(mine)} finding(s) from "
               + ("today's rules" if mine is current else "the families switched on"),
               symptoms=sorted({f['symptom'] for f in mine}))
        tr.begin(None)
        findings += mine
    if mode3 != "off":
        record_window_changes(log, scan_id, detected_at, mode3, changes, report)
    if mode4 != "off":
        record_holds_changes(log, scan_id, detected_at, mode4, hold_changes, report)
    if mode5 != "off":
        record_holds_changes(log, scan_id, detected_at, mode5, new_changes, report,
                             family="new_findings", title="New findings", baseline="today's findings")
    if mode6 != "off":
        record_holds_changes(log, scan_id, detected_at, mode6, exp_changes, report,
                             family="expiry_policy", title="Expiry by policy", baseline="count/age rule")
    if log.pending("incremental_check"):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {gl.OPS_NAMESPACE}.incremental_check ({CHECK_DDL}) USING iceberg")

    print(f"=== Symptoms for scan {scan_id}: {len(findings)} findings over {len(tms)} tables ===",
          flush=True)
    for f in findings:
        f.update(scan_id=scan_id, detected_at=detected_at)
    log.append("symptoms", findings)
    log.flush()

    findings.sort(key=lambda f: (ACTION_ORDER.get(f["action"], 9), -f["score"]))
    print("\n=== Per table ===")
    for tm in sorted(tms, key=lambda t: t["table_name"]):
        mine = [f for f in findings if f["table_name"] == tm["table_name"]
                and f["action"] not in ("needs-evidence", "acknowledged")]
        held = [f for f in findings if f["table_name"] == tm["table_name"] and f["action"] == "needs-evidence"]
        names = {}
        for f in mine:
            names[f["symptom"]] = names.get(f["symptom"], 0) + 1
        label = ", ".join(f"{k}x{v}" if v > 1 else k for k, v in sorted(names.items())) or "healthy"
        extra = f"   (+{len(held)} held for workload evidence)" if held else ""
        print(f"  {tm['table_name']:40} {label}{extra}")

    if report:
        print("\n=== Ranked findings (top 40; held ones last) ===")
        for f in findings[:40]:
            where = f"  [{f['partition_key']}]" if f["partition_key"] else ""
            ev = json.loads(f["evidence_json"])
            brief = ", ".join(f"{k}={v}" for k, v in list(ev.items())[:4])
            print(f"  {f['action']:14} {f['severity']:6} {f['score']:7.2f}  {f['symptom']:22} "
                  f"{f['table_name']}{where}\n      {brief}\n      -> {f['remedy']}")
    print(f"Symptoms recorded in {sy_table} under scan_id {scan_id}", flush=True)
    return findings


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scan-id", default=None, help="default: the latest scan")
    p.add_argument("--config", default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                    "config", "health.json"))
    a = p.parse_args()

    spark = SparkSession.builder.appName("gl25-detect-symptoms").getOrCreate()
    scan_id = a.scan_id or latest_scan_id(spark)
    if not scan_id:
        sys.exit("No scan found: run make gl-metrics first.")
    run_detect(spark, scan_id, gl.load_config(a.config))
    spark.stop()


if __name__ == "__main__":
    main()
