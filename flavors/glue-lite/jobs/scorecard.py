"""GL2.5d: check a scan's findings against the expectation matrix.

Each scenario table in config/expectations.json lists the symptoms the engine
must find (with optional partition selectors). A table passes when every
expected symptom is found where expected and nothing else active is found.
Findings held for workload evidence, and advisory ones, are ignored. Tables not in the file are
listed but not scored.

STALE: a table marked "fresh" (s3) is only checkable while its newest
partition is younger than the hot window at scan time. Past that, the
partition has legitimately cooled, so the result is STALE with a hint to
rebuild, not FAIL.

Output (GL2.5l): grouped by phase. "detect" = no fix yet, PASS means the
problem the table was built with was found; "fixed" = a fix ran, PASS means
the table is now healthy. Fixed rows show the last successful action, detect
rows the next step.

Pure scoring lives in score_table() and printing in render(), so both are
unit-tested without Spark.
Usage (via scripts/run-job.sh py scorecard.py ...):
  scorecard.py [--scan-id scan-...]
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

SCORECARD_DDL = """
    scan_id STRING, scored_at TIMESTAMP, table_name STRING, status STRING,
    found STRING, missing STRING, unexpected STRING, notes STRING, phase STRING,
    last_fix STRING"""


def _declare_scorecard():
    import state_store as ss
    ss.declare_log("scorecard", SCORECARD_DDL, "scored_at")


_declare_scorecard()

HERE = os.path.dirname(os.path.abspath(__file__))


def partition_value(key):
    """'{"occurred_at_day":"2026-09-03"}' -> '2026-09-03' (multi-field: joined by '/')."""
    if not key:
        return None
    try:
        d = json.loads(key)
    except ValueError:
        return key
    if isinstance(d, dict):
        return "/".join(str(v) for v in d.values()) if d else None
    return str(d)


def _check(exp, hits, newest):
    """Problems with one expected symptom, given its findings (partition values)."""
    name = exp["symptom"]
    if not hits:
        return [] if exp.get("optional") else [f"{name} not found"]
    vals = sorted(v for v in hits if v is not None)
    problems = []
    if "partitions" in exp and vals != sorted(exp["partitions"]):
        problems.append(f"{name} on {vals}, expected {sorted(exp['partitions'])}")
    if exp.get("newest") and vals != [newest]:
        problems.append(f"{name} on {vals}, expected only the newest partition {newest}")
    if exp.get("not_newest") and newest in vals:
        problems.append(f"{name} on the newest partition {newest}")
    if "prefix" in exp:
        off = [v for v in vals if not v.startswith(exp["prefix"])]
        if off:
            problems.append(f"{name} on partitions not starting with {exp['prefix']!r}: {off}")
    if "min_value" in exp:
        low = [v for v in vals if v < exp["min_value"]]
        if low:
            problems.append(f"{name} on partitions older than {exp['min_value']}: {low}")
    if "count" in exp and len(hits) != exp["count"]:
        problems.append(f"{name} x{len(hits)}, expected x{exp['count']}")
    if "max_count" in exp and len(hits) > exp["max_count"]:
        problems.append(f"{name} x{len(hits)}, expected at most {exp['max_count']}")
    if "min_count" in exp and len(hits) < exp["min_count"]:
        problems.append(f"{name} x{len(hits)}, expected at least {exp['min_count']}")
    return problems


def score_table(table, expectation, findings, partition_rows, hot_minutes, phase="before", writer_age_min=None):
    """-> dict(status, found, missing, unexpected, notes, phase).

    phase "after" = plan.py has applied fixes to this table (same table UUID)
    since it was built; the expectation's "after" block is used if it has one.
    """
    active = [f for f in findings if f["action"] not in ("needs-evidence", "advisory", "acknowledged")]
    by_symptom = {}
    for f in active:
        by_symptom.setdefault(f["symptom"], []).append(partition_value(f.get("partition_key")))
    found = sorted(f"{k}x{len(v)}" if len(v) > 1 else k for k, v in by_symptom.items())

    values = {partition_value(r.get("partition_key")): r for r in partition_rows}
    newest = max((v for v in values if v is not None), default=None)

    if expectation is None:
        return {"status": "NOT SCORED", "found": found, "missing": [], "unexpected": [], "notes": "",
                "phase": phase}
    fresh = expectation.get("fresh")
    if phase == "after":
        if "after" not in expectation:
            return {"status": "NOT SCORED", "found": found, "missing": [], "unexpected": [],
                    "notes": "fixed by plan.py; no 'after' expectations yet", "phase": phase}
        expectation = expectation["after"]

    problems = []
    expected_names = set(expectation.get("allow", []))
    for exp in expectation.get("expect", []):
        expected_names.add(exp["symptom"])
        problems += _check(exp, by_symptom.get(exp["symptom"], []), newest)
    unexpected = sorted(s for s in by_symptom if s not in expected_names)

    status = "PASS" if not problems and not unexpected else "FAIL"
    notes = ""
    if fresh and phase == "before" and newest is not None:
        age = values[newest].get("minutes_since_update")
        if age is not None and age >= hot_minutes:
            notes = (f"newest partition {newest} was {age:.1f} min old at scan time "
                     f"(hot window {hot_minutes:g} min): rebuild it and scan straight after "
                     f"(make gl-scan FRESH_S3=1)")
            if status == "FAIL":
                status = "STALE"
    # Scenarios whose symptom only holds for a while after their last write
    # (s12: churn is measured over the last 24 h) are STALE, not FAIL, past that.
    stale_h = expectation.get("stale_after_hours")
    if (stale_h and phase == "before" and status == "FAIL" and writer_age_min is not None
            and writer_age_min / 60.0 >= float(stale_h)):
        status = "STALE"
        notes = (f"last write was {writer_age_min / 60.0:.1f} h ago; this scenario's symptom only holds for "
                 f"{stale_h:g} h after it: rebuild it (make gl-test-tables TT_ARGS=\"--only "
                 f"{table.rsplit('.', 1)[-1].split('_', 1)[0]}\") and scan")
    return {"status": status, "found": found, "missing": problems,
            "unexpected": unexpected, "notes": notes, "phase": phase}


PHASE_LABEL = {"before": "detect", "after": "fixed"}


def next_step(table, findings, mode="auto"):
    """What fixing a detect-phase table would take, from its findings' action types."""
    short = table.rsplit(".", 1)[-1].split("_", 1)[0]
    acts = {}
    for f in findings:
        acts.setdefault(f["action"], set()).add(f["symptom"])
    parts = []
    if acts.get("auto"):
        parts.append(f"auto fix (make gl-plan T={short} APPLY=1)" if mode == "auto" else
                     f"needs approval, advisor.mode={mode} "
                     f"(APPROVE={','.join(sorted(acts['auto']))})")
    if acts.get("approval"):
        parts.append(f"needs approval (APPROVE={','.join(sorted(acts['approval']))})")
    if acts.get("defer"):
        parts.append("deferred while the writer is active")
    return "; ".join(parts)


def outcome(r):
    """The OUTCOME column: what was checked, in words."""
    def nice(x):
        base, sep, n = x.rpartition("x")
        return f"{base} x{n}" if sep and n.isdigit() and base else x
    found = ", ".join(nice(x) for x in r["found"])
    if r["status"] == "NOT SCORED":
        return found or "healthy"
    if r["status"] == "STALE":
        return "too old to judge (see note): rebuild and scan straight after"
    if r["phase"] == "after":
        if r["status"] == "PASS":
            return f"healthy after fix{', allowed: ' + found if found else ''}"
        return f"fix ran, but still found: {found}" if found else "fix ran, expectations not met"
    if r["status"] == "PASS":
        return f"found as built: {found}" if found else "healthy, as built"
    return f"detection off: found {found or 'nothing'}"


def render(scan_id, results):
    """Scorecard lines, grouped by phase (pure, unit-tested)."""
    scored = [r for r in results if r["status"] != "NOT SCORED"]
    passed = [r for r in scored if r["status"] == "PASS"]
    det = sum(1 for r in passed if r["phase"] == "before")
    fixd = sum(1 for r in passed if r["phase"] == "after")
    stale = sum(1 for r in scored if r["status"] == "STALE")
    other = len(scored) - len(passed) - stale
    parts = [f"{det} detected as built", f"{fixd} fixed and verified"]
    if stale:
        parts.append(f"{stale} stale")
    if other:
        parts.append(f"{other} failed")
    out = [f"\n=== Scorecard for scan {scan_id} ===",
           f"{len(passed)}/{len(scored)} pass: " + ", ".join(parts), "",
           f"{'RESULT':7} {'PHASE':7} {'TABLE':26} OUTCOME"]

    def row(r):
        name = r["table_name"].rsplit(".", 1)[-1]
        out.append(f"{r['status']:7} {PHASE_LABEL.get(r['phase'], r['phase']):7} {name:26} {outcome(r)}")
        pad = " " * 44
        if r["phase"] == "after" and r.get("last_fix"):
            out.append(f"{pad}last fix: {r['last_fix']}")
        for m in ([] if r["status"] == "STALE" else r["missing"]):   # STALE: the note says why
            out.append(f"{pad}missing: {m}")
        if r["unexpected"] and r["phase"] == "before":
            out.append(f"{pad}unexpected: {', '.join(r['unexpected'])}")
        if r["phase"] == "after" and r["status"] == "FAIL" and r.get("next"):
            out.append(f"{pad}remaining fix: {r['next']}")
        if r["phase"] == "before" and r["status"] == "PASS" and r.get("next"):
            out.append(f"{pad}next: {r['next']}")
        if r["notes"]:
            out.append(f"{pad}note: {r['notes']}")

    groups = [
        ("Fixed: a fix ran; the scan must now find the table healthy",
         [r for r in scored if r["phase"] == "after" and r["status"] in ("PASS", "FAIL")]),
        ("Detect: no fix yet; the scan must find the problem the table was built with",
         [r for r in scored if r["phase"] == "before" and r["status"] in ("PASS", "FAIL")]),
        ("Not judged", [r for r in scored if r["status"] not in ("PASS", "FAIL")]),
    ]
    for title, rows in groups:
        if not rows:
            continue
        out.append(f"-- {title} " + "-" * max(0, 84 - len(title)))
        for r in sorted(rows, key=lambda r: (r["status"] == "PASS", r["table_name"])):   # failures first
            row(r)
        out.append("")
    unscored = [r["table_name"].rsplit(".", 1)[-1] for r in results if r["status"] == "NOT SCORED"]
    if unscored:
        out.append(f"{'--':16}{', '.join(unscored)}: not test tables, not scored")
    return out


def run_scorecard(spark, scan_id, config, expectations, partial=False):
    import gl_common as gl
    import gltrace as tr
    import probes

    import scan_metrics as sm
    import scan_state
    import state_store as ss
    from detect_symptoms import FINDINGS
    # the scan and its findings: from this job's memory when they ran here, else read back
    store = ss.make_state_store(spark, config)
    log = ss.make_log_sink(spark, config)
    tms, scan_parts = sm.scan_results(spark, scan_id, store, config, log)
    scan_state.seed_actions(store, log)          # once, when action_state is empty
    tmrows = {t["table_name"]: t for t in tms}
    tables = sorted(tmrows)
    last_fix = {}   # table UUID -> latest successful action ("kind ok HH:MM UTC"), from action_state
    uu = sorted({t.get("table_uuid") for t in tms if t.get("table_uuid")})
    if uu:
        scan_state.ensure_tables(spark, gl.OPS_NAMESPACE, store)
        store.preload("action_state", uuids=uu)
        for u in uu:
            ok = scan_state.last_ok(store.get("action_state", (u,)))
            if ok:
                last_fix[u] = f"{ok[0]} ok {scan_state.from_ms(ok[1]).strftime('%Y-%m-%d %H:%M')} UTC"
    fixed = set(last_fix)
    findings = {}
    rows = FINDINGS.get(scan_id)
    if rows is None:
        rows = log.rows("symptoms", eq={"scan_id": scan_id})
    for r in rows:
        findings.setdefault(r["table_name"], []).append(r)
    parts = {t: [{"table_name": t, "partition_key": r.get("partition_key"),
                  "minutes_since_update": r.get("minutes_since_update")} for r in rows]
             for t, rows in scan_parts.items()}

    exp_tables = expectations.get("tables", {})
    results = []
    for t in tables:
        cfg = gl.table_config(config, t)
        phase = "after" if tmrows[t].get("table_uuid") in fixed else "before"
        res = score_table(t, exp_tables.get(t), findings.get(t, []), parts.get(t, []),
                          float(cfg.get("hot_partition_minutes", 15)), phase,
                          writer_age_min=tmrows[t].get("minutes_since_writer_commit"))
        res["table_name"] = t
        tr.begin(t)
        tr.log("score", f"{res['status']} ({phase})", found=res.get("found"), missing=res.get("missing"),
               unexpected=res.get("unexpected"), notes=res.get("notes"))
        tr.begin(None)
        res["last_fix"] = last_fix.get(tmrows[t].get("table_uuid")) if phase == "after" else None
        props = json.loads(tmrows[t].get("properties_json") or "{}")
        mode = props.get("advisor.mode") or cfg.get("advisor_mode", "auto")
        res["next"] = next_step(t, [f for f in findings.get(t, [])
                                    if f["action"] not in ("needs-evidence", "advisory")], mode)
        results.append(res)
    for t in ([] if partial else sorted(set(exp_tables) - set(tables))):    # a --tables scan covers only some
        results.append({"table_name": t, "status": "MISSING TABLE", "found": [], "missing": [],
                        "unexpected": [], "notes": "not in this scan: run make gl-test-tables",
                        "phase": "before", "last_fix": None, "next": ""})

    for line in render(scan_id, results):
        print(line)

    scored_at = probes.now_utc()
    log.append("scorecard", [{"scan_id": scan_id, "scored_at": scored_at, "table_name": r["table_name"],
                              "status": r["status"], "found": ", ".join(r["found"]),
                              "missing": "; ".join(r["missing"]), "unexpected": ", ".join(r["unexpected"]),
                              "notes": r["notes"], "phase": r["phase"], "last_fix": r.get("last_fix")}
                             for r in results])
    log.flush()
    return results


def main():
    import gl_common as gl
    from detect_symptoms import latest_scan_id
    from pyspark.sql import SparkSession

    p = argparse.ArgumentParser()
    p.add_argument("--scan-id", default=None, help="default: the latest scan")
    p.add_argument("--config", default=os.path.join(HERE, "config", "health.json"))
    p.add_argument("--expectations", default=os.path.join(HERE, "config", "expectations.json"))
    a = p.parse_args()
    spark = SparkSession.builder.appName("gl25-scorecard").getOrCreate()
    config = gl.load_config(a.config)
    scan_id = a.scan_id or latest_scan_id(spark, config)
    if not scan_id:
        sys.exit("No scan found: run make gl-scan first.")
    run_scorecard(spark, scan_id, config, gl.load_config(a.expectations))
    spark.stop()


if __name__ == "__main__":
    main()
