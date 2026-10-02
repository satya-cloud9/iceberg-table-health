"""GL2.5d: check a scan's findings against the expectation matrix.

Each scenario table in config/expectations.json lists the symptoms the engine
must find (with optional partition selectors). A table passes when every
expected symptom is found where expected and nothing else active is found.
Findings held for workload evidence are ignored. Tables not in the file are
listed but not scored.

STALE: a table marked "fresh" (s3) is only checkable while its newest
partition is younger than the hot window at scan time. Past that, the
partition has legitimately cooled, so the result is STALE with a hint to
rebuild, not FAIL.

Pure scoring lives in score_table() so it is unit-tested without Spark.
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
    found STRING, missing STRING, unexpected STRING, notes STRING, phase STRING"""

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
    if "min_count" in exp and len(hits) < exp["min_count"]:
        problems.append(f"{name} x{len(hits)}, expected at least {exp['min_count']}")
    return problems


def score_table(table, expectation, findings, partition_rows, hot_minutes, phase="before"):
    """-> dict(status, found, missing, unexpected, notes, phase).

    phase "after" = plan.py has applied fixes to this table (same table UUID)
    since it was built; the expectation's "after" block is used if it has one.
    """
    active = [f for f in findings if f["action"] != "needs-evidence"]
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
    return {"status": status, "found": found, "missing": problems,
            "unexpected": unexpected, "notes": notes, "phase": phase}


def run_scorecard(spark, scan_id, config, expectations):
    import gl_common as gl
    import probes

    sy = f"{gl.OPS_NAMESPACE}.symptoms"
    pm = f"{gl.OPS_NAMESPACE}.partition_metrics"
    tm = f"{gl.OPS_NAMESPACE}.table_metrics"
    sc = f"{gl.OPS_NAMESPACE}.scorecard"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {sc} ({SCORECARD_DDL}) USING iceberg")
    gl.ensure_columns(spark, sc, SCORECARD_DDL)

    tmrows = {r.table_name: r.asDict() for r in
              spark.sql(f"SELECT * FROM {tm} WHERE scan_id = '{scan_id}'").collect()}
    tables = sorted(tmrows)
    fixed = set()   # table UUIDs plan.py has applied fixes to
    if spark.catalog.tableExists(f"{gl.OPS_NAMESPACE}.actions"):
        fixed = {r.table_uuid for r in spark.sql(
            f"SELECT DISTINCT table_uuid FROM {gl.OPS_NAMESPACE}.actions "
            f"WHERE status = 'ok' AND table_uuid IS NOT NULL").collect()}
    findings, parts = {}, {}
    for r in spark.sql(f"SELECT * FROM {sy} WHERE scan_id = '{scan_id}'").collect():
        findings.setdefault(r.table_name, []).append(r.asDict())
    for r in spark.sql(f"SELECT table_name, partition_key, minutes_since_update FROM {pm} "
                       f"WHERE scan_id = '{scan_id}'").collect():
        parts.setdefault(r.table_name, []).append(r.asDict())

    exp_tables = expectations.get("tables", {})
    results = []
    for t in tables:
        cfg = gl.table_config(config, t)
        phase = "after" if tmrows[t].get("table_uuid") in fixed else "before"
        res = score_table(t, exp_tables.get(t), findings.get(t, []), parts.get(t, []),
                          float(cfg.get("hot_partition_minutes", 15)), phase)
        res["table_name"] = t
        results.append(res)
    for t in sorted(set(exp_tables) - set(tables)):
        results.append({"table_name": t, "status": "MISSING TABLE", "found": [], "missing": [],
                        "unexpected": [], "notes": "not in this scan: run make gl-test-tables",
                        "phase": "before"})

    scored = [r for r in results if r["status"] != "NOT SCORED"]
    passed = sum(1 for r in scored if r["status"] == "PASS")
    print(f"\n=== Scorecard for scan {scan_id}: {passed}/{len(scored)} scenario tables pass ===")
    for r in results:
        ph = " (after fix)" if r["phase"] == "after" else ""
        print(f"  {r['status']:13} {r['table_name']:36} {', '.join(r['found']) or 'healthy'}{ph}")
        for m in r["missing"]:
            print(f"                  missing: {m}")
        if r["unexpected"]:
            print(f"                  unexpected: {', '.join(r['unexpected'])}")
        if r["notes"]:
            print(f"                  note: {r['notes']}")

    scored_at = probes.now_utc()
    rows = [(scan_id, scored_at, r["table_name"], r["status"], ", ".join(r["found"]),
             "; ".join(r["missing"]), ", ".join(r["unexpected"]), r["notes"], r["phase"])
            for r in results]
    spark.createDataFrame(rows, spark.table(sc).schema).writeTo(sc).append()
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
    scan_id = a.scan_id or latest_scan_id(spark)
    if not scan_id:
        sys.exit("No scan found: run make gl-scan first.")
    run_scorecard(spark, scan_id, gl.load_config(a.config), gl.load_config(a.expectations))
    spark.stop()


if __name__ == "__main__":
    main()
