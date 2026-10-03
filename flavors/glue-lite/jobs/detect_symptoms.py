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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession

import gl_common as gl
import probes
import symptom_rules

SYMPTOMS_DDL = """
    scan_id STRING, detected_at TIMESTAMP, table_name STRING, partition_key STRING,
    symptom STRING, category STRING, level STRING, action STRING, automatic BOOLEAN,
    score DOUBLE, severity STRING, remedy STRING, evidence_level STRING,
    evidence_json STRING, rule_version STRING"""

ACTION_ORDER = {"auto": 0, "defer": 1, "approval": 2, "advisory": 3, "needs-evidence": 4}


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


def load_history(spark, tms, scan_id):
    """Per table UUID: earlier scans (oldest first, up to the current one) and actions.
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
            SELECT table_uuid, scanned_at, {', '.join(want)}
            FROM {gl.OPS_NAMESPACE}.table_metrics
            WHERE table_uuid IN ({ids}) AND scanned_at <= (
                SELECT max(scanned_at) FROM {gl.OPS_NAMESPACE}.table_metrics WHERE scan_id = '{scan_id}')
            ORDER BY scanned_at""").collect():
        history.setdefault(r.table_uuid, []).append(r.asDict())
    if spark.catalog.tableExists(f"{gl.OPS_NAMESPACE}.actions"):
        for r in spark.sql(f"""
                SELECT table_uuid, started_at, kind, status, result_json
                FROM {gl.OPS_NAMESPACE}.actions WHERE table_uuid IN ({ids})
                ORDER BY started_at""").collect():
            actions.setdefault(r.table_uuid, []).append(r.asDict())
    return history, actions


def run_detect(spark, scan_id, config, report=True):
    """Apply the rules to one scan, append to glue.ops.symptoms, return the findings."""
    tm_table = f"{gl.OPS_NAMESPACE}.table_metrics"
    pm_table = f"{gl.OPS_NAMESPACE}.partition_metrics"
    sy_table = f"{gl.OPS_NAMESPACE}.symptoms"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {sy_table} ({SYMPTOMS_DDL}) USING iceberg")
    schema = spark.table(sy_table).schema

    tms = [r.asDict() for r in spark.sql(f"SELECT * FROM {tm_table} WHERE scan_id = '{scan_id}'").collect()]
    parts = {}
    for r in spark.sql(f"SELECT * FROM {pm_table} WHERE scan_id = '{scan_id}'").collect():
        parts.setdefault(r.table_name, []).append(r.asDict())

    history, actions = load_history(spark, tms, scan_id)
    detected_at = probes.now_utc()
    findings = []
    for tm in sorted(tms, key=lambda t: t["table_name"]):
        table, uuid = tm["table_name"], tm.get("table_uuid")
        findings += symptom_rules.evaluate(table, tm, parts.get(table, []),
                                           gl.table_config(config, table),
                                           history=history.get(uuid, []), actions=actions.get(uuid, []))

    print(f"=== Symptoms for scan {scan_id}: {len(findings)} findings over {len(tms)} tables ===",
          flush=True)
    if findings:
        rows = []
        for f in findings:
            f.update(scan_id=scan_id, detected_at=detected_at)
            rows.append(tuple(coerce(f.get(c.name), c.dataType) for c in schema))
        spark.createDataFrame(rows, schema).writeTo(sy_table).append()

    findings.sort(key=lambda f: (ACTION_ORDER.get(f["action"], 9), -f["score"]))
    print("\n=== Per table ===")
    for tm in sorted(tms, key=lambda t: t["table_name"]):
        mine = [f for f in findings if f["table_name"] == tm["table_name"] and f["action"] != "needs-evidence"]
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
