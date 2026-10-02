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

ACTION_ORDER = {"auto": 0, "defer": 1, "approval": 2, "needs-evidence": 3}


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


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scan-id", default=None, help="default: the latest scan")
    p.add_argument("--config", default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                    "config", "health.json"))
    a = p.parse_args()

    config = gl.load_config(a.config)
    spark = SparkSession.builder.appName("gl25-detect-symptoms").getOrCreate()
    tm_table = f"{gl.OPS_NAMESPACE}.table_metrics"
    pm_table = f"{gl.OPS_NAMESPACE}.partition_metrics"
    sy_table = f"{gl.OPS_NAMESPACE}.symptoms"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {sy_table} ({SYMPTOMS_DDL}) USING iceberg")
    schema = spark.table(sy_table).schema

    scan_id = a.scan_id or spark.sql(
        f"SELECT max_by(scan_id, scanned_at) AS s FROM {tm_table}").collect()[0].s
    if not scan_id:
        sys.exit("No scan found: run make gl-metrics first.")

    tms = [r.asDict() for r in spark.sql(f"SELECT * FROM {tm_table} WHERE scan_id = '{scan_id}'").collect()]
    parts = {}
    for r in spark.sql(f"SELECT * FROM {pm_table} WHERE scan_id = '{scan_id}'").collect():
        parts.setdefault(r.table_name, []).append(r.asDict())

    detected_at = probes.now_utc()
    findings = []
    for tm in sorted(tms, key=lambda t: t["table_name"]):
        table = tm["table_name"]
        findings += symptom_rules.evaluate(table, tm, parts.get(table, []),
                                           gl.table_config(config, table))

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

    print("\n=== Ranked findings (top 40; held ones last) ===")
    for f in findings[:40]:
        where = f"  [{f['partition_key']}]" if f["partition_key"] else ""
        ev = json.loads(f["evidence_json"])
        brief = ", ".join(f"{k}={v}" for k, v in list(ev.items())[:4])
        print(f"  {f['action']:14} {f['severity']:6} {f['score']:7.2f}  {f['symptom']:22} "
              f"{f['table_name']}{where}\n      {brief}\n      -> {f['remedy']}")
    print(f"\nRecorded in {sy_table} under scan_id {scan_id}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
