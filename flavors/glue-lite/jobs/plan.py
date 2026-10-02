"""GL2.5e: turn a scan's findings into maintenance statements, and optionally run them.

Reads one scan (default: the latest) from glue.ops.symptoms and
glue.ops.table_metrics. For each table it builds, in order:

  1. rewrite_data_files (binpack) over the flagged partitions
       SMALL_FILES / SCATTERED_SMALL_FILES / OVERSIZED_FILES / DELETE_BUILDUP;
       top max_partitions_per_run by score; hot partitions are never included
  2. rewrite_manifests                       MANIFEST_BLOAT
  3. expire_snapshots (retain_last N)        SNAPSHOT_BUILDUP

Options come from the same config the scan used, so a flagged partition is
always one the rewrite will change: target size = the target the scan judged
by; min-input-files = min_excess_files + 1; delete-file-threshold = 1 when
deletes are the problem.

Approval and needs-evidence findings are printed as suggested statements but
never run. Without --apply nothing runs. With --apply the auto statements run
and each one is recorded in glue.ops.actions (with the table's UUID, so the
scorecard knows the table has been fixed until it is rebuilt).

Usage (via scripts/run-job.sh py plan.py ...):
  plan.py [--tables s0,s2_mor_deletes] [--scan-id scan-...] [--apply]
"""
import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

HERE = os.path.dirname(os.path.abspath(__file__))
EPOCH = datetime(1970, 1, 1)
BINPACK_SYMPTOMS = ("SMALL_FILES", "OVERSIZED_FILES", "DELETE_BUILDUP")

ACTIONS_DDL = """
    run_id STRING, scan_id STRING, started_at TIMESTAMP, table_name STRING,
    table_uuid STRING, kind STRING, symptoms STRING, statement STRING,
    status STRING, duration_s DOUBLE, result_json STRING"""


# ---------- partition key -> WHERE predicate (pure) ----------

def _ts(dt):
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S')}'"


def _range(col, lo, hi, is_date):
    if is_date:
        return f"{col} >= DATE '{lo.date().isoformat()}' AND {col} < DATE '{hi.date().isoformat()}'"
    return f"{col} >= {_ts(lo)} AND {col} < {_ts(hi)}"


def _add_months(dt, n):
    m = dt.month - 1 + n
    return dt.replace(year=dt.year + m // 12, month=m % 12 + 1, day=1)


def _literal(value, source_type):
    if value is None:
        return None
    if source_type in ("string", "uuid"):
        return "'" + str(value).replace("'", "''") + "'"
    if source_type == "date":
        return f"DATE '{value}'"
    if source_type.startswith("timestamp"):
        return f"TIMESTAMP '{value}'"
    return str(value)


def field_predicate(field, value):
    """SQL predicate on the source column for one partition field, or None if
    the transform can't be expressed as a range (bucket, truncate on strings)."""
    col, t, st = field["source"], field["transform"], field["source_type"]
    is_date = st == "date"
    if value is None:
        return f"{col} IS NULL"
    if t == "identity":
        return f"{col} = {_literal(value, st)}"
    if t == "day":
        d = (datetime.combine(date.fromisoformat(value), datetime.min.time())
             if isinstance(value, str) else EPOCH + timedelta(days=int(value)))
        return _range(col, d, d + timedelta(days=1), is_date)
    if t == "hour":
        if isinstance(value, str):          # 'YYYY-MM-DD-HH'
            d = datetime.strptime(value, "%Y-%m-%d-%H")
        else:                               # hours since epoch
            d = EPOCH + timedelta(hours=int(value))
        return _range(col, d, d + timedelta(hours=1), False)
    if t == "month":
        if isinstance(value, str):          # 'YYYY-MM'
            d = datetime.strptime(value, "%Y-%m")
        else:
            d = _add_months(EPOCH, int(value))
        return _range(col, d, _add_months(d, 1), is_date)
    if t == "year":
        y = int(value) if not isinstance(value, str) else int(value)
        y = y + 1970 if y < 1000 else y
        d = datetime(y, 1, 1)
        return _range(col, d, datetime(y + 1, 1, 1), is_date)
    return None


def partition_predicate(partition_key, fields):
    """'{"occurred_at_day":"2026-09-03"}' -> SQL on source columns, or None."""
    try:
        values = json.loads(partition_key or "{}")
    except ValueError:
        return None
    if not values:
        return "TRUE"
    by_name = {f["name"]: f for f in fields}
    parts = []
    for name, value in values.items():
        f = by_name.get(name)
        if f is None or f["transform"] == "void":
            continue
        p = field_predicate(f, value)
        if p is None:
            return None
        parts.append(p)
    return " AND ".join(parts) if parts else "TRUE"


# ---------- findings -> statements (pure) ----------

def _sql_str(s):
    return s.replace("\\", "\\\\").replace('"', '\\"')


def plan_table(table, findings, tm, cfg):
    """-> list of steps: dict(kind, auto, symptoms, statement, note)."""
    th, rw = cfg["thresholds"], cfg.get("rewrite", {})
    ident = table.split(".", 1)[1] if table.startswith("glue.") else table
    fields = json.loads(tm.get("partition_fields_json") or "[]")
    target = int(tm.get("target_file_bytes") or cfg["target_file_bytes"])
    active = [f for f in findings if f["action"] in ("auto", "approval", "defer", "needs-evidence")]
    names = {f["symptom"] for f in active}
    steps = []

    # 1. binpack over flagged partitions
    parts = {}
    for f in active:
        if f["action"] == "auto" and f["symptom"] in BINPACK_SYMPTOMS and f.get("partition_key"):
            p = parts.setdefault(f["partition_key"], {"score": 0.0, "symptoms": set()})
            p["score"] = max(p["score"], float(f["score"]))
            p["symptoms"].add(f["symptom"])
    if parts:
        budget = int(rw.get("max_partitions_per_run", 10))
        ranked = sorted(parts.items(), key=lambda kv: -kv[1]["score"])
        chosen, skipped = ranked[:budget], ranked[budget:]
        preds, unsupported = [], []
        for key, _ in chosen:
            pr = partition_predicate(key, fields)
            (preds.append(f"({pr})") if pr else unsupported.append(key))
        syms = sorted(set().union(*(v["symptoms"] for _, v in chosen)))
        if "SCATTERED_SMALL_FILES" in names:
            syms.append("SCATTERED_SMALL_FILES")
        min_input = rw.get("min_input_files") or int(th["min_excess_files"]) + 1
        opts = {"target-file-size-bytes": str(target), "min-input-files": str(min_input),
                "max-concurrent-file-group-rewrites": str(rw.get("max_concurrent_file_group_rewrites", 2))}
        per_part = {}
        for f in active:
            if f.get("partition_key") in dict(chosen):
                n = int(json.loads(f.get("evidence_json") or "{}").get("data_files", 0))
                per_part[f["partition_key"]] = max(per_part.get(f["partition_key"], 0), n)
        files_in_scope = sum(per_part.values())
        if files_in_scope >= int(rw.get("partial_progress_min_files", 500)):
            opts["partial-progress.enabled"] = "true"
            opts["partial-progress.max-commits"] = str(rw.get("partial_progress_max_commits", 10))
        if any("DELETE_BUILDUP" in v["symptoms"] for _, v in chosen):
            opts["delete-file-threshold"] = "1"        # pick up files with any delete
            opts["remove-dangling-deletes"] = "true"   # drop delete files left pointing at nothing
        note = []
        if skipped:
            note.append(f"{len(skipped)} more flagged partitions left for the next run (budget {budget})")
        if unsupported:
            note.append(f"{len(unsupported)} partitions skipped: transform can't be scoped by a range")
        if preds:
            where = " OR ".join(preds)
            opt_sql = ", ".join(f"'{k}', '{v}'" for k, v in opts.items())
            steps.append({
                "kind": "rewrite_data_files", "auto": True, "symptoms": syms,
                "statement": (f"CALL glue.system.rewrite_data_files(table => '{ident}', "
                              f"strategy => 'binpack', where => \"{_sql_str(where)}\", "
                              f"options => map({opt_sql}))"),
                "note": "; ".join(note) or f"{len(chosen)} partitions, ~{files_in_scope} files"})

    hot = [f for f in active if f["symptom"] == "HOT_PARTITION"]
    if hot:
        steps.append({"kind": "hold", "auto": False, "symptoms": ["HOT_PARTITION"], "statement": "",
                      "note": f"{len(hot)} hot partition(s) left out until writes stop: "
                              + ", ".join(f["partition_key"] for f in hot)})

    # 2. manifests
    if any(f["symptom"] == "MANIFEST_BLOAT" and f["action"] == "auto" for f in active):
        steps.append({"kind": "rewrite_manifests", "auto": True, "symptoms": ["MANIFEST_BLOAT"],
                      "statement": f"CALL glue.system.rewrite_manifests(table => '{ident}')",
                      "note": "merges manifests; planning reads fewer of them"})
        props = json.loads(tm.get("properties_json") or "{}")
        if str(props.get("commit.manifest-merge.enabled", "true")).lower() == "false":
            steps.append({"kind": "suggest", "auto": False, "symptoms": ["MANIFEST_BLOAT"],
                          "statement": f"ALTER TABLE {table} SET TBLPROPERTIES "
                                       f"('commit.manifest-merge.enabled' = 'true')",
                          "note": "needs approval: otherwise the manifests pile up again"})

    # 3. snapshots last, so the snapshots the rewrites replaced can expire too
    if any(f["symptom"] == "SNAPSHOT_BUILDUP" and f["action"] == "auto" for f in active):
        keep = int(rw.get("expire_retain_last", 5))
        steps.append({"kind": "expire_snapshots", "auto": True, "symptoms": ["SNAPSHOT_BUILDUP"],
                      "statement": (f"CALL glue.system.expire_snapshots(table => '{ident}', "
                                    f"older_than => {{now}}, retain_last => {keep})"),
                      "note": f"keeps the last {keep} snapshots"})

    # approval / needs-evidence: suggestions only
    for f in active:
        if f["action"] not in ("approval", "needs-evidence") or f["symptom"] == "MANIFEST_BLOAT":
            continue
        ev = json.loads(f.get("evidence_json") or "{}")
        stmt = ""
        if f["symptom"] == "OVER_PARTITIONED":
            fine = [x for x in fields if x["transform"] in ("hour", "day", "month")]
            if fine:
                x = fine[0]
                coarser = {"hour": "days", "day": "months", "month": "years"}[x["transform"]]
                stmt = (f"ALTER TABLE {table} REPLACE PARTITION FIELD {x['name']} WITH "
                        f"{coarser}({x['source']}); CALL glue.system.rewrite_data_files("
                        f"table => '{ident}', options => map('rewrite-all', 'true'))")
        elif f["symptom"] == "POOR_CLUSTERING" and ev.get("column"):
            stmt = (f"ALTER TABLE {table} WRITE ORDERED BY {ev['column']}; "
                    f"CALL glue.system.rewrite_data_files(table => '{ident}', strategy => 'sort')")
        steps.append({"kind": "suggest", "auto": False, "symptoms": [f["symptom"]], "statement": stmt,
                      "note": f"{f['action']}: {f['remedy']}"})
    return steps


def match_tables(all_tables, wanted):
    """'s0' matches glue.demo.s0_small_appends; full or short names also work."""
    if not wanted:
        return list(all_tables)
    out = []
    for t in all_tables:
        short = t.rsplit(".", 1)[-1]
        if any(w in (t, short) or short.startswith(w + "_") for w in wanted):
            out.append(t)
    return out


# ---------- Spark job ----------

def main():
    import gl_common as gl
    import probes
    from detect_symptoms import latest_scan_id
    from pyspark.sql import SparkSession

    p = argparse.ArgumentParser()
    p.add_argument("--tables", default="", help="comma-separated, e.g. s0,s2 (default: all in the scan)")
    p.add_argument("--scan-id", default=None)
    p.add_argument("--apply", action="store_true", help="run the auto statements")
    p.add_argument("--config", default=os.path.join(HERE, "config", "health.json"))
    a = p.parse_args()

    config = gl.load_config(a.config)
    spark = SparkSession.builder.appName("gl25-plan").getOrCreate()
    scan_id = a.scan_id or latest_scan_id(spark)
    if not scan_id:
        sys.exit("No scan found: run make gl-scan first.")
    ns = gl.OPS_NAMESPACE
    tms = {r.table_name: r.asDict() for r in
           spark.sql(f"SELECT * FROM {ns}.table_metrics WHERE scan_id = '{scan_id}'").collect()}
    findings = {}
    for r in spark.sql(f"SELECT * FROM {ns}.symptoms WHERE scan_id = '{scan_id}'").collect():
        findings.setdefault(r.table_name, []).append(r.asDict())

    tables = match_tables(sorted(tms), [t.strip() for t in a.tables.split(",") if t.strip()])
    run_id = gl.new_run_id("plan")
    print(f"=== Plan {run_id} from scan {scan_id} ({'APPLY' if a.apply else 'dry run'}) ===", flush=True)

    if a.apply:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {ns}.actions ({ACTIONS_DDL}) USING iceberg")
        schema = spark.table(f"{ns}.actions").schema
    records = []
    for t in tables:
        steps = plan_table(t, findings.get(t, []), tms[t], gl.table_config(config, t))
        print(f"\n--- {t}: {'nothing to do' if not steps else ''}", flush=True)
        for s in steps:
            tag = "RUN " if s["auto"] else ("HOLD" if s["kind"] == "hold" else "ASK ")
            print(f"  [{tag}] {s['kind']:18} {', '.join(s['symptoms'])}\n         {s['note']}", flush=True)
            if s["statement"]:
                print(f"         {s['statement']}", flush=True)
        if not a.apply:
            continue
        uuid = probes.table_info(spark, t).get("uuid")
        for s in steps:
            if not s["auto"]:
                continue
            now = datetime.now(timezone.utc)
            stmt = s["statement"].replace("{now}", f"TIMESTAMP '{now.strftime('%Y-%m-%d %H:%M:%S')}'")
            t0 = time.perf_counter()
            try:
                rows = [r.asDict() for r in spark.sql(stmt).collect()]
                status, result = "ok", json.dumps(rows, default=str)
            except Exception as e:  # record and continue with the next table
                status, result = "failed", f"{type(e).__name__}: {e}"[:2000]
            dur = round(time.perf_counter() - t0, 2)
            print(f"  -> {s['kind']}: {status} in {dur}s  {result[:300]}", flush=True)
            records.append((run_id, scan_id, now, t, uuid, s["kind"], ",".join(s["symptoms"]),
                            stmt, status, float(dur), result))
    if a.apply and records:
        spark.createDataFrame(records, schema).writeTo(f"{ns}.actions").append()
        print(f"\nRecorded {len(records)} actions in {ns}.actions under {run_id}. "
              f"Run make gl-scan to check the result.", flush=True)
    elif not a.apply:
        print("\nDry run: nothing changed. Add APPLY=1 to run the RUN steps.", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
