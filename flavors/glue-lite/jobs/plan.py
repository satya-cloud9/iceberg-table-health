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
    status STRING, duration_s DOUBLE, result_json STRING,
    snapshot_before BIGINT, snapshot_after BIGINT, rollback_hint STRING"""

MODES = ("auto", "approve-only", "off")


def advisor_mode(props, cfg):
    """Per-table opt-out. The table property advisor.mode (set by the table's
    owner) wins over the config default advisor_mode:
      auto          auto steps run with APPLY=1 (default)
      approve-only  auto steps become ASK; they run only via APPROVE=<symptom>
      off           detect and report only; plan.py runs nothing on this table
    """
    mode = str((props or {}).get("advisor.mode") or cfg.get("advisor_mode", "auto")).strip().lower()
    return mode if mode in MODES else "auto"


def action_row(schema, **fields):
    """A glue.ops.actions row in the table's own column order (older tables
    gain new columns at the end)."""
    return tuple(fields.get(f.name) for f in schema)


def current_snapshot(spark, table):
    """The table's current snapshot id (None for an empty table), read fresh."""
    jt = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    jt.refresh()
    snap = jt.currentSnapshot()
    return None if snap is None else int(snap.snapshotId())


def rollback_hint(ident, before, after):
    """Only when the statement made a new snapshot; property changes don't."""
    if before is None or after is None or before == after:
        return None
    return (f"CALL glue.system.rollback_to_snapshot('{ident}', {before})  "
            f"-- undoes everything committed after it, including writers' commits; "
            f"not possible once that snapshot is expired")


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
        # Spark SQL escapes with backslashes. A doubled quote ('o''neil') is
        # two adjacent literals that Spark concatenates into 'oneil'.
        return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"
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


def partition_predicate(partition_key, fields, widened=None):
    """'{"occurred_at_day":"2026-09-03"}' -> SQL on source columns.

    "TRUE" for an unpartitioned table. A field whose transform can't be a
    range (bucket, truncate on strings) is dropped, which widens the scope to
    every value of that field; the rewrite still only picks small files, so
    widening costs some extra reading, never correctness. The dropped field
    names are added to `widened`. None if no field can be expressed.
    """
    try:
        values = json.loads(partition_key or "{}")
    except ValueError:
        return None
    if not values:
        return "TRUE"
    by_name = {f["name"]: f for f in fields}
    # After spec evolution a key carries every field ever used, with null for
    # fields from the other spec ({"occurred_at_day": "2026-09-01",
    # "occurred_at_month": null}). A null next to a non-null field on the same
    # source column means "not in this file's spec", not IS NULL.
    set_sources = {by_name[n]["source"] for n, v in values.items() if v is not None and n in by_name}
    parts, dropped = [], []
    for name, value in values.items():
        f = by_name.get(name)
        if f is None or f["transform"] == "void":
            continue
        if value is None and f["source"] in set_sources:
            continue
        p = field_predicate(f, value)
        if p is None:
            dropped.append(name)
        else:
            parts.append(p)
    if dropped and not parts:
        return None
    if widened is not None:
        widened.update(dropped)
    return " AND ".join(parts) if parts else "TRUE"


# ---------- findings -> statements (pure) ----------

def _sql_str(s):
    return s.replace("\\", "\\\\").replace('"', '\\"')


def plan_table(table, findings, tm, cfg, now=None):
    """-> list of steps: dict(kind, auto, symptoms, statement, note)."""
    th, rw = cfg["thresholds"], cfg.get("rewrite", {})
    ident = table.split(".", 1)[1] if table.startswith("glue.") else table
    fields = json.loads(tm.get("partition_fields_json") or "[]")
    target = int(tm.get("target_file_bytes") or cfg["target_file_bytes"])
    active = [f for f in findings if f["action"] in ("auto", "approval", "defer", "needs-evidence", "advisory")]
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
        preds, unsupported, widened = [], [], set()
        for key, _ in chosen:
            pr = partition_predicate(key, fields, widened)
            if pr is None:
                unsupported.append(key)
            elif f"({pr})" not in preds:              # widened keys can collapse to one
                preds.append(f"({pr})")
        syms = sorted(set().union(*(v["symptoms"] for _, v in chosen)))
        if "SCATTERED_SMALL_FILES" in names:
            syms.append("SCATTERED_SMALL_FILES")
        min_input = rw.get("min_input_files") or int(th["min_excess_files"]) + 1
        # the band detection judged by (small / oversized ratios), passed explicitly so
        # the rewrite picks exactly the files the scan flagged even if the ratios change
        opts = {"target-file-size-bytes": str(target),
                "min-file-size-bytes": str(int(target * float(cfg.get("small_file_ratio", 0.75)))),
                "max-file-size-bytes": str(int(target * float(cfg.get("oversized_file_ratio", 1.8)))),
                "min-input-files": str(min_input),
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
        if widened:
            note.append(f"scope widened over {', '.join(sorted(widened))} (bucket/truncate can't be a range)")
        appending = [json.loads(f.get("evidence_json") or "{}") for f in active
                     if f.get("partition_key") in dict(chosen)
                     and json.loads(f.get("evidence_json") or "{}").get("appends_continue")]
        if preds == ["(TRUE)"] and cfg.get("time_column") and appending:
            # GL2.6b: an unpartitioned table still being appended to: appends don't
            # conflict with the rewrite, but files written in the hot window are
            # likely to have neighbours soon; rewrite only rows older than it
            mins = float(appending[0].get("compact_older_than_min") or cfg.get("hot_partition_minutes", 15))
            older = (now or datetime.now(timezone.utc)) - timedelta(minutes=mins)
            preds = [f"({cfg['time_column']} < TIMESTAMP '{older.strftime('%Y-%m-%d %H:%M:%S')}')"]
            note.append(f"appends continue: only rows older than {mins:g} min ({cfg['time_column']})")
        if preds:
            where = "" if "(TRUE)" in preds else \
                f"where => \"{_sql_str(' OR '.join(preds))}\", "   # TRUE: whole table, no where
            opt_sql = ", ".join(f"'{k}', '{v}'" for k, v in opts.items())
            # honor the table's sort order: binpack would concatenate sorted files
            # unsorted and widen every output file's value range
            strategy = "sort" if tm.get("sort_order_defined") else "binpack"
            if strategy == "sort":
                note.append("sort strategy: the table has a sort order")
            steps.append({
                "kind": "rewrite_data_files", "auto": True, "symptoms": syms, "where": where,
                "statement": (f"CALL glue.system.rewrite_data_files(table => '{ident}', "
                              f"strategy => '{strategy}', {where}"
                              f"options => map({opt_sql}))"),
                "note": "; ".join(note) or f"{len(chosen)} partitions, ~{files_in_scope} files"})

    # 1b. deletes: the rewrite applies them, but the delete files can stay
    # attached. rewrite_data_files gives new files the starting sequence
    # number, so a delete file from the last DELETE still "applies" by
    # sequence number and remove-dangling-deletes keeps it. This procedure
    # drops delete rows whose data files are gone, and empty delete files.
    rw_step = next((s for s in steps if s["kind"] == "rewrite_data_files" and "DELETE_BUILDUP" in s["symptoms"]), None)
    if rw_step:
        # scoped to the partitions the rewrite touched (same predicate)
        steps.append({"kind": "rewrite_position_delete_files", "auto": True,
                      "symptoms": ["DELETE_BUILDUP"],
                      "statement": (f"CALL glue.system.rewrite_position_delete_files(table => '{ident}', "
                                    f"{rw_step.get('where', '')}options => map('rewrite-all', 'true'))"),
                      "note": "removes delete files left pointing at rewritten data files"})

    # 1c. GL2.6c DELETE_FILE_SPRAWL: merge many small position delete files in
    # the flagged partitions; no data file is rewritten. Partitions the data
    # rewrite already covers are left to step 1b.
    covered = {k for k, _ in chosen} if parts else set()
    sprawl = [f for f in active if f["symptom"] == "DELETE_FILE_SPRAWL" and f["action"] == "auto"
              and f.get("partition_key") not in covered]
    if sprawl:
        preds, widened = [], set()
        for f in sprawl:
            pr = partition_predicate(f["partition_key"], fields, widened)
            if pr is not None and f"({pr})" not in preds:
                preds.append(f"({pr})")
        if preds:
            where = "" if "(TRUE)" in preds else f"where => \"{_sql_str(' OR '.join(preds))}\", "
            steps.append({"kind": "rewrite_position_delete_files", "auto": True,
                          "symptoms": ["DELETE_FILE_SPRAWL"],
                          "statement": (f"CALL glue.system.rewrite_position_delete_files(table => '{ident}', "
                                        f"{where}options => map('rewrite-all', 'true'))"),
                          "note": f"merges position delete files in {len(sprawl)} partition(s); no data rewrite"})

    hot = [f for f in active if f["symptom"] in ("HOT_PARTITION", "SETTLING")]
    if hot:
        steps.append({"kind": "hold", "auto": False, "symptoms": sorted({f["symptom"] for f in hot}),
                      "statement": "",
                      "note": f"{len(hot)} partition(s) left out (still being written, or late data "
                              f"still expected): " + ", ".join(f["partition_key"] for f in hot)})

    held = [f for f in active if f["action"] == "advisory"]
    if held:
        steps.append({"kind": "hold", "auto": False, "symptoms": sorted({f["symptom"] for f in held}),
                      "statement": "", "note": f"{len(held)} finding(s) held: {held[0]['remedy']}"})

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
        older, note = "{now}", f"keeps the last {keep} snapshots"
        pre = tm.get("pre_refresh_snapshot_ms")
        if pre and int(cfg.get("keep_full_copies", 1)) >= 1:
            # GL2.5o+: keep the copy before the latest possible full refresh, so a bad
            # refresh can still be rolled back (expire only what is older than it)
            older = "TIMESTAMP '" + datetime.fromtimestamp(int(pre) / 1000.0, timezone.utc) \
                .strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "+00:00'"
            note += "; keeps the snapshot before the latest possible full refresh"
        steps.append({"kind": "expire_snapshots", "auto": True, "symptoms": ["SNAPSHOT_BUILDUP"],
                      "statement": (f"CALL glue.system.expire_snapshots(table => '{ident}', "
                                    f"older_than => {older}, retain_last => {keep})"),
                      "note": note})

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
        elif f["symptom"] == "REWRITE_CHURN":
            stmt = (f"ALTER TABLE {table} SET TBLPROPERTIES ('write.merge.mode' = 'merge-on-read', "
                    f"'write.update.mode' = 'merge-on-read', 'write.delete.mode' = 'merge-on-read')")
        elif f["symptom"] == "MIXED_SPEC":
            # rewrite_data_files writes into the current spec; rewrite-all also
            # picks files that are well sized but sit in the old layout.
            stmt = (f"CALL glue.system.rewrite_data_files(table => '{ident}', "
                    f"options => map('rewrite-all', 'true', 'target-file-size-bytes', '{target}'))")
        elif f["symptom"] == "RETAINED_STORAGE":
            # expire to the configured policy now; shortening the policy is the owner's call
            age_h = float(th.get("max_snapshot_age_h", 120))
            keep = int(rw.get("expire_retain_last", 5))
            cut = (now or datetime.now(timezone.utc)) - timedelta(hours=age_h)
            stmt = (f"CALL glue.system.expire_snapshots(table => '{ident}', "
                    f"older_than => TIMESTAMP '{cut.strftime('%Y-%m-%d %H:%M:%S')}+00:00', retain_last => {keep})")
        elif f["symptom"] == "METADATA_BLOAT":
            props = json.loads(tm.get("properties_json") or "{}")
            if str(props.get("write.metadata.delete-after-commit.enabled", "false")).lower() != "true":
                stmt = (f"ALTER TABLE {table} SET TBLPROPERTIES "
                        f"('write.metadata.delete-after-commit.enabled' = 'true')")
        elif f["symptom"] == "UNBOUNDED_RETENTION":
            stmt = (f"ALTER TABLE {table} SET TBLPROPERTIES "
                    f"('write.metadata.delete-after-commit.enabled' = 'true')")
        elif f["symptom"] == "ORPHAN_FILES":
            stmt = (f"CALL glue.system.remove_orphan_files(table => '{ident}', "
                    f"older_than => {{orphan_cutoff}}, prefix_listing => true)")
        elif f["symptom"] == "POOR_CLUSTERING" and ev.get("column"):
            rewrite = f"CALL glue.system.rewrite_data_files(table => '{ident}', strategy => 'sort')"
            stmt = rewrite if tm.get("sort_order_defined") else \
                f"ALTER TABLE {table} WRITE ORDERED BY {ev['column']}; {rewrite}"
        steps.append({"kind": "suggest", "auto": False, "symptoms": [f["symptom"]], "statement": stmt,
                      "note": f"{f['action']}: {f['remedy']}"})

    # Per-table opt-out: auto steps become suggestions the owner approves.
    mode = advisor_mode(json.loads(tm.get("properties_json") or "{}"), cfg)
    if mode != "auto" and steps:
        for s in steps:
            if s["auto"]:
                s.update(auto=False, kind="suggest", planned_kind=s["kind"],
                         note=f"advisor.mode={mode}: {s['note']}")
        steps.insert(0, {"kind": "hold", "auto": False, "symptoms": [], "statement": "",
                         "note": (f"advisor.mode={mode}: nothing runs on this table"
                                  if mode == "off" else
                                  f"advisor.mode={mode}: auto steps need APPROVE=<symptom>")})
    return steps


DANGLING = "'remove-dangling-deletes', 'true'"


def _ident(table):
    return table.split(".", 1)[1] if table.startswith("glue.") else table


def _unsupported_option(error):
    e = error.lower()
    return "remove-dangling-deletes" in e or "cannot use options" in e or "not supported" in e


def strip_dangling_option(stmt):
    return stmt.replace(", " + DANGLING, "").replace(DANGLING + ", ", "")


def run_sql(spark, stmt):
    """-> (status, result_json_or_error, seconds)."""
    t0 = time.perf_counter()
    try:
        rows = [r.asDict() for r in spark.sql(stmt).collect()]
        status, result = "ok", json.dumps(rows, default=str)
    except Exception as e:  # record and carry on with the next step
        status, result = "failed", f"{type(e).__name__}: {e}"[:2000]
    return status, result, round(time.perf_counter() - t0, 2)


PROCEDURE_MIN_ORPHAN_MINUTES = 24 * 60   # remove_orphan_files refuses older_than < 24 h ago


def remove_orphans_action(spark, table, older_than_ms, prefix_listing=True):
    """remove_orphan_files through Iceberg's action API, for cutoffs younger
    than the procedure's 24-hour floor (the test setup: orphans minutes old).
    Same work as the CALL: list the location, keep what any retained snapshot
    or metadata version references, delete the rest older than the cutoff.
    -> (status, result_json_or_error, seconds)."""
    t0 = time.perf_counter()
    try:
        jvm = spark._jvm
        jt = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
        action = (jvm.org.apache.iceberg.spark.actions.SparkActions.get(spark._jsparkSession)
                  .deleteOrphanFiles(jt).olderThan(int(older_than_ms)))
        if prefix_listing:
            try:
                action = action.usePrefixListing(True)
            except Exception:       # older Iceberg: Hadoop listing
                pass
        it = action.execute().orphanFileLocations().iterator()
        removed = []
        while it.hasNext():
            removed.append(str(it.next()))
        status = "ok"
        result = json.dumps([{"orphan_file_location_count": len(removed), "sample": removed[:5]}])
    except Exception as e:
        status, result = "failed", f"{type(e).__name__}: {e}"[:2000]
    return status, result, round(time.perf_counter() - t0, 2)


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
    p.add_argument("--approve", default="",
                   help="comma-separated symptoms whose suggested (approval) statements to run, "
                        "e.g. MIXED_SPEC,ORPHAN_FILES; recorded in glue.ops.actions as approved")
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
    approve = {x.strip().upper() for x in a.approve.split(",") if x.strip()}
    mode = " + ".join(m for m in ("APPLY" if a.apply else "", f"APPROVE {sorted(approve)}" if approve else "")
                      if m) or "dry run"
    print(f"=== Plan {run_id} from scan {scan_id} ({mode}) ===", flush=True)

    if a.apply or approve:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {ns}.actions ({ACTIONS_DDL}) USING iceberg")
        gl.ensure_columns(spark, f"{ns}.actions", ACTIONS_DDL)
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
        if not (a.apply or approve):
            continue
        uuid = probes.table_info(spark, t).get("uuid")
        cfg_t = gl.table_config(config, t)
        if advisor_mode(json.loads(tms[t].get("properties_json") or "{}"), cfg_t) == "off":
            print(f"  advisor.mode=off: skipped", flush=True)
            continue
        ident_t = _ident(t)
        for s in steps:
            approved = (not s["auto"] and s["kind"] == "suggest" and s["statement"]
                        and approve & set(s["symptoms"]))
            if not ((s["auto"] and a.apply) or approved):
                continue
            now = datetime.now(timezone.utc)
            cutoff = now.timestamp() - float(cfg_t.get("orphan_min_age_minutes", 4320)) * 60
            cutoff_txt = datetime.fromtimestamp(cutoff, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') + "+00:00"
            text = (s["statement"]
                    .replace("{now}", f"TIMESTAMP '{now.strftime('%Y-%m-%d %H:%M:%S')}+00:00'")
                    .replace("{orphan_cutoff}", "TIMESTAMP '"
                             + datetime.fromtimestamp(cutoff, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') + "+00:00'"))
            # The +00:00 matters: a bare TIMESTAMP literal is read in the Spark session's
            # time zone; off UTC, the orphan cutoff would move (later = younger files deleted).
            kind = s["kind"] if s["auto"] else "approved:" + ",".join(s["symptoms"])
            for stmt in [x.strip() for x in text.split("; ") if x.strip()]:   # suggestions may hold several
                before = current_snapshot(spark, t)
                age_min = float(cfg_t.get("orphan_min_age_minutes", 4320))
                if "remove_orphan_files" in stmt and age_min < PROCEDURE_MIN_ORPHAN_MINUTES:
                    # Test scale: the procedure refuses a cutoff under 24 h, the action API doesn't.
                    stmt = (f"SparkActions.deleteOrphanFiles({ident_t}).olderThan({cutoff_txt})"
                            f".usePrefixListing(true)  -- action API: orphan_min_age_minutes={age_min:g} "
                            f"is under the procedure's 24 h floor")
                    status, result, dur = remove_orphans_action(spark, t, cutoff * 1000)
                else:
                    status, result, dur = run_sql(spark, stmt)
                if status == "failed" and "prefix_listing" in stmt and "prefix_listing" in result:
                    stmt = stmt.replace(", prefix_listing => true", "")       # older Iceberg: no such arg
                    status, result, dur = run_sql(spark, stmt)
                after = current_snapshot(spark, t)
                hint = rollback_hint(ident_t, before, after) if status == "ok" else None
                print(f"  -> {kind}: {status} in {dur}s  snapshot {before} -> {after}  {result[:300]}",
                      flush=True)
                records.append(action_row(schema, run_id=run_id, scan_id=scan_id, started_at=now,
                                          table_name=t, table_uuid=uuid, kind=kind,
                                          symptoms=",".join(s["symptoms"]), statement=stmt, status=status,
                                          duration_s=float(dur), result_json=result,
                                          snapshot_before=before, snapshot_after=after, rollback_hint=hint))
                if status != "ok":
                    break
            if status == "failed" and DANGLING in stmt and _unsupported_option(result):
                # Older Iceberg: retry without the option (the next step,
                # rewrite_position_delete_files, still cleans up the deletes).
                retry = strip_dangling_option(stmt)
                before = current_snapshot(spark, t)
                status, result, dur = run_sql(spark, retry)
                after = current_snapshot(spark, t)
                print(f"  -> {s['kind']} (retry without remove-dangling-deletes): {status} in {dur}s  "
                      f"{result[:300]}", flush=True)
                records.append(action_row(schema, run_id=run_id, scan_id=scan_id,
                                          started_at=datetime.now(timezone.utc), table_name=t,
                                          table_uuid=uuid, kind=s["kind"], symptoms=",".join(s["symptoms"]),
                                          statement=retry, status=status, duration_s=float(dur),
                                          result_json=result, snapshot_before=before, snapshot_after=after,
                                          rollback_hint=rollback_hint(ident_t, before, after)
                                          if status == "ok" else None))
    if (a.apply or approve) and records:
        spark.createDataFrame(records, schema).writeTo(f"{ns}.actions").append()
        print(f"\nRecorded {len(records)} actions in {ns}.actions under {run_id} (with snapshot "
              f"before/after and a rollback statement each). Run make gl-scan to check the result.",
              flush=True)
    elif not (a.apply or approve):
        print("\nDry run: nothing changed. APPLY=1 runs the RUN steps; APPROVE=<SYMPTOM,...> runs "
              "the ASK steps for those symptoms.", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
