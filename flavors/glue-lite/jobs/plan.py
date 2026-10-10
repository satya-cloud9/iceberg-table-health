"""GL2.5e: turn a scan's findings into maintenance statements, and optionally run them.

Dry run (no --apply / --approve): reads each table's latest scan from the state
store and prints the plan; nothing is read again, nothing changes.

With --apply or --approve, one run per table in the order of design decision
D1 (Retention discussion), under one lease per table (coordinator.py):

  scan        an incremental scan and detection of the table, so decisions are
              as fresh as the run
  1 config    approved property changes and tag drops
  2 deletes   rewrite_position_delete_files for DELETE_FILE_SPRAWL
  3 rewrites  rewrite_data_files over the flagged partitions (then the delete
              files it left pointing at nothing), approved spec or sort rewrites
  4 manifests rewrite_manifests                  MANIFEST_BLOAT
  5 catch-up  a second incremental scan: the ledger ingests every commit up to
              now, the writer's and the advisor's own (labelled maintenance)
  7 expiry    expire_keep_files                  SNAPSHOT_BUILDUP, from the
              catch-up's findings; the cutoff is capped at the newest snapshot
              the ledger ingested, so no commit the advisor expires is unseen;
              files are kept and recorded with their due time (freed_files.py)
  8 freed     delete_freed_files: recorded files past their grace
  9 orphans   approved remove_orphan_files, held while freed files wait
  (6, checkpoint tags, comes later: D5)

Every step checks its own trigger on every run, whether or not anything else
ran. A failed step skips the rest of that table's steps; the next run starts
again from its own scan. The lease is renewed before each step; a run that
loses it stops after the step in progress.

Options come from the same config the scan used, so a flagged partition is
always one the rewrite will change: target size = the target the scan judged
by; min-input-files = min_excess_files + 1; delete-file-threshold = 1 when
deletes are the problem.

Approval and needs-evidence findings are printed as suggested statements and
run only when approved (--approve SYMPTOM,...). Each executed step is recorded
in the actions log (with the table's UUID, so the scorecard knows the table has
been fixed until it is rebuilt).

Usage (via scripts/run-job.sh py plan.py ...):
  plan.py [--tables s0,s2_mor_deletes] [--scan-id scan-...] [--apply] [--approve X,Y]
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


def _declare_actions():
    import state_store as ss
    ss.declare_log("actions", ACTIONS_DDL, "started_at")


_declare_actions()

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


def ts_text(ms):
    return datetime.fromtimestamp(ms / 1000.0, timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "+00:00"


def _rewrite_step(chosen, kind, skipped, active, names, tm, cfg, th, rw, ident, fields, target, now):
    """One rewrite_data_files step over the chosen partitions. kind: None (no tier
    rule: today's options), "major" (fragments and undersized segments, deletes,
    oversized files) or "minor" (fragments only); tiered passes run with partial
    progress (max-commits 3) and the largest groups first."""
    preds, unsupported, widened = [], [], set()
    for key, _ in chosen:
        pr = partition_predicate(key, fields, widened)
        if pr is None:
            unsupported.append(key)
        elif f"({pr})" not in preds:              # widened keys can collapse to one
            preds.append(f"({pr})")
    syms = sorted(set().union(*(v["symptoms"] for _, v in chosen)))
    if "SCATTERED_SMALL_FILES" in names and kind != "minor":
        syms.append("SCATTERED_SMALL_FILES")
    keys = dict(chosen)
    evs = [json.loads(f.get("evidence_json") or "{}") for f in active if f.get("partition_key") in keys]
    if kind == "minor":
        # fragments only: files under target / fragment_ratio are the candidates,
        # nothing is too big, groups need as many fragments as made the pass worth it
        ratio = float((cfg.get("compaction") or {}).get("fragment_ratio", 8))
        frags = [int(e.get("fragments") or 0) for e in evs if e.get("tier_pass") == "minor"]
        min_frags = int((cfg.get("compaction") or {}).get("minor_min_fragments", 8))
        opts = {"target-file-size-bytes": str(target),
                "min-file-size-bytes": str(int(target / ratio)),
                "max-file-size-bytes": str(1 << 62),
                "min-input-files": str(max(2, min([min_frags] + frags))),
                "max-concurrent-file-group-rewrites": str(rw.get("max_concurrent_file_group_rewrites", 2))}
    else:
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
        if f.get("partition_key") in keys:
            n = int(json.loads(f.get("evidence_json") or "{}").get("data_files", 0))
            per_part[f["partition_key"]] = max(per_part.get(f["partition_key"], 0), n)
    files_in_scope = sum(per_part.values())
    if kind:
        opts["partial-progress.enabled"] = "true"
        opts["partial-progress.max-commits"] = str(rw.get("tier_partial_progress_max_commits", 3))
        opts["rewrite-job-order"] = "bytes-desc"
    elif files_in_scope >= int(rw.get("partial_progress_min_files", 500)):
        opts["partial-progress.enabled"] = "true"
        opts["partial-progress.max-commits"] = str(rw.get("partial_progress_max_commits", 10))
    if any("DELETE_BUILDUP" in v["symptoms"] for _, v in chosen):
        opts["delete-file-threshold"] = "1"        # pick up files with any delete
        opts["remove-dangling-deletes"] = "true"   # drop delete files left pointing at nothing
    note = []
    if kind:
        note.append(f"{kind} pass")
    if skipped:
        note.append(f"{len(skipped)} more flagged partitions left for the next run "
                    f"(budget {int(rw.get('max_partitions_per_run', 10))})")
    if unsupported:
        note.append(f"{len(unsupported)} partitions skipped: transform can't be scoped by a range")
    if widened:
        note.append(f"scope widened over {', '.join(sorted(widened))} (bucket/truncate can't be a range)")
    appending = [e for e in evs if e.get("appends_continue")]
    if preds == ["(TRUE)"] and cfg.get("time_column") and appending:
        # an unpartitioned table still being appended to: appends don't
        # conflict with the rewrite, but files written in the hot window are
        # likely to have neighbours soon; rewrite only rows older than it
        mins = float(appending[0].get("compact_older_than_min") or cfg.get("hot_partition_minutes", 15))
        older = (now or datetime.now(timezone.utc)) - timedelta(minutes=mins)
        preds = [f"({cfg['time_column']} < TIMESTAMP '{older.strftime('%Y-%m-%d %H:%M:%S')}')"]
        note.append(f"appends continue: only rows older than {mins:g} min ({cfg['time_column']})")
    if not preds:
        return None
    where = "" if "(TRUE)" in preds else \
        f"where => \"{_sql_str(' OR '.join(preds))}\", "   # TRUE: whole table, no where
    opt_sql = ", ".join(f"'{k}', '{v}'" for k, v in opts.items())
    # honor the table's sort order: binpack would concatenate sorted files
    # unsorted and widen every output file's value range (a minor sorts only the fragments)
    strategy = "sort" if tm.get("sort_order_defined") else "binpack"
    if strategy == "sort":
        note.append("sort strategy: the table has a sort order")
    step = {"kind": "rewrite_data_files", "auto": True, "symptoms": syms, "where": where,
            "statement": (f"CALL glue.system.rewrite_data_files(table => '{ident}', "
                          f"strategy => '{strategy}', {where}"
                          f"options => map({opt_sql}))"),
            "note": "; ".join(note[:1] + [f"{len(chosen)} partitions, ~{files_in_scope} files"] + note[1:])
            if kind else ("; ".join(note) or f"{len(chosen)} partitions, ~{files_in_scope} files")}
    if kind:
        step["tier_pass"] = kind
    return step


def plan_table(table, findings, tm, cfg, now=None, freed=None):
    """-> list of steps: dict(kind, auto, symptoms, statement, note[, op, params]).
    freed: freed_files.summary() of this table's recorded files, or None.
    Steps with an "op" run through Python (freed_files.py), not SQL; their
    statement is a readable description."""
    th, rw = cfg["thresholds"], cfg.get("rewrite", {})
    ident = table.split(".", 1)[1] if table.startswith("glue.") else table
    fields = json.loads(tm.get("partition_fields_json") or "[]")
    target = int(tm.get("target_file_bytes") or cfg["target_file_bytes"])
    active = [f for f in findings if f["action"] in ("auto", "approval", "defer", "needs-evidence", "advisory")]
    names = {f["symptom"] for f in active}
    steps = []

    # 1. binpack over flagged partitions. With the tier rule (item 12) each
    # partition's SMALL_FILES says its pass: partitions on a minor pass get one
    # CALL over their fragments only, the rest (major passes, deletes, oversized
    # files, and findings from before the tier rule) one CALL as before.
    parts = {}
    for f in active:
        if f["action"] == "auto" and f["symptom"] in BINPACK_SYMPTOMS and f.get("partition_key"):
            p = parts.setdefault(f["partition_key"], {"score": 0.0, "symptoms": set(), "passes": set()})
            p["score"] = max(p["score"], float(f["score"]))
            p["symptoms"].add(f["symptom"])
            p["passes"].add(json.loads(f.get("evidence_json") or "{}").get("tier_pass")
                            if f["symptom"] == "SMALL_FILES" else "other")
    chosen = []
    if parts:
        budget = int(rw.get("max_partitions_per_run", 10))
        ranked = sorted(parts.items(), key=lambda kv: -kv[1]["score"])
        chosen, skipped = ranked[:budget], ranked[budget:]
        minor = [(k, v) for k, v in chosen if v["passes"] == {"minor"}]
        rest = [(k, v) for k, v in chosen if v["passes"] != {"minor"}]
        tiered = any(v["passes"] & {"minor", "major"} for _, v in chosen)
        groups = [(g, kind) for g, kind in ((rest, "major" if tiered else None), (minor, "minor")) if g]
        for i, (group, kind) in enumerate(groups):
            step = _rewrite_step(group, kind, skipped if i == 0 else [], active, names, tm, cfg, th, rw, ident,
                                 fields, target, now)
            if step:
                steps.append(step)

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
    covered = {k for k, _ in chosen}
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

    hot = [f for f in active if f["symptom"] == "HOT_PARTITION"]
    if hot:
        steps.append({"kind": "hold", "auto": False, "symptoms": ["HOT_PARTITION"],
                      "statement": "",
                      "note": f"{len(hot)} partition(s) left out (a writer changed their files moments ago): "
                              + ", ".join(f["partition_key"] or "(table)" for f in hot)})

    held = [f for f in active if f["action"] == "advisory" and f["symptom"] != "RETAINED_STORAGE"]
    for f in active:
        if f["action"] == "advisory" and f["symptom"] == "RETAINED_STORAGE":
            # nothing past the policy: advice for the owner, nothing to run or approve
            steps.append({"kind": "advice", "auto": False, "symptoms": [f["symptom"]], "statement": "",
                          "note": f"advice: {f['remedy']}"})
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

    # 3. snapshots last, so the snapshots the rewrites replaced can expire too.
    #    Files are kept: the expiry records what it freed, deleted after the grace (4).
    exp = cfg.get("expiry") or {}
    sb = next((f for f in active if f["symptom"] == "SNAPSHOT_BUILDUP" and f["action"] == "auto"), None)
    if sb:
        keep = int(rw.get("expire_retain_last", 5))
        pol = json.loads(sb.get("evidence_json") or "{}")
        now_ms = (now or datetime.now(timezone.utc)).timestamp() * 1000
        cut_ms = now_ms
        note = f"keeps the last {keep} snapshots"
        if pol.get("policy_age_h") is not None:
            # expire to the retention policy, never inside it
            keep = int(pol.get("policy_min_keep") or keep)
            cut_ms = now_ms - float(pol["policy_age_h"]) * 3600000
            note = (f"policy: older than {float(pol['policy_age_h']):g} h, keeping the last {keep} "
                    f"({pol.get('policy_source')})")
        pre = tm.get("pre_refresh_snapshot_ms")
        if pre and int(cfg.get("keep_full_copies", 1)) >= 1 and int(pre) < cut_ms:
            # keep the copy before the latest possible full refresh, so a bad
            # refresh can still be rolled back (expire only what is older than it)
            cut_ms = int(pre)
            note += "; keeps the snapshot before the latest possible full refresh"
        grace = pol.get("policy_file_grace_h")
        gsrc = pol.get("policy_grace_source")
        if grace is None:
            import expiry as xp
            grace, gsrc = xp.file_grace(json.loads(tm.get("properties_json") or "{}"), exp)
        steps.append({"kind": "expire_keep_files", "auto": True, "symptoms": ["SNAPSHOT_BUILDUP"],
                      "op": "expire_keep_files",
                      "params": {"older_than_ms": int(cut_ms), "retain_last": keep, "grace_h": float(grace)},
                      "statement": (f"expire snapshots older than {ts_text(int(cut_ms))}, retain_last {keep}, "
                                    f"files kept; freed files deleted after {float(grace):g} h"),
                      "note": f"{note}; file grace {float(grace):g} h ({gsrc})"})

    # 4. files earlier expiries freed, past their grace
    if freed and freed.get("due"):
        nxt = freed.get("next_due_ms")
        steps.append({"kind": "delete_freed_files", "auto": True, "symptoms": ["FREED_FILES"], "op": "delete_freed_files",
                      "params": {}, "statement": f"delete {freed['due']} freed files past their grace",
                      "note": (f"{freed['due']} files freed by earlier expiries are past their grace"
                               + (f"; {freed['waiting']} more wait until {ts_text(nxt)} or later"
                                  if freed.get("waiting") and nxt else ""))})

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
            # expire to the configured policy now, files kept for the grace;
            # shortening the policy is the owner's call
            import expiry as xp
            if any(s["kind"] == "expire_keep_files" for s in steps):
                # SNAPSHOT_BUILDUP's expiry already expires to the same policy
                steps.append({"kind": "advice", "auto": False, "symptoms": [f["symptom"]], "statement": "",
                              "note": (f"covered by the expiry above (SNAPSHOT_BUILDUP, same policy); what old "
                                       f"snapshots still keep afterwards is reported on the next scan: "
                                       f"{f['remedy']}")})
                continue
            # the table's own resolved policy (as for SNAPSHOT_BUILDUP); the global
            # threshold only for scans from before the policy was recorded
            age_h = float(tm["policy_age_h"]) if tm.get("policy_age_h") is not None \
                else float(th.get("max_snapshot_age_h", 120))
            keep = int(tm.get("policy_min_keep") or rw.get("expire_retain_last", 5))
            cut_ms = int(((now or datetime.now(timezone.utc)) - timedelta(hours=age_h)).timestamp() * 1000)
            if tm.get("policy_file_grace_h") is not None:
                grace, gsrc = float(tm["policy_file_grace_h"]), tm.get("policy_grace_source")
            else:
                grace, gsrc = xp.file_grace(json.loads(tm.get("properties_json") or "{}"), cfg.get("expiry") or {})
            steps.append({"kind": "suggest", "auto": False, "symptoms": [f["symptom"]], "op": "expire_keep_files",
                          "params": {"older_than_ms": cut_ms, "retain_last": keep, "grace_h": float(grace)},
                          "statement": (f"expire snapshots older than {ts_text(cut_ms)}, retain_last {keep}, "
                                        f"files kept; freed files deleted after {float(grace):g} h"),
                          "note": f"{f['action']}: {f['remedy']}"})
            continue
        elif f["symptom"] == "METADATA_BLOAT":
            props = json.loads(tm.get("properties_json") or "{}")
            if str(props.get("write.metadata.delete-after-commit.enabled", "false")).lower() != "true":
                stmt = (f"ALTER TABLE {table} SET TBLPROPERTIES "
                        f"('write.metadata.delete-after-commit.enabled' = 'true')")
        elif f["symptom"] == "STALE_REF":
            stmt = "; ".join(f"ALTER TABLE {table} DROP {r.get('type', 'tag').upper()} {r['name']}"
                             for r in ev.get("refs", []))
        elif f["symptom"] == "UNBOUNDED_RETENTION":
            stmt = (f"ALTER TABLE {table} SET TBLPROPERTIES "
                    f"('write.metadata.delete-after-commit.enabled' = 'true')")
        elif f["symptom"] == "ORPHAN_FILES":
            waiting = (freed or {}).get("waiting", 0) + (freed or {}).get("due", 0)
            if waiting:
                # a freed file may be months old: orphan removal (age by creation time)
                # would delete it before its grace is over
                steps.append({"kind": "suggest", "auto": False, "symptoms": [f["symptom"]], "statement": "",
                              "note": (f"held: {waiting} files freed by an expiry are waiting out their grace; "
                                       f"orphan removal runs once none are left")})
                continue
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


# ---------- run order (design decision D1) ----------

# approved suggestions by what they change: configuration first, rewrites with
# the data passes, expiry and orphans at the end
APPROVED_PHASE = {"MIXED_SPEC": 3, "POOR_CLUSTERING": 3, "OVER_PARTITIONED": 3,
                  "RETAINED_STORAGE": 7, "ORPHAN_FILES": 9}


def phase_of(step):
    """The D1 step a planned step belongs to (1-4 before the ledger catch-up,
    7-9 after it); 0 for holds, which never run."""
    kind = step["kind"]
    if kind == "hold":
        return 0
    if kind == "advice":
        return 7          # shown with the expiry steps; never runs
    if kind == "rewrite_position_delete_files":
        return 2 if "DELETE_FILE_SPRAWL" in step["symptoms"] else 3   # else: cleanup after the rewrite
    if kind == "rewrite_data_files":
        return 3
    if kind == "rewrite_manifests":
        return 4
    if kind == "expire_keep_files":
        return 7
    if kind == "delete_freed_files":
        return 8
    if kind == "suggest":
        planned = step.get("planned_kind")       # advisor.mode turned an auto step into a suggestion
        if planned:
            return phase_of(dict(step, kind=planned))
        return max([APPROVED_PHASE.get(x, 1) for x in step["symptoms"]] or [1])
    return 1


def in_order(steps, phases):
    """The steps of the given phases, in run order (stable within a phase)."""
    return sorted((s for s in steps if phase_of(s) in phases), key=phase_of)


def cap_expiry(step, ledger_ms):
    """Expiry never passes what the ledger ingested: older_than is capped at
    the newest ingested snapshot's time (exclusive, so that snapshot stays).
    -> the step (a copy when changed). ledger_ms None: ledger unknown, no cap."""
    if step.get("op") != "expire_keep_files" or ledger_ms is None:
        return step
    p = dict(step.get("params") or {})
    if int(p["older_than_ms"]) <= int(ledger_ms):
        return step
    p["older_than_ms"] = int(ledger_ms)
    return dict(step, params=p,
                statement=step["statement"].split(", retain_last")[0].rsplit("older than", 1)[0]
                + f"older than {ts_text(int(ledger_ms))}, retain_last" + step["statement"].split(", retain_last", 1)[1],
                note=step["note"] + f"; capped at the newest snapshot the ledger ingested ({ts_text(int(ledger_ms))})")


def savings_estimate(before, after, results, cost):
    """Step 10: what this run changed, from the table's metrics at its scan and
    at the catch-up, with a request-cost estimate when config cost.* is set.
    before / after: table_metrics rows; results: [(kind, status, result_json)].
    Fewer files means fewer object reads (GET requests) for every full read of
    the table; storage comes back only after expiry and the file grace, so it
    is reported as files freed, not as money (sizes are not recorded yet)."""
    def files(tm):
        return int((tm or {}).get("data_files") or 0) + int((tm or {}).get("delete_files") or 0)
    freed = 0
    for kind, status, result in results:
        if kind == "expire_keep_files" and status == "ok":
            try:
                freed += int(json.loads(result).get("freed") or 0)
            except (ValueError, AttributeError):
                pass
    out = {"files_before": files(before), "files_after": files(after),
           "files_removed": max(0, files(before) - files(after)),
           "metadata_json_bytes_before": (before or {}).get("metadata_json_bytes"),
           "metadata_json_bytes_after": (after or {}).get("metadata_json_bytes"),
           "files_freed_for_deletion": freed}
    cost = cost or {}
    scans = float(cost.get("full_reads_per_day") or 0)
    get_usd = float(cost.get("get_usd_per_1000") or 0)
    if scans and get_usd:
        req = out["files_removed"] * scans * 30
        out["requests_saved_per_month"] = int(req)
        out["usd_saved_per_month_requests"] = round(req / 1000.0 * get_usd, 4)
        out["assumption"] = f"{scans:g} full reads a day at {get_usd:g} USD per 1,000 GET requests"
    return out


def savings_text(e):
    t = (f"files {e['files_before']} -> {e['files_after']} ({e['files_removed']} fewer)"
         f"; {e['files_freed_for_deletion']} files freed, deleted after the grace")
    if e.get("usd_saved_per_month_requests") is not None:
        t += (f"; ~{e['requests_saved_per_month']:,} fewer GET requests a month"
              f" (~{e['usd_saved_per_month_requests']:g} USD; {e['assumption']})")
    return t


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


def run_op(spark, step, table, uuid, store, run_id):
    """A step with an "op" (freed_files.py). -> (status, result_json_or_error, seconds)."""
    import freed_files as ff
    t0 = time.perf_counter()
    try:
        p = step.get("params") or {}
        if step["op"] == "expire_keep_files":
            out = ff.expire_keep_files(spark, table, uuid, p["older_than_ms"], p["retain_last"], p["grace_h"],
                                       store, run_id)
        elif step["op"] == "delete_freed_files":
            out = ff.delete_due(spark, table, uuid, store)
        else:
            raise ValueError(f"unknown op {step['op']}")
        status, result = "ok", json.dumps(out, default=str)
    except Exception as e:  # record and carry on with the next table
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
    from pyspark.sql import SparkSession

    p = argparse.ArgumentParser()
    p.add_argument("--tables", default="", help="comma-separated, e.g. s0,s2 (default: all in the scan)")
    p.add_argument("--scan-id", default=None)
    p.add_argument("--apply", action="store_true", help="run the auto statements")
    p.add_argument("--approve", default="",
                   help="comma-separated symptoms whose suggested (approval) statements to run, "
                        "e.g. MIXED_SPEC,ORPHAN_FILES; recorded in glue.ops.actions as approved")
    p.add_argument("--config", default=os.path.join(HERE, "config", "health.json"))
    p.add_argument("--profile", default="", help="profile JSON whose coordination backend holds the "
                                                "table leases (default config/profile.json)")
    a = p.parse_args()

    config = gl.load_config(a.config)
    spark = SparkSession.builder.appName("gl25-plan").getOrCreate()
    ns = gl.OPS_NAMESPACE
    window_h = int(float((config.get("incremental") or {}).get("previous_scan_window_hours", 168)))
    import freed_files as ff
    import scan_state
    import state_store as ss
    log = ss.make_log_sink(spark, config, ns)
    store = ss.make_state_store(spark, config, ns)
    if a.scan_id:
        tms = {r["table_name"]: r for r in log.rows("table_metrics", eq={"scan_id": a.scan_id})}
    else:
        # each table's own latest scan (group runs scan only their tables, so the newest
        # scan of the whole catalog does not cover every table): its facts in table_state
        scan_state.ensure_tables(spark, ns, store)
        tms = scan_state.latest_facts(store, int(time.time() * 1000), window_h)
    if not tms:
        sys.exit("No scan found: run make gl-scan first.")
    pairs = {(t, m["scan_id"]) for t, m in tms.items()}
    findings = {}
    for r in log.rows("symptoms", eq={"scan_id": sorted({sid for _, sid in pairs})}):
        if (r["table_name"], r["scan_id"]) in pairs:
            findings.setdefault(r["table_name"], []).append(r)
    scan_id = a.scan_id or (next(iter(pairs))[1] if len({s for _, s in pairs}) == 1 else "per-table latest")

    tables = match_tables(sorted(tms), [t.strip() for t in a.tables.split(",") if t.strip()])
    run_id = gl.new_run_id("plan")
    approve = {x.strip().upper() for x in a.approve.split(",") if x.strip()}
    mode = " + ".join(m for m in ("APPLY" if a.apply else "", f"APPROVE {sorted(approve)}" if approve else "")
                      if m) or "dry run"
    print(f"=== Plan {run_id} from scan {scan_id} ({mode}) ===", flush=True)

    if getattr(store, "iceberg", True):
        ff.ensure_table(spark, ns)
    store.preload(ff.KIND)
    if not (a.apply or approve):
        for t in tables:
            t_uuid = tms[t].get("table_uuid")
            freed = ff.summary(store.range(ff.KIND, (t_uuid,)), ff.now_ms()) if t_uuid else None
            print_steps(t, plan_table(t, findings.get(t, []), tms[t], gl.table_config(config, t), freed=freed))
        print("\nDry run: nothing changed. APPLY=1 runs the RUN steps; APPROVE=<SYMPTOM,...> runs "
              "the ASK steps for those symptoms. Either one scans each table again first and runs "
              "the steps in the D1 order (see plan.py).", flush=True)
        spark.stop()
        return

    import coordinator as coordination
    import profiles
    path = a.profile or os.path.join(HERE, "config", "profile.json")
    profile = profiles.load_profile(path) if os.path.exists(path) else None
    coord = coordination.make(profile, run_id)
    ttl = int(float((config.get("defaults") or {}).get("lease_minutes", 30)) * 60)
    total = estimates = 0
    for t in tables:
        recs = run_table(spark, t, config, coord, ttl, run_id, a.apply, approve, store, log)
        if recs:
            log.append("actions", recs)
            log.flush()                  # the log first, then the state the next scan reads
            scan_state.ensure_tables(spark, ns, store)
            scan_state.record_actions(store, [r for r in recs if r["kind"] != "estimate"],
                                      datetime.now(timezone.utc))
            store.flush()
            n_est = sum(1 for r in recs if r["kind"] == "estimate")
            total += len(recs) - n_est
            estimates += n_est
    print(f"\nRecorded {total} action(s) and {estimates} estimate(s) in the actions log under {run_id} "
          f"(actions with snapshot before/after and a rollback statement each; one estimate per table that "
          f"changed).", flush=True)
    spark.stop()


def print_steps(t, steps, title=None):
    print(f"\n--- {t}{(': ' + title) if title else ''}: {'nothing to do' if not steps else ''}", flush=True)
    for s in steps:
        tag = "RUN " if s["auto"] else {"hold": "HOLD", "advice": "NOTE"}.get(s["kind"], "ASK ")
        print(f"  [{tag}] {s['kind']:18} {', '.join(s['symptoms'])}\n         {s['note']}", flush=True)
        if s["statement"]:
            print(f"         {s['statement']}", flush=True)


def _scanned(spark, scan_id, table, config, findings):
    """(the table's metrics row, its findings) from a scan and detection in this job."""
    import scan_metrics as sm
    tms, _ = sm.scan_results(spark, scan_id, None, config)
    tm = next((dict(r) for r in tms if r["table_name"] == table), None)
    return tm, [f for f in findings if f["table_name"] == table]


def run_table(spark, t, config, coord, ttl, run_id, apply, approve, store, log):
    """One table through the D1 run order under its lease. -> action records."""
    import freed_files as ff
    import gl_common as gl
    import ledger as ledger_mod
    import state_store as ss
    from detect_symptoms import run_detect
    from scan_metrics import run_scan

    cfg_t = gl.table_config(config, t)
    ns, short = t.rsplit(".", 1)
    recs, results = [], []
    if not coord.claim_table(t, "run", ttl):
        print(f"\n--- {t}: another run holds its lease; skipped", flush=True)
        return recs

    def renew():
        if coord.claim_table(t, "run", ttl):
            return True
        print(f"  lease lost (another run took the table over): the rest of {t} is skipped", flush=True)
        return False

    try:
        # scan: the decisions are as fresh as the run
        sid = run_scan(spark, ns, config, tables=[short], report=False, housekeeping=False)
        before, found = _scanned(spark, sid, t, config, run_detect(spark, sid, config, report=False))
        if not before or before.get("scan_mode") == "failed":
            print(f"\n--- {t}: scan failed; nothing runs", flush=True)
            return recs
        if advisor_mode(json.loads(before.get("properties_json") or "{}"), cfg_t) == "off":
            print(f"\n--- {t}: advisor.mode=off: skipped", flush=True)
            return recs
        uuid = before.get("table_uuid")
        freed = ff.summary(store.range(ff.KIND, (uuid,)), ff.now_ms()) if uuid else None
        steps = plan_table(t, found, before, cfg_t, freed=freed)
        print_steps(t, in_order(steps, (0, 1, 2, 3, 4)), "steps 1-4 (scan " + sid + ")")
        kw = dict(apply=apply, approve=approve, cfg_t=cfg_t, uuid=uuid, store=store, run_id=run_id,
                  renew=renew, recs=recs, results=results)
        if not exec_steps(spark, t, in_order(steps, (1, 2, 3, 4)), scan_id=sid, **kw):
            return recs
        if not renew():
            return recs
        # 5: ledger catch-up, the writer's commits and the advisor's own
        sid2 = run_scan(spark, ns, config, tables=[short], report=False, housekeeping=False)
        after, found2 = _scanned(spark, sid2, t, config, run_detect(spark, sid2, config, report=False))
        ledger_ms = None
        if uuid and ledger_mod.mode_of(config) == "on":
            st = ss.make_state_store(spark, config).get("ledger_state", (uuid,))
            ledger_ms = int(st["last_ts_ms"]) if st and st.get("last_ts_ms") is not None else None
        print(f"  catch-up (scan {sid2}): ledger at "
              f"{ts_text(ledger_ms) if ledger_ms is not None else 'unknown (ledger off): expiry not capped'}",
              flush=True)
        freed2 = ff.summary(store.range(ff.KIND, (uuid,)), ff.now_ms()) if uuid else None
        late = [cap_expiry(s, ledger_ms) for s in
                in_order(plan_table(t, found2, after or before, cfg_t, freed=freed2), (7, 8, 9))]
        print_steps(t, late, "steps 7-9 after the catch-up")
        exec_steps(spark, t, late, scan_id=sid2, **kw)
        # 10: what the run changed
        if results:
            est = savings_estimate(before, after, results, cfg_t.get("cost"))
            print(f"  estimate: {savings_text(est)}", flush=True)
            recs.append(dict(run_id=run_id, scan_id=sid2, started_at=datetime.now(timezone.utc), table_name=t,
                             table_uuid=uuid, kind="estimate", symptoms="", statement="savings estimate (step 10)",
                             status="ok", duration_s=0.0, result_json=json.dumps(est), snapshot_before=None,
                             snapshot_after=None, rollback_hint=None))
    finally:
        coord.release_table(t, "run")
    return recs


def exec_steps(spark, t, steps, *, apply, approve, cfg_t, uuid, scan_id, store, run_id, renew, recs, results):
    """Run the steps in order; record each. False when one failed or the lease
    was lost (the rest of the table's steps are skipped)."""
    ident_t = _ident(t)
    for s in steps:
        approved = (not s["auto"] and s["kind"] == "suggest" and s["statement"]
                    and approve & set(s["symptoms"]))
        if not ((s["auto"] and apply) or approved):
            continue
        if not renew():
            return False
        now = datetime.now(timezone.utc)
        cutoff = now.timestamp() - float(cfg_t.get("orphan_min_age_minutes", 4320)) * 60
        cutoff_txt = datetime.fromtimestamp(cutoff, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') + "+00:00"
        text = (s["statement"]
                .replace("{now}", f"TIMESTAMP '{now.strftime('%Y-%m-%d %H:%M:%S')}+00:00'")
                .replace("{orphan_cutoff}", f"TIMESTAMP '{cutoff_txt}'"))
        # The +00:00 matters: a bare TIMESTAMP literal is read in the Spark session's
        # time zone; off UTC, the orphan cutoff would move (later = younger files deleted).
        kind = s["kind"] if s["auto"] else "approved:" + ",".join(s["symptoms"])

        def record(stmt, status, result, dur, before, after, hint):
            print(f"  -> {kind}: {status} in {dur}s  snapshot {before} -> {after}  {result[:300]}", flush=True)
            recs.append(dict(run_id=run_id, scan_id=scan_id, started_at=now, table_name=t, table_uuid=uuid,
                             kind=kind, symptoms=",".join(s["symptoms"]), statement=stmt, status=status,
                             duration_s=float(dur), result_json=result, snapshot_before=before,
                             snapshot_after=after, rollback_hint=hint))
            results.append((s.get("op") or s["kind"], status, result))

        if s.get("op"):
            # Python actions (freed_files.py): expiry that keeps files, deferred deletion
            before = current_snapshot(spark, t)
            status, result, dur = run_op(spark, s, t, uuid, store, run_id)
            record(s["statement"], status, result, dur, before, current_snapshot(spark, t), None)
            if status != "ok":
                return False
            continue
        status = "ok"
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
            if status == "failed" and DANGLING in stmt and _unsupported_option(result):
                # Older Iceberg: retry without the option (the delete-file cleanup
                # that follows still removes the deletes).
                stmt = strip_dangling_option(stmt)
                status, result, dur = run_sql(spark, stmt)
            after = current_snapshot(spark, t)
            record(stmt, status, result, dur, before, after,
                   rollback_hint(ident_t, before, after) if status == "ok" else None)
            if status != "ok":
                return False
    return True

if __name__ == "__main__":
    main()
