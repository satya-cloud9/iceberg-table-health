"""GL2.5c symptom rules: turn one table's metrics into named findings.

Pure Python (no Spark), so the rules can be unit-tested against plain dicts.
Input is what scan_metrics.py stores: one table_metrics row and that table's
partition_metrics rows (as dicts). Output is a list of findings, one per
symptom (per partition for partition-level symptoms).

Every threshold comes from config/health.json ("thresholds", overridable per
table). Detection never knows which scenario built a table.

Each finding carries an action:
  auto           safe maintenance a scheduler may run unattended
  approval       changes layout, spec or properties; a human signs off
  defer          would be auto, but a writer changed files in the partition moments ago
  needs-evidence workload-dependent; held until query evidence reaches the
                 configured level (declared filter columns are not enough)
  advisory       reported, never acted on: advice for the owner, where nothing
                 the advisor could run would change it (FREQUENT_FULL_REFRESH;
                 RETAINED_STORAGE with no snapshot past the policy, with the numbers)
"""
import json
import math
import re

import gltrace as tr
import holds as hl

RULE_VERSION = "t1-1"

# symptom -> (category, level, action, remedy)
CATALOG = {
    "SMALL_FILES":           ("file_layout", "partition", "auto",
                              "rewrite_data_files (binpack) scoped to this partition"),
    "HOT_PARTITION":         ("file_layout", "partition", "defer",
                              "wait: a writer changed files in this partition moments ago (MERGE, UPDATE, "
                              "DELETE or INSERT OVERWRITE); compact once the conflict window has passed"),
    "FREQUENT_FULL_REFRESH": ("write_pattern", "table", "advisory",
                              "the table is replaced (nearly) whole again and again: consider an incremental "
                              "load; if deliberate, acknowledge with the table property "
                              "advisor.ack = FREQUENT_FULL_REFRESH"),
    "LATE_ARRIVALS":         ("write_pattern", "table", "approval",
                              "data keeps landing long after its partition ended: batch late rows, narrow "
                              "the incremental lookback, stage late data and merge once, or partition by "
                              "ingestion date and sort by event time"),
    "DELETE_BUILDUP":        ("file_layout", "partition", "auto",
                              "rewrite_data_files with delete-file-threshold, then "
                              "rewrite_position_delete_files"),
    "OVERSIZED_FILES":       ("file_layout", "partition", "auto",
                              "rewrite_data_files to split files to the target size"),
    "SCATTERED_SMALL_FILES": ("file_layout", "table", "auto",
                              "rank partitions by excess files and compact the top N per run; "
                              "upstream: batch late rows or narrow the incremental lookback"),
    "SNAPSHOT_BUILDUP":      ("metadata", "table", "auto",
                              "expire_snapshots (retain_last / older_than per policy)"),
    "DELETE_FILE_SPRAWL":    ("file_layout", "partition", "auto",
                              "rewrite_position_delete_files scoped to this partition: merges many small "
                              "position delete files into few; no data file is rewritten"),
    "RETAINED_STORAGE":      ("storage", "table", "approval",
                              "old snapshots keep a large share of the table alive: expire to the retention "
                              "policy now; if it stays high inside the policy, shorten the policy "
                              "(history.expire.max-snapshot-age-ms)"),
    "METADATA_BLOAT":        ("metadata", "table", "approval",
                              "metadata is large: shorten the snapshot retention policy or commit less "
                              "often upstream (every commit rewrites metadata.json and adds a manifest list)"),
    "STALE_REF":             ("metadata", "table", "approval",
                              "a tag or branch other than main points at an old snapshot and keeps its files "
                              "alive (expire_snapshots never removes what a ref points at): drop it, or give it "
                              "a retention (ALTER TABLE ... CREATE OR REPLACE TAG ... RETAIN n DAYS)"),
    "SNAPSHOT_RATE":         ("write_pattern", "table", "approval",
                              "the writer commits very often and each commit is small: every commit adds a "
                              "snapshot, a manifest list and small files; commit less often upstream "
                              "(streaming trigger, Flink checkpoint interval, batch schedule)"),
    "HISTORY_LOST":          ("operations", "table", "approval",
                              "commits were expired before the advisor saw them: keep the table's history "
                              "longer than the time between scans, or scan more often"),
    "MANIFEST_BLOAT":        ("metadata", "table", "auto",
                              "rewrite_manifests; re-enable commit.manifest-merge.enabled"),
    "REWRITE_CHURN":         ("write_config", "table", "approval",
                              "switch MERGE/UPDATE/DELETE to merge-on-read and compact on a schedule; "
                              "or narrow each merge (partition predicate, dbt incremental_predicates); "
                              "or sort on the merge key so changed rows sit in few files"),
    "UNBOUNDED_RETENTION":   ("write_config", "table", "approval",
                              "set write.metadata.delete-after-commit.enabled=true so metadata.json "
                              "versions that fall off the log are deleted; remove_orphan_files for the "
                              "ones already left behind"),
    "MAINTENANCE_LAG":       ("operations", "table", "approval",
                              "maintenance is not keeping up: run plans more often or raise "
                              "max_partitions_per_run; check what changed in the writer"),
    "MAINTENANCE_FAILING":   ("operations", "table", "approval",
                              "the same maintenance step keeps failing: read the error in "
                              "glue.ops.actions and fix the cause before the next run"),
    "ORPHAN_FILES":          ("storage", "table", "approval",
                              "remove_orphan_files older than the safety age (dry run first)"),
    "PARTITION_SKEW":        ("partition_design", "table", "approval",
                              "split the heavy key: add bucket() or a finer transform; "
                              "compact the large partition with a higher parallelism"),
    "OVER_PARTITIONED":      ("partition_design", "table", "approval",
                              "evolve to a coarser partition spec, then rewrite to the new spec"),
    "MIXED_SPEC":            ("partition_design", "table", "approval",
                              "rewrite_data_files on files written with older specs"),
    "LEGACY_FORMAT":         ("write_config", "table", "approval",
                              "upgrade format-version to 2"),
    "METRICS_DISABLED":      ("data_clustering", "table", "approval",
                              "enable column metrics (write.metadata.metrics.*) for filter columns"),
    "POOR_CLUSTERING":       ("data_clustering", "table", "needs-evidence",
                              "set a sort order on the filter column, then sort rewrite"),
}

EVIDENCE_RANK = {"heuristic": 0, "declared": 1, "inferred": 2, "observed": 3}


def _num(v, default=0):
    return default if v is None else v


def _severity(score):
    if score >= 5:
        return "high"
    if score >= 2:
        return "medium"
    return "low"


def _finding(table, symptom, score, evidence, partition_key=None, remedy=None,
             evidence_level="metadata"):
    category, level, action, default_remedy = CATALOG[symptom]
    score = round(float(score), 2)
    return {
        "table_name": table,
        "partition_key": partition_key,
        "symptom": symptom,
        "category": category,
        "level": level,
        "action": action,
        "automatic": action == "auto",
        "score": score,
        "severity": _severity(score),
        "remedy": remedy or default_remedy,
        "evidence_level": evidence_level,
        "evidence_json": json.dumps(evidence, sort_keys=True, default=str),
        "rule_version": RULE_VERSION,
    }


def _props(tm):
    try:
        return json.loads(tm.get("properties_json") or "{}")
    except ValueError:
        return {}


def coarser_spec_hint(spec):
    s = (spec or "").lower()
    if "hour" in s:
        return "evolve hours() to days()"
    if "day" in s:
        return "evolve days() to months()"
    if "month" in s:
        return "evolve months() to years()"
    if "bucket" in s:
        m = re.search(r"bucket\[(\d+)\]", s)
        return f"reduce the bucket count (now {m.group(1)})" if m else "reduce the bucket count"
    return "drop or coarsen the partition transform"


def partition_findings(table, rows, th, hot_minutes, new_findings=False):
    """Partition symptoms. The only hold is the writer-conflict hold: a writer
    commit that removed data files or added or removed delete files in the
    partition within hot_minutes (minutes_since_conflict, from holds.holds_rows)
    turns small files, deletes or sprawl into HOT_PARTITION (defer). Appends never
    hold a partition, whether its time range has ended or not, and late data does
    not hold it either (the settle window, SETTLING and the still-filling hold
    were retired 2026-10-09; the tier rule decides minor or major passes, item 12).
    A partition writers rewrite fast (rewrite_rate at or above
    churn_partition_rate) keeps its SMALL_FILES auto, with churn advice to the
    owner added to the finding: a note, not a hold."""
    out = []
    min_excess = th["min_excess_files"]
    for r in rows:
        key = r.get("partition_key")
        excess = _num(r.get("excess_files"))
        data_files = _num(r.get("data_files"))
        records = _num(r.get("records"))
        del_files = _num(r.get("delete_files_pos")) + _num(r.get("delete_files_eq"))
        del_records = _num(r.get("delete_records"))
        del_ratio = del_records / records if records else 0.0
        minutes = r.get("minutes_since_update")
        hot, hot_reason = hl.decide(r, hot_minutes)

        small = excess >= min_excess
        eq_files = _num(r.get("delete_files_eq"))
        # Position deletes: judged by the share of rows deleted. Equality
        # deletes: one record can delete many rows and every eq-delete file is
        # checked against every older data file on read, so their count matters.
        pos_deletes = del_files >= th["delete_min_files"] and del_ratio >= th["delete_ratio"]
        eq_deletes = eq_files >= th.get("eq_delete_min_files", 5)
        deletes = pos_deletes or eq_deletes
        # GL2.6c DELETE_FILE_SPRAWL: many position delete files with few rows
        # deleted (DELETE_BUILDUP's ratio isn't reached). Readers open every one
        # of them; merging them is cheap and rewrites no data.
        pos_files = _num(r.get("delete_files_pos"))
        sprawl = new_findings and not deletes and pos_files >= th.get("delete_sprawl_min_files", 10)
        ev = {"data_files": data_files, "excess_files": excess,
              "small_files": _num(r.get("small_files")), "ideal_files": _num(r.get("ideal_files")),
              "delete_files": del_files, "delete_files_eq": eq_files, "delete_ratio": round(del_ratio, 4),
              "minutes_since_update": None if minutes is None else round(minutes, 1),
              "hot_reason": hot_reason, "rewrite_rate": r.get("rewrite_rate"),
              "minutes_since_conflict": (None if r.get("minutes_since_conflict") is None
                                         else round(r["minutes_since_conflict"], 1))}
        if not hot and minutes is not None and minutes < hot_minutes:
            # appends are still landing: no hold, but an unpartitioned table with a
            # time_column is rewritten only below the newest rows (plan.py)
            ev.update(appends_continue=True, compact_older_than_min=hot_minutes)

        tr.log("rule.partition", key, excess=f"{excess} vs min {min_excess} -> small={small}",
               deletes=f"pos+eq files {del_files}, ratio {del_ratio:.4f} vs {th['delete_ratio']}, eq {eq_files} vs "
                       f"{th.get('eq_delete_min_files', 5)} -> {deletes}",
               sprawl=(f"pos files {pos_files} vs {th.get('delete_sprawl_min_files', 10)} -> {sprawl}"
                       if new_findings else "off"),
               hot=f"{hot_reason or 'no hold'}", rewrite_rate=r.get("rewrite_rate"),
               hours_since_end=None if r.get("hours_since_end") is None else round(r["hours_since_end"], 1))
        if (small or deletes or sprawl) and hot:
            remedy = f"wait: {hot_reason}; compact once {hot_minutes:g} min have passed without another"
            out.append(_finding(table, "HOT_PARTITION", max(excess / min_excess, 1.0), ev, key, remedy=remedy))
            continue
        if small:
            remedy = None
            if r.get("last_write_label") == "possible_full_refresh":
                remedy = ("written by a possible full refresh: set the refresh job's target file size or "
                          "distribution mode, otherwise every refresh needs this compaction again; "
                          + CATALOG["SMALL_FILES"][3])
            f = _finding(table, "SMALL_FILES", excess / min_excess, ev, key, remedy=remedy)
            rate = r.get("rewrite_rate")
            limit = th.get("churn_partition_rate", 1.0)
            if rate is not None and rate >= limit:
                # churn is a note, not a hold: compact as usual, tell the owner
                f["remedy"] = (f"{f['remedy']}; churn advice for the owner: writers rewrote {rate:g}x this "
                               f"partition's bytes in the churn window (>= {limit:g}), so much of any compaction "
                               f"is rewritten again within hours: merge-on-read, or fewer, larger rewrites")
            out.append(f)
        if deletes:
            out.append(_finding(table, "DELETE_BUILDUP",
                                max(del_ratio / th["delete_ratio"],
                                    eq_files / th.get("eq_delete_min_files", 5)), ev, key))
        if sprawl:
            out.append(_finding(table, "DELETE_FILE_SPRAWL", pos_files / th.get("delete_sprawl_min_files", 10),
                                dict(ev, delete_files_pos=pos_files), key))
        if _num(r.get("oversized_files")) >= th["min_oversized_files"]:
            out.append(_finding(table, "OVERSIZED_FILES",
                                _num(r.get("oversized_files")) / th["min_oversized_files"], ev, key))
    return out


def table_findings(table, tm, rows, th, cfg):
    out = []
    props = _props(tm)
    target = _num(tm.get("target_file_bytes"), 1) or 1
    parts = _num(tm.get("partitions"))

    # SCATTERED_SMALL_FILES: many partitions fragmented enough to compact, and
    # the writer touches several partitions per commit (late-arrival
    # fingerprint). Counts only partitions SMALL_FILES would flag, so the
    # finding clears once the planner has worked through them.
    with_excess = sum(1 for r in rows if _num(r.get("excess_files")) >= th["min_excess_files"])
    share = with_excess / parts if parts else 0.0
    avg_parts = _num(tm.get("avg_changed_partitions_per_commit"))
    tr.log("rule.SCATTERED_SMALL_FILES", partitions_to_compact=f"{with_excess} vs {th['scattered_min_partitions']}",
           share=f"{share:.3f} vs {th['scattered_partition_share']}",
           avg_changed_partitions=f"{avg_parts} vs {th['scattered_avg_changed_partitions']}")
    if (with_excess >= th["scattered_min_partitions"] and share >= th["scattered_partition_share"]
            and avg_parts >= th["scattered_avg_changed_partitions"]):
        out.append(_finding(table, "SCATTERED_SMALL_FILES", share / th["scattered_partition_share"],
                            {"partitions_to_compact": with_excess, "partitions": parts,
                             "share": round(share, 3), "avg_changed_partitions_per_commit": avg_parts}))

    # SNAPSHOT_BUILDUP: too many, or too old, snapshots kept.
    snaps = _num(tm.get("snapshots"))
    age_h = _num(tm.get("oldest_snapshot_age_h"), 0.0)
    tr.log("rule.SNAPSHOT_BUILDUP", snapshots=f"{snaps} vs {th['max_snapshots']}",
           oldest_h=f"{age_h} vs {th['max_snapshot_age_h']}")
    if snaps > th["max_snapshots"] or age_h > th["max_snapshot_age_h"]:
        score = max(snaps / th["max_snapshots"], age_h / th["max_snapshot_age_h"])
        out.append(_finding(table, "SNAPSHOT_BUILDUP", score,
                            {"snapshots": snaps, "oldest_snapshot_age_h": age_h,
                             "retained_bytes": tm.get("retained_bytes")}))

    # MANIFEST_BLOAT: with merging on, Iceberg merges by itself once the count
    # reaches commit.manifest.min-count-to-merge, so only flag past that. With
    # merging off, nothing ever merges, so a lower limit applies.
    manifests = _num(tm.get("data_manifests"))
    merge_on = str(props.get("commit.manifest-merge.enabled", "true")).lower() != "false"
    limit = (int(props.get("commit.manifest.min-count-to-merge", th["manifest_merge_min_count"]))
             if merge_on else th["max_manifests_no_merge"])
    avg_mb = _num(tm.get("avg_manifest_bytes"), 0)
    tr.log("rule.MANIFEST_BLOAT", data_manifests=f"{manifests} vs {limit}", merge_on=merge_on,
           avg_manifest_bytes=f"{avg_mb} vs < {th['small_manifest_bytes']}")
    if manifests >= limit and avg_mb < th["small_manifest_bytes"]:
        out.append(_finding(table, "MANIFEST_BLOAT", manifests / limit,
                            {"data_manifests": manifests, "limit": limit,
                             "manifest_merge_enabled": merge_on, "avg_manifest_bytes": avg_mb}))

    # REWRITE_CHURN: overwrite commits keep rewriting a large share of the
    # table (copy-on-write merges touching most files, frequent full
    # refreshes). Files look healthy; the cost is in the writes.
    n_ow = _num(tm.get("overwrite_commits_recent"))
    share_ow = _num(tm.get("avg_overwrite_rewrite_share"), 0.0)
    turnover = _num(tm.get("table_turnover_24h"), 0.0)
    tr.log("rule.REWRITE_CHURN", overwrite_commits=f"{n_ow} vs {th.get('churn_min_commits', 5)}",
           rewrite_share=f"{share_ow} vs {th.get('churn_rewrite_share', 0.3)}",
           turnover_24h=f"{turnover} vs {th.get('churn_turnover_24h', 2.0)}")
    if (n_ow >= th.get("churn_min_commits", 5) and share_ow >= th.get("churn_rewrite_share", 0.3)
            and turnover >= th.get("churn_turnover_24h", 2.0)):
        mode = (tm.get("write_merge_mode") or "copy-on-write (default)")
        out.append(_finding(table, "REWRITE_CHURN", turnover / th.get("churn_turnover_24h", 2.0),
                            {"overwrite_commits": n_ow, "avg_rewrite_share": share_ow,
                             "table_turnover_24h": turnover, "write_merge_mode": mode,
                             "rewritten_bytes_24h": tm.get("rewritten_bytes_24h")}))

    # UNBOUNDED_RETENTION: Iceberg keeps at most previous-versions-max entries
    # in the metadata log, so the log never grows past it. Once it is full,
    # every commit pushes the oldest metadata.json off the log; without
    # delete-after-commit that file stays in storage for good.
    versions = _num(tm.get("metadata_versions"))
    max_versions = int(props.get("write.metadata.previous-versions-max", th["max_metadata_versions"]))
    auto_delete = str(props.get("write.metadata.delete-after-commit.enabled", "false")).lower() == "true"
    tr.log("rule.UNBOUNDED_RETENTION", metadata_versions=f"{versions} vs {max_versions}", delete_after_commit=auto_delete)
    if versions >= max_versions and not auto_delete:
        out.append(_finding(table, "UNBOUNDED_RETENTION", 1 + versions / max(1, max_versions),
                            {"metadata_versions": versions, "previous_versions_max": max_versions,
                             "delete_after_commit": auto_delete,
                             "metadata_json_files": tm.get("metadata_json_files")}))

    # ORPHAN_FILES: objects under the table location nothing references.
    orphans = _num(tm.get("orphan_files"))
    tr.log("rule.ORPHAN_FILES", orphan_files=f"{orphans} vs {th.get('min_orphan_files', 1)}")
    if orphans >= th.get("min_orphan_files", 1):
        out.append(_finding(table, "ORPHAN_FILES", 1 + orphans / 10,
                            {"orphan_files": orphans, "orphan_bytes": tm.get("orphan_bytes"),
                             "listed_objects": tm.get("listed_objects"),
                             "sample": tm.get("orphan_sample")}))

    # PARTITION_SKEW: one partition far above the median, and big in absolute
    # terms (a skewed table of tiny partitions is not worth acting on).
    skew = _num(tm.get("skew_ratio"), 0.0)
    largest = max((_num(r.get("data_bytes")) for r in rows), default=0)
    tr.log("rule.PARTITION_SKEW", partitions=f"{parts} vs {th['skew_min_partitions']}",
           skew=f"{skew} vs {th['skew_ratio']}", largest=f"{largest} vs {th['skew_min_largest_targets'] * target}")
    if (parts >= th["skew_min_partitions"] and skew >= th["skew_ratio"]
            and largest >= th["skew_min_largest_targets"] * target):
        out.append(_finding(table, "PARTITION_SKEW", skew / th["skew_ratio"],
                            {"skew_ratio": skew, "largest_partition_bytes": largest,
                             "top1pct_share": tm.get("top1pct_share"), "partitions": parts}))

    # OVER_PARTITIONED: most partitions are tiny and there are many of them.
    # Per-partition rules can't see this (each partition holds one file).
    under = _num(tm.get("undersized_partition_share"), 0.0)
    tr.log("rule.OVER_PARTITIONED", partitions=f"{parts} vs {th['over_partitioned_min_partitions']}",
           undersized_share=f"{under} vs {th['over_partitioned_share']}")
    if parts >= th["over_partitioned_min_partitions"] and under >= th["over_partitioned_share"]:
        ideal_unpartitioned = max(1, math.ceil(_num(tm.get("data_bytes")) / target))
        out.append(_finding(table, "OVER_PARTITIONED", parts / th["over_partitioned_min_partitions"],
                            {"partitions": parts, "undersized_partition_share": under,
                             "avg_file_bytes": tm.get("avg_file_bytes"),
                             "files_if_unpartitioned": ideal_unpartitioned,
                             "spec": tm.get("partition_spec")},
                            remedy=f"{coarser_spec_hint(tm.get('partition_spec'))}, "
                                   f"then rewrite to the new spec"))

    old_spec = sum(_num(r.get("files_old_spec")) for r in rows)
    tr.log("rule.MIXED_SPEC", files_old_spec=old_spec)
    if old_spec > 0:
        out.append(_finding(table, "MIXED_SPEC", 1 + old_spec / max(1, _num(tm.get("data_files"), 1)),
                            {"files_old_spec": old_spec, "current_spec_id": tm.get("current_spec_id")}))

    if _num(tm.get("format_version"), 2) == 1:
        out.append(_finding(table, "LEGACY_FORMAT", 1.0, {"format_version": 1}))

    # Column-level: METRICS_DISABLED (metadata fact) and POOR_CLUSTERING
    # (workload-dependent: held unless evidence reaches the configured level).
    try:
        pruning = json.loads(tm.get("pruning_json") or "{}")
    except ValueError:
        pruning = {}
    no_stats = [c for c, v in pruning.items() if v.get("status") == "no column stats"]
    if no_stats:
        out.append(_finding(table, "METRICS_DISABLED", len(no_stats), {"columns": no_stats}))

    level = cfg.get("filter_columns_evidence", "declared")
    need = cfg.get("workload_min_evidence", "observed")
    for col, v in sorted(pruning.items()):
        eff = v.get("efficiency")
        tr.log("rule.POOR_CLUSTERING", col, efficiency=f"{eff} vs {th['min_pruning_efficiency']}",
               status=v.get("status"), evidence=f"{level} vs needed {need}")
        if v.get("status") != "ok" or eff is None or eff >= th["min_pruning_efficiency"]:
            continue
        f = _finding(table, "POOR_CLUSTERING",
                     (th["min_pruning_efficiency"] - eff) / th["min_pruning_efficiency"] * 2 + 1,
                     {"column": col, "pruning_efficiency": eff,
                      "sort_order_defined": tm.get("sort_order_defined")},
                     remedy=(f"set a sort order on {col} and sort-rewrite"
                             if not tm.get("sort_order_defined") else
                             f"sort order exists but files are not clustered on {col}: sort-rewrite"),
                     evidence_level=level)
        f["action"] = "approval" if EVIDENCE_RANK.get(level, 0) >= EVIDENCE_RANK.get(need, 3) \
            else "needs-evidence"
        f["automatic"] = False
        out.append(f)
    return out


TREND_METRICS = {                 # metric -> floor below which growth isn't worth flagging
    "excess_files_total": "trend_floor_excess_files",
    "delete_files": "trend_floor_delete_files",
    "data_manifests": "trend_floor_manifests",
}


def trend_findings(table, history, actions, th):
    """Maintenance falling behind, from scan history and the actions log.

    history: this table's table_metrics rows (same table UUID), oldest first,
             ending with the current scan.
    actions: this table's glue.ops.actions rows, oldest first.
    """
    out = []
    n = int(th.get("trend_scans", 3))
    pts = history[-n:]
    if len(pts) == n:
        span_min = (pts[-1]["scanned_at"] - pts[0]["scanned_at"]).total_seconds() / 60
        if span_min >= th.get("trend_min_span_minutes", 0):
            for metric, floor_key in TREND_METRICS.items():
                vals = [p.get(metric) for p in pts]
                if any(v is None for v in vals):
                    continue
                rising = all(b > a for a, b in zip(vals, vals[1:]))
                grew = vals[-1] >= max(vals[0], 1) * (1 + th.get("trend_growth", 0.5))
                if rising and grew and vals[-1] >= th.get(floor_key, 10):
                    out.append(_finding(table, "MAINTENANCE_LAG", vals[-1] / max(vals[0], 1),
                                        {"metric": metric, "values": vals, "scans": n,
                                         "span_minutes": round(span_min, 1),
                                         "actions_in_span": sum(1 for a in actions
                                                                if a["started_at"] >= pts[0]["scanned_at"])}))
                    break
    streak = int(th.get("fail_streak", 2))
    by_kind = {}
    for a in actions:
        by_kind.setdefault(a["kind"], []).append(a)
    for kind, acts in sorted(by_kind.items()):
        last = acts[-streak:]
        if len(last) == streak and all(a["status"] == "failed" for a in last):
            out.append(_finding(table, "MAINTENANCE_FAILING", float(streak),
                                {"kind": kind, "failures_in_a_row": streak,
                                 "last_error": (last[-1].get("result_json") or "")[:300]}))
    return out


def late_arrival_findings(table, tm, th, min_batches=20):
    """GL2.5o: a long lateness tail or partitions reopened again and again is an
    upstream problem in its own right (the writer or the partition key). The
    lateness trigger needs min_batches batches (a p99 of 4 batches is just the
    worst of 4); the reopen trigger is a count and needs none."""
    p99, reopened = tm.get("lateness_p99_h"), _num(tm.get("reopened_partitions"))
    n = _num(tm.get("lateness_p99_batches"))
    long_wait = p99 is not None and n >= min_batches and float(p99) >= th.get("late_arrivals_hours", 24)
    reopens = reopened >= th.get("late_reopened_partitions", 3)
    tr.log("rule.LATE_ARRIVALS", lateness_p99_h=f"{p99} vs {th.get('late_arrivals_hours', 24)} "
                                                f"({n} batches vs {min_batches})",
           reopened=f"{reopened} vs {th.get('late_reopened_partitions', 3)}")
    if not (long_wait or reopens):
        return []
    ev = {"lateness_p95_h": tm.get("lateness_p95_h"), "lateness_p99_h": p99,
          "batches_in_window": tm.get("lateness_p99_batches"), "lookback_days": tm.get("lateness_lookback_days"),
          "reopened_partitions": reopened,
          "possible_backfill_batches_30d": tm.get("possible_backfill_batches_30d"),
          "possible_full_refreshes_30d": tm.get("possible_full_refreshes_30d")}
    score = max((float(p99) / th.get("late_arrivals_hours", 24)) if long_wait else 0,
                reopened / th.get("late_reopened_partitions", 3))
    return [_finding(table, "LATE_ARRIVALS", score, ev)]


def refresh_findings(table, tm, th, keep_full_copies=1):
    """GL2.5o+ (learned windows only): possible full refreshes.
    FREQUENT_FULL_REFRESH (advisory, never acted on): the table is replaced
    whole again and again and it is large. Full copies kept by older snapshots:
    SNAPSHOT_BUILDUP, so expire_snapshots runs sooner (the plan keeps the copy
    before the latest refresh, keep_full_copies)."""
    out = []
    n = _num(tm.get("possible_full_refreshes_30d"))
    size = _num(tm.get("data_bytes"))
    if (n >= th.get("frequent_full_refresh_min", 4)
            and size >= th.get("frequent_full_refresh_min_bytes", 1073741824)):
        out.append(_finding(table, "FREQUENT_FULL_REFRESH", n / th.get("frequent_full_refresh_min", 4),
                            {"possible_full_refreshes_30d": n, "avg_bytes_per_refresh": tm.get("full_refresh_avg_bytes"),
                             "data_bytes": size}))
    copies = _num(tm.get("retained_full_copies"))
    if copies > keep_full_copies:
        # retained_bytes already counts only what old snapshots keep alive
        # (all_files minus the current files), so it is the extra itself.
        extra = None if tm.get("retained_bytes") is None else int(tm["retained_bytes"])
        out.append(_finding(table, "SNAPSHOT_BUILDUP", copies / max(keep_full_copies, 1),
                            {"retained_full_copies": copies, "keep_full_copies": keep_full_copies,
                             "bytes_kept_beyond_current": extra, "snapshots": tm.get("snapshots")},
                            remedy=f"{copies} earlier full copies are still referenced by old snapshots: "
                                   f"expire_snapshots, keeping {keep_full_copies} copy before the latest refresh"))
    return out


def _size(b):
    """Human size: MiB, GiB or TiB."""
    b = b or 0
    for unit, n in (("TiB", 1 << 40), ("GiB", 1 << 30)):
        if b >= n:
            return f"{b / n:.1f} {unit}"
    return f"{b / 1048576:.1f} MiB"


def _usd(x):
    return f"about {x:,.2f} USD" if x >= 0.01 else "under 0.01 USD"


def _priced(ev, b):
    """bytes, with what they'd cost a month when a storage price is set."""
    p = ev.get("storage_usd_per_gb_month")
    priced = p and (ev.get("usd_per_month") or 0) >= 0.01      # not worth pricing fractions of a cent
    return _size(b) + (f" ({_usd(b / 1073741824 * p)} a month)" if priced else "")


def retained_remedy(ev):
    """RETAINED_STORAGE remedy text: an expiry when snapshots are past the policy,
    otherwise advice with the numbers (a shorter policy, or merge-on-read writers)."""
    pol = ev.get("policy_age_h")
    pol_txt = (f"the policy ({float(pol):g} h, keep {ev.get('policy_min_keep')}"
               f"{', ' + ev['policy_source'] if ev.get('policy_source') else ''})" if pol is not None
               else "the policy")
    usd = ev.get("usd_per_month")
    head = (f"old snapshots keep {_size(ev['retained_bytes'])}, {ev['share']:g}x the live "
            f"{_size(ev['live_bytes'])}"
            + (f" ({_usd(usd)} a month in storage at {ev['storage_usd_per_gb_month']:g} USD per GB-month; "
               f"reads don't touch these files)" if usd is not None else ""))
    n = ev.get("expirable_snapshots")
    if n:
        frees = (f", which frees {_size(ev['expirable_bytes'])} and leaves {_size(ev['inside_policy_bytes'])} "
                 f"inside the policy" if ev.get("expirable_bytes") is not None else "")
        return (f"{head}: {n} snapshot(s) are past {pol_txt}: expire to the policy now{frees} "
                f"(files kept for the grace); if it stays high, see the advice on the next scan")
    if n is None:
        return (f"{head}: expire to the retention policy now; if it stays high inside the policy, "
                f"shorten the policy (history.expire.max-snapshot-age-ms)")
    parts = [f"{head}, none of it expirable under {pol_txt}: no expiry would free it"]
    by_op = ev.get("retained_by_operation") or {}
    writers = sum(v for k, v in by_op.items() if k != "replace")
    rewrites = by_op.get("replace", 0)
    if by_op:
        parts.append(f"writers' overwrites and deletes keep {_size(writers)}, rewrites (compaction) {_size(rewrites)}")
    options = []
    total = ev["retained_bytes"]
    keep = ev.get("policy_min_keep")
    shorter = [w for w in (ev.get("what_if") or []) if w.get("retained_bytes") is not None
               and w["retained_bytes"] < total]
    fewer = [w for w in (ev.get("what_if_keep") or []) if w.get("retained_bytes") is not None
             and w["retained_bytes"] < total]
    if ev.get("held_by_min_keep"):
        parts.append(f"snapshots past the age limit stay because they are among the last {keep} kept whatever "
                     f"their age (min-snapshots), so on a table that stops committing they stay until the "
                     f"minimum is lowered")
    if shorter:
        options.append("shorten the policy (history.expire.max-snapshot-age-ms): " + ", ".join(
            f"{w['hours']:g} h would keep {_priced(ev, w['retained_bytes'])}" for w in shorter))
    if fewer:
        options.append("lower the minimum kept (history.expire.min-snapshots-to-keep): " + ", ".join(
            f"{w['keep']} would keep {_priced(ev, w['retained_bytes'])}" for w in fewer))
    if not shorter and not fewer:
        options.append("shorten the policy (history.expire.max-snapshot-age-ms) or lower the minimum kept "
                       "(history.expire.min-snapshots-to-keep)")
    if rewrites > writers:
        parts.append("most of it is the copy compaction left behind, which goes once those snapshots leave "
                     "the window and the minimum kept")
    if writers and writers >= rewrites:
        w24 = ev.get("writer_removed_24h")
        options.append("switch the writers to merge-on-read (write.merge.mode, write.update.mode, "
                       "write.delete.mode = merge-on-read) so a MERGE writes delete files instead of copying "
                       "whole data files" + (f" (they replaced {_size(w24)} in the last 24 h)" if w24 else ""))
    return "; ".join(parts) + ". Options: " + "; or ".join(options)


def storage_findings(table, tm, th, cost=None):
    """GL2.6c, size-based bloat (Traceability: snapshot count stays evidence only;
    an append-only stream with thousands of snapshots can keep almost nothing
    extra alive, a copy-on-write table with forty can keep ten copies).
    RETAINED_STORAGE (M16): bytes only old snapshots reference, over
    retained_storage_min_bytes AND over retained_storage_min_share of the live
    bytes. METADATA_BLOAT (M35, M36): the current metadata.json over
    metadata_max_json_bytes, or manifests only old snapshots reference over
    metadata_max_retained_bytes. Both are suggestions (approval)."""
    out = []
    live = _num(tm.get("data_bytes"))
    kept = tm.get("retained_bytes")
    tr.log("rule.RETAINED_STORAGE", retained=f"{kept} vs {th.get('retained_storage_min_bytes', 1073741824)}",
           share=(None if not (kept is not None and live) else
                  f"{kept / live:.2f} vs {th.get('retained_storage_min_share', 1.0)}"))
    if kept is not None and live:
        share = kept / live
        price = float((cost or {}).get("storage_usd_per_gb_month") or 0)
        usd = round(kept / 1073741824 * price, 4) if price else None
        min_usd = float(th.get("retained_storage_min_usd_month") or 0)
        if (kept >= th.get("retained_storage_min_bytes", 1073741824)
                and share > th.get("retained_storage_min_share", 1.0)
                and (not min_usd or usd is None or usd >= min_usd)):
            ev = {"retained_bytes": int(kept), "live_bytes": int(live), "share": round(share, 2),
                  "snapshots": tm.get("snapshots"), "oldest_snapshot_age_h": tm.get("oldest_snapshot_age_h"),
                  "expirable_snapshots": tm.get("expirable_snapshots"), "policy_age_h": tm.get("policy_age_h"),
                  "policy_min_keep": tm.get("policy_min_keep"), "policy_source": tm.get("policy_source"),
                  "usd_per_month": usd, "storage_usd_per_gb_month": price or None}
            try:
                detail = json.loads(tm.get("retained_detail_json") or "null")
            except ValueError:
                detail = None
            if detail:
                ev["expirable_bytes"] = detail.get("expirable_bytes")
                ev["inside_policy_bytes"] = detail.get("inside_policy_bytes")
                ev["retained_by_operation"] = detail.get("by_operation")
                ev["writer_removed_24h"] = detail.get("writer_removed_24h")
                ev["what_if"] = detail.get("what_if")
                ev["what_if_keep"] = detail.get("what_if_keep")
                ev["held_by_min_keep"] = detail.get("held_by_min_keep")
            f = _finding(table, "RETAINED_STORAGE", share / th.get("retained_storage_min_share", 1.0), ev,
                         remedy=retained_remedy(ev))
            if tm.get("expirable_snapshots") == 0:
                # nothing past the policy: no expiry would change it, the policy is the owner's
                f["action"], f["automatic"] = "advisory", False
            out.append(f)
    js, meta = tm.get("metadata_json_bytes"), tm.get("retained_metadata_bytes")
    max_js = th.get("metadata_max_json_bytes", 8388608)
    max_meta = th.get("metadata_max_retained_bytes", 1073741824)
    tr.log("rule.METADATA_BLOAT", metadata_json=f"{js} vs {max_js}", old_manifests=f"{meta} vs {max_meta}")
    if (js is not None and js >= max_js) or (meta is not None and meta >= max_meta):
        score = max((js or 0) / max_js, (meta or 0) / max_meta)
        out.append(_finding(table, "METADATA_BLOAT", score,
                            {"metadata_json_bytes": js, "retained_metadata_bytes": meta,
                             "snapshots": tm.get("snapshots"), "commits_24h": tm.get("commits_24h"),
                             "metadata_versions": tm.get("metadata_versions")}))
    return out


def expiry_findings(table, tm, th):
    """GL2.6e, expiry by policy (expiry.py fills the facts). SNAPSHOT_BUILDUP when
    the policy would expire at least expire_min_snapshots snapshots: snapshots
    older than the policy age, beyond the newest min_keep, not held by a ref.
    Snapshot count alone no longer triggers it (it stays evidence). STALE_REF
    when a ref other than main is older than expiry.stale_ref_hours and has no
    retention of its own."""
    out = []
    n = tm.get("expirable_snapshots")
    need = int(th.get("expire_min_snapshots", 1))
    tr.log("rule.SNAPSHOT_BUILDUP(policy)", expirable=f"{n} vs {need}",
           policy=f"{tm.get('policy_age_h')} h / keep {tm.get('policy_min_keep')} ({tm.get('policy_source')})",
           category=tm.get("write_category"), snapshots=tm.get("snapshots"))
    if n is not None and n >= need:
        out.append(_finding(table, "SNAPSHOT_BUILDUP", max(1.0, n / max(need, 1) / 10 + 1),
                            {"expirable_snapshots": n, "oldest_expirable_age_h": tm.get("oldest_expirable_age_h"),
                             "policy_age_h": tm.get("policy_age_h"), "policy_min_keep": tm.get("policy_min_keep"),
                             "policy_source": tm.get("policy_source"), "write_category": tm.get("write_category"),
                             "snapshots": tm.get("snapshots"), "retained_bytes": tm.get("retained_bytes"),
                             "policy_file_grace_h": tm.get("policy_file_grace_h"),
                             "policy_grace_source": tm.get("policy_grace_source"),
                             "freed_files_waiting": tm.get("freed_files_waiting")},
                            remedy=f"expire snapshots to the policy: older than {tm.get('policy_age_h'):g} h, "
                                   f"keeping the last {tm.get('policy_min_keep')} ({tm.get('policy_source')}); "
                                   f"freed files kept {tm.get('policy_file_grace_h') or 12:g} h for running readers"))
    try:
        stale = json.loads(tm.get("stale_refs_json") or "[]")
    except ValueError:
        stale = []
    tr.log("rule.STALE_REF", stale_refs=stale or None)
    if stale:
        out.append(_finding(table, "STALE_REF", 1 + len(stale),
                            {"refs": stale, "retained_bytes": tm.get("retained_bytes")},
                            remedy="refs keep old snapshots alive: " + ", ".join(
                                f"{r['type']} {r['name']} ({r['head_age_h']} h old)" for r in stale)
                                   + "; drop them or set a retention"))
    return out


def _mib(b):
    return f"{b / 1048576:.1f} MiB" if b is not None else "?"


def writer_findings(table, tm, th, target_bytes):
    """GL2.7a, the writer's side (ledger facts, ledger.writer_stats / history_update).

    SNAPSHOT_RATE: more writer commits in 24 h than snapshot_rate_max_commits_24h,
    each writing less than snapshot_rate_small_share of a target file. The
    suggested trigger lets one commit carry about snapshot_rate_fill_share of a
    target file at the current data rate, at least long enough to get under the
    limit and at most snapshot_rate_max_interval_minutes.

    HISTORY_LOST: the ledger found commits that were expired before it saw them
    (event ledger-gap) at least history_lost_min_gaps times within
    history_lost_window_days. Facts built from history (commit rate, gaps,
    lateness, this rule's writer cadence) miss those commits."""
    out = []
    n = tm.get("writer_commits_24h_seen")
    if n is None:
        n = tm.get("writer_commits_24h")
    max_n = float(th.get("snapshot_rate_max_commits_24h", 288))
    per = tm.get("writer_bytes_per_commit_24h")
    small_share = float(th.get("snapshot_rate_small_share", 0.25))
    small = per is None or per < target_bytes * small_share
    tr.log("rule.SNAPSHOT_RATE", commits_24h=f"{n} vs {max_n:g}",
           bytes_per_commit=f"{_mib(per)} vs {_mib(target_bytes * small_share)}",
           gap_median_min=tm.get("writer_gap_median_min"))
    if n is not None and n > max_n and small:
        gap = tm.get("writer_gap_median_min") or 1440.0 / n
        floor = 1440.0 / max_n                                    # gets the count under the limit
        fill = float(th.get("snapshot_rate_fill_share", 0.5)) * target_bytes
        want = gap * fill / per if per else floor
        cap = float(th.get("snapshot_rate_max_interval_minutes", 60))
        suggested = round(min(max(want, floor), cap), 1)
        out.append(_finding(
            table, "SNAPSHOT_RATE", n / max_n,
            {"writer_commits_24h": n, "gap_median_min": gap, "bytes_per_commit": per,
             "files_per_commit": tm.get("writer_files_per_commit_24h"), "target_file_bytes": target_bytes,
             "suggested_interval_min": suggested, "commits_per_day_after": round(1440.0 / suggested),
             "snapshots": tm.get("snapshots"), "metadata_json_bytes": tm.get("metadata_json_bytes")},
            evidence_level="observed",
            remedy=(f"the writer commits every {gap:.1f} min ({int(n)} commits in 24 h, {_mib(per)} each): every "
                    f"commit adds a snapshot, a manifest list and small files. Commit about every {suggested:g} min "
                    f"(~{round(1440.0 / suggested)} commits a day, ~{_mib(per * suggested / gap) if per else '?'} "
                    f"each): streaming trigger(processingTime='{suggested:g} minutes'), Flink checkpoint interval, "
                    f"or the batch schedule. Compaction and expiry only clean up after it.")))
    g = tm.get("history_gaps_window")
    need = int(th.get("history_lost_min_gaps", 1))
    window = th.get("history_lost_window_days", 7)
    interval = tm.get("minutes_since_previous_scan")
    kept_h = tm.get("oldest_snapshot_age_h")
    tr.log("rule.HISTORY_LOST", gaps=f"{g} vs {need} in {window} d", lost_commits=tm.get("history_lost_commits"),
           scan_interval_min=interval, history_kept_h=kept_h)
    if g is not None and g >= need:
        lost = tm.get("history_lost_commits")
        lost_txt = f"{lost} commits were" if lost else "commits were"
        keep_h = round(2 * interval / 60.0, 1) if interval else None
        advice = (f"the table keeps ~{kept_h:g} h of snapshots and scans run ~{interval / 60.0:.1f} h apart: "
                  f"keep at least {keep_h:g} h (history.expire.max-snapshot-age-ms >= {int(keep_h * 3600000)}) "
                  f"or scan more often" if interval and kept_h is not None else
                  "keep the history longer than the time between scans, or scan more often")
        out.append(_finding(
            table, "HISTORY_LOST", 1 + g,
            {"gaps_in_window": g, "window_days": window, "lost_commits": lost,
             "last_gap_ms": tm.get("last_history_gap_ms"), "unseen_minutes": tm.get("history_unseen_minutes"),
             "scan_interval_min": interval, "history_kept_h": kept_h, "snapshots": tm.get("snapshots")},
            evidence_level="observed",
            remedy=(f"{lost_txt} expired before the advisor saw them ({g} time(s) in {window} days), so history-based "
                    f"facts (commit rate, gaps, lateness, writer cadence) miss them; {advice}. If another job "
                    f"expires this table, align its retention.")))
    return out


def apply_acks(findings, tm):
    """Table property advisor.ack = SYMPTOM[,SYMPTOM...]: the owner knows and it's
    deliberate. Those findings stay recorded (action 'acknowledged') but drop
    out of plans and the ranked list."""
    acks = {a.strip() for a in str(_props(tm).get("advisor.ack", "")).split(",") if a.strip()}
    for f in findings:
        if f["symptom"] in acks:
            f["action"], f["automatic"] = "acknowledged", False
            f["remedy"] = "acknowledged by the owner (advisor.ack); " + f["remedy"]
    return findings


def evaluate(table, tm, rows, cfg, history=None, actions=None):
    """All findings for one table. tm = table_metrics dict, rows = partition dicts."""
    if tm.get("load_error") and not rows:
        return []
    th = cfg["thresholds"]
    hot = float(cfg.get("hot_partition_minutes", 15))
    new = bool(cfg.get("new_findings"))
    out = (partition_findings(table, rows, th, hot, new)
           + table_findings(table, tm, rows, th, cfg))
    if new:
        out += storage_findings(table, tm, th, cfg.get("cost"))
    if cfg.get("writer_findings"):
        out += writer_findings(table, tm, th, float(tm.get("target_file_bytes") or cfg.get("target_file_bytes", 536870912)))
    if cfg.get("expiry_policy") and tm.get("policy_age_h") is not None:
        # GL2.6e: the policy decides; the count/age SNAPSHOT_BUILDUP gives way
        out = [f for f in out if f["symptom"] != "SNAPSHOT_BUILDUP"] + expiry_findings(table, tm, th)
    if cfg.get("learned_windows"):
        out += late_arrival_findings(table, tm, th, int(cfg.get("min_batches", 20)))
        extra = refresh_findings(table, tm, th, int(cfg.get("keep_full_copies", 1)))
        if any(f["symptom"] == "SNAPSHOT_BUILDUP" for f in out):      # one SNAPSHOT_BUILDUP, with both reasons
            for f in out:
                if f["symptom"] == "SNAPSHOT_BUILDUP":
                    for e in extra:
                        if e["symptom"] == "SNAPSHOT_BUILDUP":
                            ev = dict(json.loads(f["evidence_json"]), **json.loads(e["evidence_json"]))
                            f["evidence_json"] = json.dumps(ev)
                            f["score"] = max(f["score"], e["score"])
            extra = [e for e in extra if e["symptom"] != "SNAPSHOT_BUILDUP"]
        out += extra
        if tm.get("last_full_refresh_ms") and history:
            # trends restart after a possible full refresh (it replaces everything)
            history = [h for h in history if (h.get("scanned_ms") or 0) >= tm["last_full_refresh_ms"]]
    if history or actions:
        out += trend_findings(table, history or [], actions or [], th)
    if cfg.get("learned_windows"):
        apply_acks(out, tm)
    for f in out:
        tr.log("rule.result", f"{f['symptom']} {f.get('partition_key') or '(table)'}", action=f["action"],
               score=f["score"], remedy=f["remedy"][:120])
    if not out:
        tr.log("rule.result", "no findings: healthy")
    return out
