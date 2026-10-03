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
  defer          would be auto, but the partition is still being written
  needs-evidence workload-dependent; held until query evidence reaches the
                 configured level (declared filter columns are not enough)
  advisory       reported, never acted on: another finding on the table makes
                 this remedy counterproductive (e.g. bigger files under
                 copy-on-write churn)
"""
import json
import math
import re

RULE_VERSION = "2.5g-1"

# symptom -> (category, level, action, remedy)
CATALOG = {
    "SMALL_FILES":           ("file_layout", "partition", "auto",
                              "rewrite_data_files (binpack) scoped to this partition"),
    "HOT_PARTITION":         ("file_layout", "partition", "defer",
                              "wait until writes stop, then compact (still being written)"),
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
    "MANIFEST_BLOAT":        ("metadata", "table", "auto",
                              "rewrite_manifests; re-enable commit.manifest-merge.enabled"),
    "REWRITE_CHURN":         ("write_config", "table", "approval",
                              "switch MERGE/UPDATE/DELETE to merge-on-read and compact on a schedule; "
                              "or narrow each merge (partition predicate, dbt incremental_predicates); "
                              "or sort on the merge key so changed rows sit in few files"),
    "UNBOUNDED_RETENTION":   ("write_config", "table", "approval",
                              "set write.metadata.delete-after-commit.enabled=true and "
                              "write.metadata.previous-versions-max"),
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


def partition_findings(table, rows, th, hot_minutes):
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
        hot = minutes is not None and minutes < hot_minutes

        small = excess >= min_excess
        eq_files = _num(r.get("delete_files_eq"))
        # Position deletes: judged by the share of rows deleted. Equality
        # deletes: one record can delete many rows and every eq-delete file is
        # checked against every older data file on read, so their count matters.
        pos_deletes = del_files >= th["delete_min_files"] and del_ratio >= th["delete_ratio"]
        eq_deletes = eq_files >= th.get("eq_delete_min_files", 5)
        deletes = pos_deletes or eq_deletes
        ev = {"data_files": data_files, "excess_files": excess,
              "small_files": _num(r.get("small_files")), "ideal_files": _num(r.get("ideal_files")),
              "delete_files": del_files, "delete_files_eq": eq_files, "delete_ratio": round(del_ratio, 4),
              "minutes_since_update": None if minutes is None else round(minutes, 1)}

        if (small or deletes) and hot:
            out.append(_finding(table, "HOT_PARTITION", max(excess / min_excess, 1.0), ev, key,
                                remedy=f"wait: last write {ev['minutes_since_update']} min ago "
                                       f"(< {hot_minutes}); compact after it cools"))
            continue
        if small:
            out.append(_finding(table, "SMALL_FILES", excess / min_excess, ev, key))
        if deletes:
            out.append(_finding(table, "DELETE_BUILDUP",
                                max(del_ratio / th["delete_ratio"],
                                    eq_files / th.get("eq_delete_min_files", 5)), ev, key))
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
    if (with_excess >= th["scattered_min_partitions"] and share >= th["scattered_partition_share"]
            and avg_parts >= th["scattered_avg_changed_partitions"]):
        out.append(_finding(table, "SCATTERED_SMALL_FILES", share / th["scattered_partition_share"],
                            {"partitions_to_compact": with_excess, "partitions": parts,
                             "share": round(share, 3), "avg_changed_partitions_per_commit": avg_parts}))

    # SNAPSHOT_BUILDUP: too many, or too old, snapshots kept.
    snaps = _num(tm.get("snapshots"))
    age_h = _num(tm.get("oldest_snapshot_age_h"), 0.0)
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
    if (n_ow >= th.get("churn_min_commits", 5) and share_ow >= th.get("churn_rewrite_share", 0.3)
            and turnover >= th.get("churn_turnover_24h", 2.0)):
        mode = (tm.get("write_merge_mode") or "copy-on-write (default)")
        out.append(_finding(table, "REWRITE_CHURN", turnover / th.get("churn_turnover_24h", 2.0),
                            {"overwrite_commits": n_ow, "avg_rewrite_share": share_ow,
                             "table_turnover_24h": turnover, "write_merge_mode": mode,
                             "rewritten_bytes_24h": tm.get("rewritten_bytes_24h")}))

    # UNBOUNDED_RETENTION: metadata.json versions pile up with no cleanup.
    versions = _num(tm.get("metadata_versions"))
    max_versions = int(props.get("write.metadata.previous-versions-max", th["max_metadata_versions"]))
    auto_delete = str(props.get("write.metadata.delete-after-commit.enabled", "false")).lower() == "true"
    if versions > max_versions and not auto_delete:
        out.append(_finding(table, "UNBOUNDED_RETENTION", versions / max_versions,
                            {"metadata_versions": versions, "previous_versions_max": max_versions}))

    # PARTITION_SKEW: one partition far above the median, and big in absolute
    # terms (a skewed table of tiny partitions is not worth acting on).
    skew = _num(tm.get("skew_ratio"), 0.0)
    largest = max((_num(r.get("data_bytes")) for r in rows), default=0)
    if (parts >= th["skew_min_partitions"] and skew >= th["skew_ratio"]
            and largest >= th["skew_min_largest_targets"] * target):
        out.append(_finding(table, "PARTITION_SKEW", skew / th["skew_ratio"],
                            {"skew_ratio": skew, "largest_partition_bytes": largest,
                             "top1pct_share": tm.get("top1pct_share"), "partitions": parts}))

    # OVER_PARTITIONED: most partitions are tiny and there are many of them.
    # Per-partition rules can't see this (each partition holds one file).
    under = _num(tm.get("undersized_partition_share"), 0.0)
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


def evaluate(table, tm, rows, cfg):
    """All findings for one table. tm = table_metrics dict, rows = partition dicts."""
    if tm.get("load_error") and not rows:
        return []
    th = cfg["thresholds"]
    hot = float(cfg.get("hot_partition_minutes", 15))
    out = partition_findings(table, rows, th, hot) + table_findings(table, tm, rows, th, cfg)
    if any(f["symptom"] == "REWRITE_CHURN" for f in out):
        # Bigger files make every copy-on-write merge rewrite more bytes, so
        # don't compact toward the target while churn is the problem.
        for f in out:
            if f["symptom"] in ("SMALL_FILES", "SCATTERED_SMALL_FILES") and f["action"] == "auto":
                f["action"], f["automatic"] = "advisory", False
                f["remedy"] = "held: REWRITE_CHURN on this table; fix the write mode first (" + f["remedy"] + ")"
    return out
