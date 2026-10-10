"""The writer-conflict hold, and the per-partition facts it and the churn advice need.

A rewrite only conflicts with a concurrent commit that removes or rewrites the
same files (overwrite, delete, a merge, delete files added or removed); appends
just add files. So a partition is held (HOT_PARTITION, defer) only when such a
conflicting writer commit touched it within hot_partition_minutes. Appends never
hold a partition, open time range or not. (GL2.6b shipped this as the
partition_holds family next to the older "any write in the hot window" rule;
since 2026-10-10 it is the only rule, the still-filling branch and the churn
hold are gone, and churn is advice on the finding: symptom_rules.)

Churn facts: per partition, M32 rewrite rate = data bytes removed from the
partition by writer commits in the last churn_window_h / its live data bytes.

Inputs come from family 2 (partition_activity / partition_state); a table with
no activity rows yet has no minutes_since_conflict and is never held.

Pure logic; detect_symptoms.py does the I/O.
"""
import windows as win


def activity_sql(pa, ids, since_ms, churn_since_ms):
    """Per (table, partition): last conflicting writer commit, data bytes removed
    by writer commits since churn_since_ms, and how many such commits."""
    conflict = ("is_writer AND (coalesce(data_files_removed, 0) > 0 OR coalesce(delete_files_added, 0) > 0 "
                "OR coalesce(delete_files_removed, 0) > 0)")
    return f"""
        SELECT table_uuid, partition_key,
               max(CASE WHEN {conflict} THEN ts_ms END) AS last_conflict_ms,
               sum(CASE WHEN is_writer AND ts_ms >= {int(churn_since_ms)}
                        THEN coalesce(data_bytes_removed, 0) ELSE 0 END) AS removed_bytes,
               count(DISTINCT CASE WHEN is_writer AND ts_ms >= {int(churn_since_ms)}
                                    AND coalesce(data_files_removed, 0) > 0 THEN snapshot_id END) AS rewrite_commits
        FROM {pa}
        WHERE table_uuid IN ({ids}) AND ts_ms >= {int(since_ms)}
        GROUP BY table_uuid, partition_key"""


def activity_from_rows(rows, since_ms, churn_since_ms):
    """activity_sql over commit_partition items (state store): {partition_key:
    {last_conflict_ms, removed_bytes, rewrite_commits}} for one table."""
    out, commits = {}, {}
    for r in rows:
        ts = r.get("ts_ms")
        if ts is None or ts < since_ms:
            continue
        pk = r.get("partition_key")
        a = out.setdefault(pk, {"last_conflict_ms": None, "removed_bytes": 0, "rewrite_commits": 0})
        if not r.get("is_writer"):
            continue
        if (r.get("data_files_removed") or 0) > 0 or (r.get("delete_files_added") or 0) > 0 \
                or (r.get("delete_files_removed") or 0) > 0:
            a["last_conflict_ms"] = ts if a["last_conflict_ms"] is None else max(a["last_conflict_ms"], ts)
        if ts >= churn_since_ms:
            a["removed_bytes"] += int(r.get("data_bytes_removed") or 0)
            if (r.get("data_files_removed") or 0) > 0:
                commits.setdefault(pk, set()).add(r.get("snapshot_id"))
    for pk, ids in commits.items():
        out[pk]["rewrite_commits"] = len(ids)
    return out


def holds_rows(rows, pstate, activity, fields, scan_ms):
    """Copies of the partition rows with what the conflict hold and the churn
    advice need: minutes_since_update from the ledger's last writer write (when
    known), hours_since_end (time partitions, for the evidence),
    minutes_since_conflict, the M32 rewrite rate, and the oldest fragment's
    age (the tier rule's minor wait). activity: {partition_key: {last_conflict_ms, removed_bytes,
    rewrite_commits}}."""
    out = []
    for r in rows:
        r = dict(r)
        pk = r.get("partition_key")
        st = (pstate or {}).get(pk) or {}
        if st.get("last_write_ms") is not None:
            r["minutes_since_update"] = (scan_ms - st["last_write_ms"]) / 60000.0
        end = win.partition_end_from_key(pk, fields)
        r["hours_since_end"] = None if end is None else (scan_ms - end) / 3600000.0
        a = (activity or {}).get(pk) or {}
        lc = a.get("last_conflict_ms")
        r["minutes_since_conflict"] = None if lc is None else (scan_ms - lc) / 60000.0
        removed = int(a.get("removed_bytes") or 0)
        live = r.get("data_bytes") or 0
        r["removed_bytes_window"] = removed
        r["rewrite_commits_window"] = int(a.get("rewrite_commits") or 0)
        r["rewrite_rate"] = round(removed / live, 3) if live else (0.0 if not removed else None)
        of = r.get("oldest_fragment_ms")
        r["oldest_fragment_age_h"] = None if of is None else max(0.0, (scan_ms - int(of)) / 3600000.0)
        out.append(r)
    return out


def decide(r, hot_minutes):
    """-> (hold, reason): held only for a conflicting writer commit within
    hot_minutes."""
    conflict = r.get("minutes_since_conflict")
    if conflict is not None and conflict < hot_minutes:
        return True, f"a writer commit removed or rewrote files {conflict:.1f} min ago"
    return False, None


def diff_actions(current, revised):
    """Finding changes, action-aware: a SMALL_FILES that turns advisory counts.
    -> list of strings."""
    key = lambda f: (f.get("partition_key") or "", f["symptom"])
    a = {key(f): f["action"] for f in current}
    b = {key(f): f["action"] for f in revised}
    out = []
    for pk in sorted({k[0] for k in set(a) | set(b)}):
        was = sorted(f"{s}:{a[(p, s)]}" for p, s in a if p == pk)
        now = sorted(f"{s}:{b[(p, s)]}" for p, s in b if p == pk)
        if was != now:
            out.append(f"{pk or '(table)'}: {','.join(was) or '-'} -> {','.join(now) or '-'}")
    return out
