"""GL2.6b: revised partition holds (Traceability group 2, shadow first).

Two holds keep a partition out of compaction. Both change:

  hot hold    Today: any write in the last hot_partition_minutes (the table's
              writer age for an unpartitioned table) holds the partition. But
              waiting is an efficiency choice, not a safety one: a rewrite only
              conflicts with a concurrent commit that removes or rewrites the
              same files (overwrite, delete, a merge, delete files added), and
              appends just add files. Revised: hold only when
                - a conflicting commit touched the partition within the hot
                  window, or
                - the partition is a time partition still filling: its time
                  range ended less than a hot window ago (or hasn't ended) and
                  it was written within the hot window.
              Append-only partitions compact on schedule. An unpartitioned table
              appended every few minutes is no longer held for ever; with a
              declared time_column the plan rewrites only rows older than the
              hot window.

  churn hold  Today: REWRITE_CHURN anywhere on the table turns every SMALL_FILES
              advisory, including partitions merges never touch. Revised: per
              partition, M32 rewrite rate = data bytes removed from the
              partition by writer commits in the last churn_window_h / the
              partition's live data bytes. At or above churn_partition_rate
              (per window) a compaction would be undone within hours, so that
              partition's SMALL_FILES turns advisory; the rest compact normally.

Inputs come from family 2 (glue.ops.partition_activity / partition_state), so
the revised holds need family 2's tables; tables without them keep today's
holds. Mode (config incremental.partition_holds): off | shadow (findings from
today's holds, the revised variant reported as "would change") | on.

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
    """Copies of the partition rows with what the revised holds need:
    minutes_since_update from the ledger's last writer write (when known),
    hours_since_end (time partitions), minutes_since_conflict, and the M32
    rewrite rate. activity: {partition_key: {last_conflict_ms, removed_bytes,
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
        out.append(r)
    return out


def decide(r, hot_minutes):
    """-> (hold, reason, appends_continue). appends_continue: today's rule would
    have held the partition (written within the hot window) but no conflicting
    commit did it and it isn't a filling time partition."""
    minutes = r.get("minutes_since_update")
    written = minutes is not None and minutes < hot_minutes
    conflict = r.get("minutes_since_conflict")
    if conflict is not None and conflict < hot_minutes:
        return True, f"a commit removed or rewrote files {conflict:.1f} min ago", False
    since_end = r.get("hours_since_end")
    if written and since_end is not None and since_end * 60 < hot_minutes:
        return True, "time partition still filling", False
    return False, None, written


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
