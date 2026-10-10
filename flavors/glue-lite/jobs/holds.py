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
import re
from datetime import datetime, timedelta, timezone

import windows as win

# SQL form of is_conflict (rows from before item 13 have no position / equality split)
CONFLICT_SQL = (
    "is_writer AND (coalesce(data_files_removed, 0) > 0 OR coalesce(delete_files_removed, 0) > 0 OR "
    "CASE WHEN pos_delete_files_added IS NULL AND eq_delete_files_added IS NULL "
    "THEN coalesce(delete_files_added, 0) > 0 "
    "ELSE pos_delete_files_added > 0 AND NOT (coalesce(eq_delete_files_added, 0) > 0 "
    "AND coalesce(data_files_removed, 0) = 0 AND coalesce(data_files_added, 0) > 0) END)")


def is_conflict(r):
    """Item 13: a writer commit that can make a concurrent rewrite fail, in this
    partition. It removed data files (copy-on-write MERGE / UPDATE / DELETE, INSERT
    OVERWRITE) or delete files, or added position deletes (merge-on-read; deletion
    vectors count as position deletes): they name files a rewrite may be
    replacing. Equality deletes never conflict (a rewrite keeps the starting
    sequence number, so they still apply to the new files: Iceberg skips them in
    the rewrite's validation), and neither do the position deletes a Flink-style
    upsert writes next to them, which point only at files the same commit added
    (equality deletes added, no data file removed, data files added). Appends
    never conflict. Rows from before the split count any added delete file."""
    if not r.get("is_writer"):
        return False
    if (r.get("data_files_removed") or 0) > 0 or (r.get("delete_files_removed") or 0) > 0:
        return True
    pos, eq = r.get("pos_delete_files_added"), r.get("eq_delete_files_added")
    if pos is None and eq is None:
        return (r.get("delete_files_added") or 0) > 0
    if (pos or 0) <= 0:
        return False
    flink_style = (eq or 0) > 0 and (r.get("data_files_added") or 0) > 0
    return not flink_style


def activity_sql(pa, ids, since_ms, churn_since_ms):
    """Per (table, partition): last conflicting writer commit, data bytes removed
    by writer commits since churn_since_ms, and how many such commits."""
    conflict = CONFLICT_SQL
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
    {last_conflict_ms, conflict_ts (sorted, for the hold limit), removed_bytes,
    rewrite_commits}} for one table."""
    out, commits = {}, {}
    for r in rows:
        ts = r.get("ts_ms")
        if ts is None or ts < since_ms:
            continue
        pk = r.get("partition_key")
        a = out.setdefault(pk, {"last_conflict_ms": None, "conflict_ts": [], "removed_bytes": 0,
                                "rewrite_commits": 0})
        if not r.get("is_writer"):
            continue
        if is_conflict(r):
            a["last_conflict_ms"] = ts if a["last_conflict_ms"] is None else max(a["last_conflict_ms"], ts)
            a["conflict_ts"].append(ts)
        if ts >= churn_since_ms:
            a["removed_bytes"] += int(r.get("data_bytes_removed") or 0)
            if (r.get("data_files_removed") or 0) > 0:
                commits.setdefault(pk, set()).add(r.get("snapshot_id"))
    for pk, ids in commits.items():
        out[pk]["rewrite_commits"] = len(ids)
    for a in out.values():
        a["conflict_ts"] = sorted(set(a["conflict_ts"]))
    return out


# ---------------------------------------------------------------- declared windows (item 13)

DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
_RANGE = re.compile(r"^(?:(mon|tue|wed|thu|fri|sat|sun)[a-z]*\s+)?(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})$")


def parse_blackout(spec):
    """advisor.compact.blackout: comma-separated UTC ranges, each 'HH:MM-HH:MM'
    (every day) or 'Sun HH:MM-HH:MM' (that weekday); a range may cross midnight
    (22:00-02:00), and then the weekday is the day it starts. -> ([(weekday or
    None, start_min, end_min)], [parts not understood])."""
    out, bad = [], []
    for part in str(spec or "").split(","):
        p = part.strip()
        if not p:
            continue
        m = _RANGE.match(p.lower())
        if not m:
            bad.append(p)
            continue
        day, h1, m1, h2, m2 = m.groups()
        a, b = int(h1) * 60 + int(m1), int(h2) * 60 + int(m2)
        if a >= 1440 or b > 1440 or a == b or int(m1) > 59 or int(m2) > 59:
            bad.append(p)
            continue
        out.append((None if day is None else DAYS.index(day[:3]), a, b))
    return out, bad


def in_blackout(ranges, ms):
    """-> the matching range as text, or None."""
    t = datetime.fromtimestamp(ms / 1000.0, timezone.utc)
    mins, wd = t.hour * 60 + t.minute, t.weekday()
    for day, a, b in ranges:
        if a < b:
            hit = a <= mins < b and (day is None or day == wd)
        else:                                   # crosses midnight
            hit = (mins >= a and (day is None or day == wd)) or \
                  (mins < b and (day is None or day == (wd - 1) % 7))
        if hit:
            label = f"{a // 60:02d}:{a % 60:02d}-{b // 60:02d}:{b % 60:02d}"
            return (DAYS[day].capitalize() + " " + label) if day is not None else label
    return None


# ---------------------------------------------------------------- hold limit (item 13)

def held_at(conflict_ts, scan_ms, window_min):
    """Would the conflict hold have held the partition at a run at scan_ms?"""
    lo = scan_ms - window_min * 60000.0
    return any(lo < t <= scan_ms for t in conflict_ts)


def held_series_start(conflict_ts, scan_times, scan_ms, window_min, horizon_ms, blackout=None):
    """The first run of the unbroken series of runs, ending with this one, that
    held the partition for a conflicting commit. Runs inside a declared blackout
    are neutral (the blackout held everything). Runs before horizon_ms + the
    window can't be judged (no activity read that far back) and end the walk.
    -> ms, or None when this run doesn't hold it."""
    if not held_at(conflict_ts, scan_ms, window_min):
        return None
    start, broke = scan_ms, False
    floor = horizon_ms + window_min * 60000.0
    for s in sorted((t for t in scan_times if t < scan_ms), reverse=True):
        if s < floor:
            broke = True
            break
        if blackout and in_blackout(blackout, s):
            continue
        if not held_at(conflict_ts, s, window_min):
            broke = True
            break
        start = s
    if not broke:
        # the run history kept (the last TREND_KEEP scans) ran out while every run held
        # it: before those runs, any run would have been held as long as the writer
        # never paused for the window, so follow the conflicting commits back
        w = window_min * 60000.0
        earlier = sorted((t for t in conflict_ts if floor <= t <= start), reverse=True)
        prev = start
        for t in earlier:
            if prev - t >= w:
                break
            prev = t
        start = min(start, prev)
    return start


def gap_stats(conflict_ts, since_ms, until_ms):
    """Writer evidence for HOLD_STARVED: conflicting commits since since_ms, the
    median and longest gap between them (minutes) and when the longest began."""
    ts = [t for t in conflict_ts if since_ms <= t <= until_ms]
    gaps = sorted((b - a, a) for a, b in zip(ts, ts[1:]))
    if not gaps:
        return {"conflicting_commits": len(ts), "median_gap_min": None, "longest_gap_min": None,
                "longest_gap_at": None}
    med = gaps[len(gaps) // 2][0] / 60000.0
    longest, at = gaps[-1]
    return {"conflicting_commits": len(ts), "median_gap_min": round(med, 1),
            "longest_gap_min": round(longest / 60000.0, 1),
            "longest_gap_at": datetime.fromtimestamp(at / 1000.0, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}


def holds_rows(rows, pstate, activity, fields, scan_ms, hold=None):
    """Copies of the partition rows with what the conflict hold and the churn
    advice need: minutes_since_update from the ledger's last writer write (when
    known), hours_since_end (time partitions, for the evidence),
    minutes_since_conflict, the M32 rewrite rate, and the oldest fragment's
    age (the tier rule's minor wait). activity: {partition_key: {last_conflict_ms, removed_bytes,
    rewrite_commits}}.

    hold (item 13, optional): {"blackout": table property text, "window_min",
    "limit_h", "busy_h" (late-arrival allowance after a time partition ends),
    "scan_times" (this table's earlier runs), "horizon_ms" (how far back
    activity was read)} adds blackout (the matching range, when this run is
    inside one) and, for a partition the conflict hold holds, held_since_ms,
    busy_until_ms and hold_starved (held at every run for limit_h after both
    the series start and, for time partitions, end + busy_h)."""
    hold = hold or {}
    ranges, bad = parse_blackout(hold.get("blackout"))
    black = in_blackout(ranges, scan_ms) if ranges else None
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
        r["blackout"] = black
        if bad:
            r["blackout_unparsed"] = ", ".join(bad)
        if hold.get("limit_h") is not None and a.get("conflict_ts") is not None:
            w = float(hold.get("window_min", 15))
            start = held_series_start(a["conflict_ts"], hold.get("scan_times") or [], scan_ms, w,
                                      int(hold.get("horizon_ms") or 0), ranges)
            r["held_since_ms"] = start
            r["held_h"] = None if start is None else round((scan_ms - start) / 3600000.0, 2)
            busy = None if end is None else end + float(hold.get("busy_h", 24)) * 3600000.0
            r["busy_until_ms"] = busy
            limit = float(hold["limit_h"]) * 3600000.0
            clock = None if start is None else max(start, busy or start)
            r["hold_starved"] = bool(clock is not None and scan_ms - clock >= limit)
            if r["hold_starved"]:
                r.update(gap_stats(a["conflict_ts"], scan_ms - limit, scan_ms))
        out.append(r)
    return out


def decide(r, hot_minutes):
    """-> (hold, reason): held inside a declared blackout (advisor.compact.blackout),
    or for a conflicting writer commit within hot_minutes."""
    if r.get("blackout"):
        return True, f"inside the table's compaction blackout ({r['blackout']} UTC, advisor.compact.blackout)"
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
