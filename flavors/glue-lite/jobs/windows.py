"""GL2.5o: incremental scan, family 3 - learned windows.

One wait learned per table, plus the lateness facts for LATE_ARRIVALS:

  hot window    how long a writer may pause between commits and still be
                "writing": 2 x the 95th-percentile commit gap (family 1's gap
                histogram), at least the configured hot_partition_minutes (the
                floor), at most hot_cap_minutes; needs min_gaps gaps. Idle
                gaps (longer than idle_gap_factor x the median gap: nights,
                weekends, a paused job) are left out first, so a writer that
                commits every 2 minutes doesn't get a 4-hour hot window.
                Shadow only; plan item 13 decides its fate.
  lateness      the 95th/99th-percentile lateness (family 2's lateness
                histogram; only append-only batches labelled on_time or late
                count) and the chance of more late data, for LATE_ARRIVALS,
                which advises the owner. Lateness no longer holds compaction:
                the settle window and SETTLING were retired 2026-10-10 (the
                tier rule decides, plan item 12).

Lookback (adaptive): both histograms are read over histogram_days; a table
with fewer than min_batches / min_gaps samples there reaches further back,
whole days at a time, until it has them or hits lookback_max_days. A table
that writes once a day gets a learned window after 20 days instead of never;
a busy table keeps its 30 days.

Long lateness or frequent reopens raise LATE_ARRIVALS.

Per-partition age comes from family 2's partition_state (last writer write),
not from the full path, whose per-partition age is only computed inside the
configured window (outside it every partition gets the table-level age, which
would mark them all hot under a longer learned window).

Mode (config incremental.learned_windows): shadow = findings come from the
configured windows and the learned variant is reported as "would change";
on = findings come from the learned windows.

Pure logic; detect_symptoms.py does the I/O.
"""
import json
from datetime import date

import activity as act


def adaptive_window(day_counts, today, base_days, max_days, min_n):
    """day_counts: {date: {bucket_edge: n}} (as far back as max_days).
    -> (counts {edge: n}, n, days_used). Every day younger than base_days is in;
    older days are added newest first only while n < min_n, up to max_days."""
    counts, n, oldest = {}, 0, None
    for d in sorted(day_counts, reverse=True):
        age = (today - d).days
        if age >= max(max_days, base_days) or (age >= base_days and n >= min_n):
            break
        for b, k in day_counts[d].items():
            counts[b] = counts.get(b, 0) + k
            n += k
        oldest = d
    used = base_days if oldest is None else max(base_days, (today - oldest).days + 1)
    return counts, n, used


def drop_idle(counts, buckets, factor):
    """Leave out gaps in buckets entirely above factor x the median gap.
    counts: {edge: n}, buckets: the edges in order (None last). -> (kept, dropped)."""
    total = sum(counts.values())
    if not total or not factor:
        return dict(counts), 0
    k, run, median = max(1, -(-total // 2)), 0, None
    for e in buckets:
        run += counts.get(e, 0)
        if run >= k:
            median = e
            break
    if median is None:                      # most gaps are longer than the last edge
        return dict(counts), 0
    limit = factor * median
    kept, dropped, lower = {}, 0, 0
    for e in buckets:
        n = counts.get(e, 0)
        if lower >= limit:
            dropped += n
        elif n:
            kept[e] = n
        lower = e if e is not None else lower
    return kept, dropped


def learned_hot(p95_edge, n_gaps, floor_min, cap_min=1440.0, min_gaps=20, factor=2.0):
    """-> (minutes, source)."""
    if not n_gaps or n_gaps < min_gaps:
        return float(floor_min), f"config (only {n_gaps or 0} gaps)"
    if p95_edge is None:                      # p95 beyond the last bucket
        return float(cap_min), "learned (capped)"
    m = factor * float(p95_edge)
    if m <= floor_min:
        return float(floor_min), f"config floor (learned {m:g})"
    if m >= cap_min:
        return float(cap_min), "learned (capped)"
    return m, "learned"


def partition_end_from_key(partition_key, fields):
    """End of a partition's time range (ms) from its key and the table's
    partition fields [{name, transform, source_type}], or None."""
    try:
        values = json.loads(partition_key or "{}")
    except ValueError:
        return None
    for f in fields or []:
        tr, name = f.get("transform", ""), f.get("name")
        src = (f.get("source_type") or "").upper()
        if name not in values:
            continue
        v = values[name]
        if tr == "day" or (tr == "identity" and src == "DATE"):
            try:
                days = (date.fromisoformat(str(v)) - act.EPOCH).days
            except ValueError:
                continue
            return act.partition_end_ms("day", "DATE", days)
        if tr in ("hour", "month", "year"):
            return act.partition_end_ms(tr, src, v)
    return None


def p_more_late(counts, age_h):
    """Upper bound on the chance that another late batch still lands in a
    partition that ended age_h ago: the share of batches in buckets whose upper
    edge is above age_h (the histogram holds on_time / late batches only)."""
    usable = {b: n for b, n in counts.items() if b is not None}
    total = sum(counts.values())
    if not total or age_h is None:
        return None
    later = sum(n for b, n in usable.items() if b > age_h) + counts.get(None, 0)
    return round(later / total, 3)


def learned_rows(rows, pstate, fields, scan_ms, late_counts=None):
    """Copies of the partition rows with the ledger's per-partition age, the
    hours since the partition's time range ended and the chance that more late
    data still comes (evidence)."""
    out = []
    for r in rows:
        r = dict(r)
        pk = r.get("partition_key")
        st = pstate.get(pk) or {}
        lw = st.get("last_write_ms")
        r["minutes_since_update"] = None if lw is None else (scan_ms - lw) / 60000.0
        end = partition_end_from_key(pk, fields)
        r["hours_since_end"] = None if end is None else (scan_ms - end) / 3600000.0
        r["p_more_late"] = p_more_late(late_counts or {}, r["hours_since_end"])
        r["last_write_label"] = st.get("last_write_label")
        out.append(r)
    return out


def diff(current, learned):
    """Partition- and table-level finding changes. -> list of strings."""
    key = lambda f: (f.get("partition_key") or "", f["symptom"])
    a, b = {key(f) for f in current}, {key(f) for f in learned}
    out = []
    for pk in sorted({k[0] for k in a ^ b}):
        was = sorted(s for p, s in a if p == pk)
        now = sorted(s for p, s in b if p == pk)
        out.append(f"{pk or '(table)'}: {','.join(was) or '-'} -> {','.join(now) or '-'}")
    return out
