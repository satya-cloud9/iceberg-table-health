"""GL2.5o: incremental scan, family 3 - learned windows.

Two waits, learned per table instead of one fixed number:

  hot window    how long a writer may pause between commits and still be
                "writing": 2 x the 95th-percentile commit gap (family 1's gap
                histogram), at least the configured hot_partition_minutes (the
                floor), at most hot_cap_minutes; needs min_samples gaps
  settle window how long after a partition's time range ends late data keeps
                landing: the 99th-percentile lateness (family 2's lateness
                histogram), at most settle_cap_hours; time-based partitions only;
                needs min_samples batches

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


def learned_hot(p95_edge, n_gaps, floor_min, cap_min=1440.0, min_samples=20, factor=2.0):
    """-> (minutes, source)."""
    if not n_gaps or n_gaps < min_samples:
        return float(floor_min), f"config (only {n_gaps or 0} gaps)"
    if p95_edge is None:                      # p95 beyond the last bucket
        return float(cap_min), "learned (capped)"
    m = factor * float(p95_edge)
    if m <= floor_min:
        return float(floor_min), f"config floor (learned {m:g})"
    if m >= cap_min:
        return float(cap_min), "learned (capped)"
    return m, "learned"


def learned_settle(p99_edge, n_batches, cap_h=168.0, min_samples=20):
    """-> (hours or None, source)."""
    if not n_batches or n_batches < min_samples:
        return None, f"none (only {n_batches or 0} batches)"
    if p99_edge is None or float(p99_edge) >= cap_h:
        return float(cap_h), "learned (capped)"
    return float(p99_edge), "learned"


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


def learned_rows(rows, pstate, fields, scan_ms):
    """Copies of the partition rows with the ledger's per-partition age and the
    hours since the partition's time range ended."""
    out = []
    for r in rows:
        r = dict(r)
        st = pstate.get(r.get("partition_key")) or {}
        lw = st.get("last_write_ms")
        r["minutes_since_update"] = None if lw is None else (scan_ms - lw) / 60000.0
        end = partition_end_from_key(r.get("partition_key"), fields)
        r["hours_since_end"] = None if end is None else (scan_ms - end) / 3600000.0
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
