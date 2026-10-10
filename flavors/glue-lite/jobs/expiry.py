"""GL2.6e: snapshot expiry by policy, refs, and retained bytes from snapshot summaries.

Expiry decides WHAT may go from a retention policy, never from what old
snapshots cost (Traceability A4). The policy, first match wins:

  1. the table's own properties   history.expire.max-snapshot-age-ms /
                                  history.expire.min-snapshots-to-keep (the writer's word)
  2. advisor.expire.max-age-hours / advisor.expire.min-snapshots (table properties
                                  an owner sets for the advisor only)
  3. config expiry.<category>     category = streaming when the table had at least
                                  expiry.streaming_min_commits_24h writer commits in the
                                  last 24 h, else batch (defaults: batch 120 h / 10,
                                  streaming 72 h / 10)
  then one bound on that window:
    max snapshots   advisor.expire.max-snapshots / expiry.max_snapshots (500, D3):
                    more snapshots inside the window shorten it, since each one
                    is an entry in metadata.json that every commit rewrites
  There is no window floor: the advisor's own expiry runs after the ledger
  has caught up in the same run and its cutoff never passes the newest
  snapshot the ledger ingested (plan.py, run order D1), so the commit log
  cannot miss a commit the advisor expires.

The window protects time travel and undo, not readers already running: on user tables the expiry deletes no
files (freed_files.py), and the freed files wait out the FILE GRACE, first match:

  1. advisor.expire.file-grace-hours           the owner's word for this table
  2. advisor.expire.reader-timeout-hours       a declared timeout every reader of
     + expiry.clock_margin_minutes             the table runs under (no floor)
  3. expiry.file_grace_hours (12)              no timeout known: an assumption
  floored at expiry.file_grace_floor_hours (1) unless it comes from 2.

Refs: expire_snapshots never removes a snapshot a tag or branch points at, nor
the ancestors a branch's own retention keeps. Snapshots behind a ref other than
main are left out of the expirable count, and a ref whose head is older than
expiry.stale_ref_hours is reported (STALE_REF): it keeps its files alive and
blocks cleanup.

Retained bytes without reading manifests (retained_bytes mode "ledger"): in a
straight-line history a file removed by snapshot i was live in i's parent; if
that parent is still retained, only old snapshots keep the file. So

  retained = sum of removed-files-size over the current snapshot's retained
             ancestors, except the oldest retained ancestor

Unknown (None, with the reason) when the history isn't one line from the oldest
retained snapshot to the current one, when refs other than main exist, or when
a snapshot has no size summary at all (writers may omit summaries; Iceberg's own
leave removed-files-size out when nothing was removed, but always write the
totals, so a missing key with totals present counts as 0).

Pure Python; the ledger feeds it the snapshots and refs it already loaded.
"""
import json

H = 3600000


def category(snaps, now_ms, cfg):
    exp = cfg.get("expiry") or {}
    n = sum(1 for s in snaps if s["operation"] != "replace" and s["ts_ms"] >= now_ms - 24 * H)
    streaming = n >= int(exp.get("streaming_min_commits_24h", 48))
    return ("streaming" if streaming else "batch"), n


def resolve_policy(props, cfg, snaps, now_ms):
    """-> {age_h, min_keep, source, category, writer_commits_24h,
    max_snapshots, file_grace_h, grace_source}."""
    exp = cfg.get("expiry") or {}
    cat, n24 = category(snaps, now_ms, cfg)
    defaults = (exp.get(cat) or {})
    age_h = float(defaults.get("max_age_hours", 120 if cat == "batch" else 72))
    keep = int(defaults.get("min_snapshots", 10))
    source = f"config expiry.{cat}"
    props = props or {}
    if props.get("advisor.expire.max-age-hours") or props.get("advisor.expire.min-snapshots"):
        age_h = float(props.get("advisor.expire.max-age-hours") or age_h)
        keep = int(props.get("advisor.expire.min-snapshots") or keep)
        source = "table property advisor.expire.*"
    if props.get("history.expire.max-snapshot-age-ms") or props.get("history.expire.min-snapshots-to-keep"):
        if props.get("history.expire.max-snapshot-age-ms"):
            age_h = int(props["history.expire.max-snapshot-age-ms"]) / H
        if props.get("history.expire.min-snapshots-to-keep"):
            keep = int(props["history.expire.min-snapshots-to-keep"])
        source = "table property history.expire.*"
    # metadata.json cost: at most max_snapshots inside the window
    max_snaps = int(props.get("advisor.expire.max-snapshots") or exp.get("max_snapshots", 500))
    max_snaps = max(max_snaps, keep)
    newest = sorted((s["ts_ms"] for s in snaps), reverse=True)
    inside = [t for t in newest if t >= now_ms - age_h * H]
    if len(inside) > max_snaps:
        capped_h = (now_ms - newest[max_snaps - 1]) / H
        if capped_h < age_h:
            age_h, source = capped_h, source + f" (shortened to keep {max_snaps} snapshots)"
    grace_h, grace_source = file_grace(props, exp)
    return {"age_h": age_h, "min_keep": keep, "source": source, "category": cat,
            "writer_commits_24h": n24, "max_snapshots": max_snaps,
            "file_grace_h": grace_h, "grace_source": grace_source}


def file_grace(props, exp):
    """How long files an expiry freed are kept: -> (hours, source)."""
    props = props or {}
    floor = float(exp.get("file_grace_floor_hours", 1))
    margin_h = float(exp.get("clock_margin_minutes", 10)) / 60
    if props.get("advisor.expire.file-grace-hours"):
        g, src = float(props["advisor.expire.file-grace-hours"]), "table property advisor.expire.file-grace-hours"
    elif props.get("advisor.expire.reader-timeout-hours"):
        g = float(props["advisor.expire.reader-timeout-hours"]) + margin_h
        return g, f"declared reader timeout {float(props['advisor.expire.reader-timeout-hours']):g} h + margin"
    else:
        g, src = float(exp.get("file_grace_hours", 12)), "config expiry.file_grace_hours (no reader timeout known)"
    if g < floor:
        g, src = floor, src + f" (raised to the {floor:g} h floor)"
    return g, src


def ancestors(by_id, current):
    chain, sid = [], current
    while sid is not None and sid in by_id:
        chain.append(sid)
        sid = by_id[sid]["parent_id"]
    return chain                         # newest first


def expiry_facts(snaps, refs, current, now_ms, policy, cfg):
    """What expire_snapshots(older_than = now - age, retain_last = min_keep) would
    remove under the policy, and the refs that keep snapshots alive."""
    exp = cfg.get("expiry") or {}
    by_id = {s["snapshot_id"]: s for s in snaps}
    chain = ancestors(by_id, current)
    keep_ids = set(chain[:policy["min_keep"]])
    other_refs = [r for r in (refs or []) if r.get("name") != "main"]
    pinned = {r["snapshot_id"] for r in other_refs}
    cutoff = now_ms - policy["age_h"] * H
    expirable = [s for s in snaps if s["ts_ms"] < cutoff and s["snapshot_id"] not in keep_ids
                 and s["snapshot_id"] not in pinned and s["snapshot_id"] != current]
    stale_h = float(exp.get("stale_ref_hours", 720))
    ref_info, stale = [], []
    for r in other_refs:
        head = by_id.get(r["snapshot_id"])
        age = None if head is None else round((now_ms - head["ts_ms"]) / H, 2)
        info = {"name": r["name"], "type": r.get("type"), "head_age_h": age,
                "max_ref_age_ms": r.get("max_ref_age_ms")}
        ref_info.append(info)
        if age is not None and age >= stale_h and not r.get("max_ref_age_ms"):
            stale.append(info)
    oldest = min((s["ts_ms"] for s in expirable), default=None)
    return {"policy_age_h": policy["age_h"], "policy_min_keep": policy["min_keep"],
            "policy_source": policy["source"], "write_category": policy["category"],
            "policy_file_grace_h": policy.get("file_grace_h"), "policy_grace_source": policy.get("grace_source"),
            "writer_commits_24h": policy["writer_commits_24h"],
            "expirable_snapshots": len(expirable),
            "oldest_expirable_age_h": None if oldest is None else round((now_ms - oldest) / H, 2),
            "refs_json": json.dumps(ref_info), "stale_refs_json": json.dumps(stale),
            "stale_refs": len(stale)}


def retained_from_summaries(snaps, refs, current):
    """-> (bytes or None, note)."""
    if current is None:
        return 0, "no current snapshot"
    by_id = {s["snapshot_id"]: s for s in snaps}
    chain = ancestors(by_id, current)
    others = [r for r in (refs or []) if r.get("name") != "main"]
    if refs is None:
        return None, "refs unknown"
    if others:
        return None, f"refs other than main: {', '.join(r['name'] for r in others)}"
    if len(chain) != len(snaps):
        return None, f"{len(snaps) - len(chain)} snapshot(s) off the current lineage"
    total = 0
    for sid in chain[:-1]:                       # every retained ancestor but the oldest
        s = by_id[sid]
        v = s.get("removed_files_size")
        if v is None:
            # Iceberg's writers leave the key out when nothing was removed, but
            # always write the totals: totals present + key absent = 0 removed
            if s.get("total_files_size") is None:
                return None, f"snapshot {sid} has no summary sizes"
            continue
        total += int(v)
    return total, f"summary formula over {len(chain)} snapshots"


def retained_detail(snaps, refs, current, now_ms, policy, what_if_hours=(72, 24, 6)):
    """Where the retained bytes come from and what a shorter policy would keep,
    from the same summary formula (RETAINED_STORAGE advice). -> dict or None
    when the formula doesn't hold (see retained_from_summaries).
      expirable_bytes / inside_policy_bytes   what an expiry to the policy would
                     free now / what would still be retained after it
      by_operation   retained bytes by the operation that removed them: replace =
                     rewrites (compaction), overwrite / delete = writers (a
                     copy-on-write MERGE, UPDATE or DELETE copies whole files)
      writer_removed_24h   bytes writers removed in the last 24 h: what each day
                     of retention costs while they keep writing like this
      what_if        [{hours, retained_bytes}] for windows shorter than the
                     policy: what would still be retained right after an
                     expiry to that window (min_keep still kept)."""
    total, _ = retained_from_summaries(snaps, refs, current)
    if total is None or current is None:
        return None
    by_id = {s["snapshot_id"]: s for s in snaps}
    chain = ancestors(by_id, current)            # newest first

    def removed(s):
        return int(s.get("removed_files_size") or 0)

    by_op = {}
    for sid in chain[:-1]:
        s = by_id[sid]
        if removed(s):
            by_op[s["operation"]] = by_op.get(s["operation"], 0) + removed(s)
    w24 = sum(removed(by_id[sid]) for sid in chain
              if by_id[sid]["operation"] != "replace" and by_id[sid]["ts_ms"] >= now_ms - 24 * H)
    def kept_after(h):
        kept = [sid for i, sid in enumerate(chain)
                if i < max(int(policy["min_keep"]), 1) or by_id[sid]["ts_ms"] >= now_ms - h * H]
        return sum(removed(by_id[sid]) for sid in kept[:-1])

    inside = kept_after(float(policy["age_h"]))
    what_if = [{"hours": h, "retained_bytes": kept_after(h)}
               for h in sorted({float(x) for x in what_if_hours}, reverse=True) if h < float(policy["age_h"])]
    return {"retained_bytes": total, "expirable_bytes": total - inside, "inside_policy_bytes": inside,
            "by_operation": by_op, "writer_removed_24h": w24, "what_if": what_if}
