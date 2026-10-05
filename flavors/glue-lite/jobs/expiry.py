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
  then the floor for work in flight: the age is never under expiry.inflight_floor_hours
  (24 h by default; a long query or write still needs its starting snapshot).

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
    """-> {age_h, min_keep, source, category, writer_commits_24h, floor_h}."""
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
    floor_h = float(exp.get("inflight_floor_hours", 24))
    if age_h < floor_h:
        age_h, source = floor_h, source + f" (raised to the {floor_h:g} h in-flight floor)"
    return {"age_h": age_h, "min_keep": keep, "source": source, "category": cat,
            "writer_commits_24h": n24, "floor_h": floor_h}


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
