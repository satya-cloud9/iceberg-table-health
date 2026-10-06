"""Profiles and groups (migration step 2).

A profile is one target (a catalog and its namespaces) with any number of
table groups. Each group has a selector, a mode, and how many shards may run
it in parallel. Groups run as separate jobs; a group's shards split its tables
by hash so they never overlap.

Selector keys (all optional; a table must pass every key that is set):

  namespaces  ["glue.demo", ...]       namespaces to list (default: the profile's)
  include     ["s1*", "events"]        glob patterns on the short table name
  exclude     ["*_tmp"]                glob patterns removed after include
  tables      ["s0_small_appends"]     an explicit list (short or full names)
  property    "advisor.group=batch"    a table property equal to a value (reads
                                       each candidate's properties: use with a
                                       namespace or include filter on big catalogs)

The shard of a table is crc32(full name) % shards. The full name is used,
not the table UUID, so a selection needs no table load; a renamed table may
change shard, and the per-table claim covers that window.

Pure Python: listing tables and reading properties are callables passed in.
"""
import fnmatch
import json
import zlib


def load_profile(path):
    with open(path, encoding="utf-8") as f:
        prof = json.load(f)
    prof.setdefault("target", "default")
    prof.setdefault("namespaces", ["glue.demo"])
    prof.setdefault("groups", {})
    for g in prof["groups"].values():
        g.setdefault("select", {})
        g.setdefault("shards", 1)
    return prof


def parse_shard(text):
    """'1/4' -> (1, 4); '' or None -> (0, 1)."""
    if not text:
        return 0, 1
    i, n = (int(x) for x in str(text).split("/"))
    if n < 1 or not 0 <= i < n:
        raise ValueError(f"bad shard {text!r}: want i/N with 0 <= i < N")
    return i, n


def shard_of(full_name, n):
    return zlib.crc32(full_name.encode("utf-8")) % n


def _matches(short, patterns):
    return any(fnmatch.fnmatchcase(short, p) for p in patterns)


def select(profile, group, list_tables, props_of=None, shard=(0, 1)):
    """-> sorted full table names of `group` in this shard.

    list_tables(namespace) -> short names; props_of(full_name) -> dict of
    table properties (only called when the group selects by property)."""
    g = profile["groups"].get(group)
    if g is None:
        raise KeyError(f"no group {group!r} in profile {profile.get('target')!r}; "
                       f"groups: {sorted(profile['groups'])}")
    sel = g.get("select") or {}
    namespaces = sel.get("namespaces") or profile["namespaces"]
    explicit = set(sel.get("tables") or [])
    out = []
    for ns in namespaces:
        for short in list_tables(ns):
            full = f"{ns}.{short}"
            if explicit and short not in explicit and full not in explicit:
                continue
            if sel.get("include") and not _matches(short, sel["include"]):
                continue
            if sel.get("exclude") and _matches(short, sel["exclude"]):
                continue
            if sel.get("property"):
                key, _, want = sel["property"].partition("=")
                if props_of is None or str((props_of(full) or {}).get(key.strip())) != want.strip():
                    continue
            out.append(full)
    i, n = shard
    return sorted(t for t in out if n == 1 or shard_of(t, n) == i)


def overlaps(profile, list_tables, props_of=None):
    """Tables selected by more than one group: {table: [groups]} (a profile
    check; overlapping groups are allowed but double the work)."""
    seen = {}
    for name in profile["groups"]:
        for t in select(profile, name, list_tables, props_of):
            seen.setdefault(t, []).append(name)
    return {t: gs for t, gs in seen.items() if len(gs) > 1}
