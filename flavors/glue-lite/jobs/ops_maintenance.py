"""Upkeep of the advisor's own tables (glue.ops.*).

Every scan appends small files and snapshots to the ops tables, and the
retention pass deletes rows, so without upkeep they fill with tiny files and
old snapshots and every read of them slows down. This runs inside the
housekeeping pass (one run at a time holds that claim), checks each ops table
from its metadata, and acts only past a threshold:

  check (no data read)                               action
  -------------------------------------------------  ----------------------------
  write.metadata.* properties not as configured      ALTER TABLE SET TBLPROPERTIES
  data files a rewrite would remove >= min_excess    rewrite_data_files
  manifests in the current snapshot > max_manifests  rewrite_manifests
  snapshots not current at any time in the last      expire_snapshots(older_than =
    safe age, beyond the newest retain_last,           safe cutoff, retain_last)
    >= min_expire

Facts: snapshot count and timestamps, current-snapshot summary (total data
files and bytes) and manifest count from the Iceberg Java API; the partition
breakdown (`<table>.partitions`, manifests only) only when the summary says a
rewrite may pay off.

Other runs keep writing while this runs:
  appends            commit alongside a rewrite (a rewrite conflicts only with
                     a commit that removed the files it rewrites)
  MERGE / DELETE     a clash fails one side; that side is retried
  readers            expire_snapshots keeps every snapshot that was current at
                     any time in the last safe age (longer than any run), found
                     from the snapshot log (safe_cutoff_ms), so no run in
                     progress loses the files it is reading
  orphan files       not removed: files another run has written but not yet
                     committed look like orphans

Standalone (make gl-ops-maintain [DRY=1]):
  ops_maintenance.py [--dry-run] [--profile config/profile.json]
takes the housekeeping claim when a profile with coordination is given.
"""
import argparse
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

DEFAULTS = {
    "enabled": True,
    "min_input_files": 5,              # a partition with fewer files is left alone
    "min_excess_files": 20,            # rewrite when it would remove at least this many files
    "target_file_bytes": 134217728,
    "max_manifests": 50,
    "retain_last": 20,
    "safe_age_hours": 6,               # never expire a snapshot younger than this
    "min_expire": 10,                  # expire when at least this many snapshots would go
    "properties": {"write.metadata.delete-after-commit.enabled": "true",
                   "write.metadata.previous-versions-max": "20"},
    "log_keep_days": 90,
}

LOG_DDL = """
    run_id STRING, table_name STRING, checked_at TIMESTAMP, data_files BIGINT, data_bytes BIGINT,
    partitions BIGINT, excess_files BIGINT, manifests BIGINT, snapshots BIGINT,
    expirable_snapshots BIGINT, actions STRING, result STRING, seconds DOUBLE"""


def _declare_log():
    import state_store as ss
    ss.declare_log("ops_maintenance", LOG_DDL, "checked_at")


_declare_log()


def settings(config):
    given = dict((config or {}).get("ops_maintenance") or {})
    props = dict(DEFAULTS["properties"], **given.pop("properties", {}))
    return dict(DEFAULTS, **given, properties=props)


def safe_cutoff_ms(log, commits, now_ms, safe_age_ms):
    """older_than for expire_snapshots that keeps every snapshot which was
    current at any time in the last safe_age (a run may have planned a read on
    it then). A snapshot's own commit time is not enough: one committed 7 h ago
    and current until a minute ago is still being read.

    log: the table's snapshot log [(made_current_ms, snapshot_id)], oldest first;
    commits: {snapshot_id: commit_ms}. The snapshots current since T = now -
    safe_age are the last log entry at or before T plus every entry after it;
    the cutoff is the earliest commit time among them (a rollback can make an
    older snapshot current again). With no log entry at or before T, the
    newest snapshot committed by T stands in. None: nothing may expire."""
    T = now_ms - safe_age_ms
    before = [sid for ts, sid in log if ts <= T][-1:]
    after = [sid for ts, sid in log if ts > T]
    if not before:
        older = [(ts, sid) for sid, ts in commits.items() if ts <= T]
        before = [max(older)[1]] if older else []
    keep = [commits[sid] for sid in set(before + after) if sid in commits]
    return min(keep) if keep else None


def expirable(snapshots, current_id, cutoff_ms, retain_last):
    """How many snapshots expire_snapshots(older_than=cutoff, retain_last) would
    remove: committed before the cutoff, not among the newest retain_last, not
    the current one. snapshots: [(snapshot_id, timestamp_ms)]; cutoff None: 0."""
    if cutoff_ms is None:
        return 0
    newest = {sid for sid, _ in sorted(snapshots, key=lambda s: -s[1])[:max(int(retain_last), 1)]}
    return sum(1 for sid, ts in snapshots if ts < cutoff_ms and sid not in newest and sid != current_id)


def decide(facts, cfg, partitions=None):
    """-> [(action, reason)] for one table. facts from table_facts(); partitions:
    {"partitions", "excess_files"} from partition_facts() or None (not read)."""
    out = []
    props = facts.get("properties") or {}
    want = {k: v for k, v in cfg["properties"].items() if props.get(k) != v}
    if want:
        out.append(("set_properties", ", ".join(f"{k}={v}" for k, v in sorted(want.items()))))
    if partitions and partitions["excess_files"] >= cfg["min_excess_files"]:
        out.append(("rewrite_data_files", f"{partitions['excess_files']} files would go "
                                          f"({facts['data_files']} in {partitions['partitions']} partitions)"))
    if facts["manifests"] > cfg["max_manifests"]:
        out.append(("rewrite_manifests", f"{facts['manifests']} manifests > {cfg['max_manifests']}"))
    if facts["expirable"] >= cfg["min_expire"]:
        out.append(("expire_snapshots", f"{facts['expirable']} of {facts['snapshots']} snapshots not current in "
                                        f"the last {float(cfg['safe_age_hours']):.4g} h, beyond the newest {cfg['retain_last']}"))
    return out


def may_rewrite(facts, cfg):
    """The cheap gate before reading the partition breakdown."""
    return facts["data_files"] >= cfg["min_excess_files"] + 1


def table_facts(spark, table, now_ms, safe_age_ms, retain_last):
    """Metadata only: snapshots, snapshot log, current summary, manifest count,
    properties; the table's safe expiry cutoff."""
    jt = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    jt.refresh()                                  # the catalog may hand back a cached table
    meta = jt.operations().current()
    snaps = [(int(s.snapshotId()), int(s.timestampMillis())) for s in meta.snapshots()]
    log = [(int(e.timestampMillis()), int(e.snapshotId())) for e in meta.snapshotLog()]
    cur = jt.currentSnapshot()
    facts = {"snapshots": len(snaps), "data_files": 0, "data_bytes": 0, "manifests": 0, "current_id": None}
    if cur is not None:
        summ = cur.summary()
        facts["current_id"] = int(cur.snapshotId())
        facts["data_files"] = int(summ.get("total-data-files") or 0)
        facts["data_bytes"] = int(summ.get("total-files-size") or 0)
        facts["manifests"] = int(cur.allManifests(jt.io()).size())
    props = jt.properties()
    facts["properties"] = {str(k): str(props.get(k)) for k in props.keySet().toArray()}
    facts["cutoff_ms"] = safe_cutoff_ms(log, dict(snaps), now_ms, safe_age_ms)
    facts["expirable"] = expirable(snaps, facts["current_id"], facts["cutoff_ms"], retain_last)
    return facts


def partition_facts(spark, table, cfg):
    """Files a rewrite would remove: in partitions with at least min_input_files,
    the files beyond what the partition's bytes need at the target size."""
    tgt, mn = int(cfg["target_file_bytes"]), int(cfg["min_input_files"])
    r = spark.sql(f"""
        SELECT count(*) AS partitions,
               coalesce(sum(CASE WHEN file_count >= {mn}
                   THEN greatest(file_count - greatest(ceil(total_data_file_size_in_bytes / {tgt}), 1), 0)
                   ELSE 0 END), 0) AS excess_files
        FROM {table}.partitions""").collect()[0]
    return {"partitions": int(r.partitions), "excess_files": int(r.excess_files)}


def statements(table, actions, cfg, cutoff_ms=None):
    """SQL for each action (CALL takes the catalog-relative name)."""
    import gl_common as gl
    ident = gl.catalog_relative(table)
    # the table's own safe cutoff, with milliseconds: exactly what the checks counted
    cutoff = (datetime.fromtimestamp(cutoff_ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
              if cutoff_ms is not None else None)
    out = []
    for action, _ in actions:
        if action == "set_properties":
            kv = ", ".join(f"'{k}'='{v}'" for k, v in sorted(cfg["properties"].items()))
            out.append((action, f"ALTER TABLE {table} SET TBLPROPERTIES ({kv})"))
        elif action == "rewrite_data_files":
            out.append((action, f"CALL glue.system.rewrite_data_files(table => '{ident}', options => map("
                                f"'min-input-files', '{int(cfg['min_input_files'])}', "
                                f"'target-file-size-bytes', '{int(cfg['target_file_bytes'])}', "
                                f"'partial-progress.enabled', 'true'))"))
        elif action == "rewrite_manifests":
            out.append((action, f"CALL glue.system.rewrite_manifests(table => '{ident}')"))
        elif action == "expire_snapshots" and cutoff:
            out.append((action, f"CALL glue.system.expire_snapshots(table => '{ident}', "
                                f"older_than => TIMESTAMP '{cutoff}+00:00', "
                                f"retain_last => {int(cfg['retain_last'])})"))
    return out


def ops_tables(spark, ops):
    return sorted(f"{ops}.{r.tableName}" for r in spark.sql(f"SHOW TABLES IN {ops}").collect())


def expire_postgres_logs(spark, config, keep_days):
    """Logs on Postgres: delete rows older than keep_days from every declared log
    kind (the Iceberg logs keep their history; their tables get the upkeep above)."""
    import state_store as ss
    if ss.backend_of(config, "logs") != "postgres":
        return {}
    import importlib
    for m in ("scan_metrics", "detect_symptoms", "ledger", "plan", "scorecard", "gl_scan"):
        try:
            importlib.import_module(m)          # their declare_log calls
        except Exception:
            pass
    out = {}
    try:
        log = ss.make_log_sink(spark, config)
        for kind in sorted(ss.LOG_KINDS):
            if kind == "run_journal":
                continue                            # a run's own journal row may still be open
            log.expire(kind, keep_days)
            out[kind] = keep_days
        print(f"  postgres logs: rows older than {keep_days} days removed from {len(out)} kinds", flush=True)
    except Exception as e:
        print(f"  (postgres log retention skipped: {type(e).__name__}: {str(e)[:160]})", flush=True)
    return out


def maintain(spark, config, run_id, renew=lambda: True, dry_run=False, ops=None, now=None):
    """Upkeep that never fails the run calling it (see _maintain)."""
    try:
        return _maintain(spark, config, run_id, renew, dry_run, ops, now)
    except Exception as e:
        print(f"  (ops-table upkeep stopped: {type(e).__name__}: {str(e)[:200]})", flush=True)
        return []


def _maintain(spark, config, run_id, renew=lambda: True, dry_run=False, ops=None, now=None):
    """Check every ops table and act past the thresholds. renew() extends the
    housekeeping claim before each table; False (taken over) stops here.
    -> log rows (also appended to <ops>.ops_maintenance unless dry_run)."""
    import gl_common as gl
    from state_store import retry_on_conflict
    cfg = settings(config)
    ops = ops or gl.OPS_NAMESPACE
    if not cfg["enabled"]:
        print("  ops-table upkeep: disabled", flush=True)
        return []
    now = now or datetime.now(timezone.utc)
    now_ms, safe_ms = int(now.timestamp() * 1000), int(float(cfg["safe_age_hours"]) * 3600 * 1000)
    rows, t0 = [], time.time()
    print(f"\n=== Ops-table upkeep ({ops}{', dry run' if dry_run else ''}) ===", flush=True)
    for table in ops_tables(spark, ops):
        if not dry_run and not renew():
            print("  housekeeping claim taken over by another run: stopping", flush=True)
            break
        start = time.time()
        row = {"run_id": run_id, "table_name": table, "checked_at": now, "actions": "", "result": "ok"}
        try:
            f = table_facts(spark, table, now_ms, safe_ms, cfg["retain_last"])
            parts = partition_facts(spark, table, cfg) if may_rewrite(f, cfg) else None
            acts = decide(f, cfg, parts)
            row.update(data_files=f["data_files"], data_bytes=f["data_bytes"], manifests=f["manifests"],
                       snapshots=f["snapshots"], expirable_snapshots=f["expirable"],
                       partitions=parts and parts["partitions"], excess_files=parts and parts["excess_files"],
                       actions=",".join(a for a, _ in acts))
            print(f"  {table.rsplit('.', 1)[-1]:22} files {f['data_files']:>6}  manifests {f['manifests']:>4}  "
                  f"snapshots {f['snapshots']:>5} (expirable {f['expirable']})"
                  f"{'  excess ' + str(parts['excess_files']) if parts else ''}", flush=True)
            for action, reason in acts:
                print(f"      -> {action}: {reason}", flush=True)
            if not dry_run:
                for action, sql in statements(table, acts, cfg, f["cutoff_ms"]):
                    retry_on_conflict(lambda: spark.sql(sql).collect(), f"{action} {table}")
        except Exception as e:                    # one table failing never stops the others
            row["result"] = f"{type(e).__name__}: {e}"[:300]
            print(f"  {table.rsplit('.', 1)[-1]:22} FAILED {row['result'][:160]}", flush=True)
        row["seconds"] = round(time.time() - start, 2)
        rows.append(row)
    acted = sum(1 for r in rows if r["actions"])
    print(f"  {len(rows)} ops tables checked, {acted} needed upkeep, {time.time() - t0:.1f}s", flush=True)
    if not dry_run and rows:
        try:
            import state_store as ss
            log = ss.make_log_sink(spark, config, ops)
            log.append("ops_maintenance", rows)
            log.flush()
            log.expire("ops_maintenance", int(cfg["log_keep_days"]))
        except Exception as e:
            print(f"  (upkeep log not written: {type(e).__name__})", flush=True)
    if not dry_run:
        expire_postgres_logs(spark, config, int(cfg["log_keep_days"]))
    return rows


def main():
    import coordinator as coordination
    import gl_common as gl
    import profiles
    from pyspark.sql import SparkSession
    here = os.path.dirname(os.path.abspath(__file__))
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=os.path.join(here, "config", "health.json"))
    p.add_argument("--profile", default=os.path.join(here, "config", "profile.json"))
    p.add_argument("--dry-run", action="store_true", help="print the checks and actions, change nothing")
    a = p.parse_args()
    config = gl.load_config(a.config)
    profile = profiles.load_profile(a.profile) if a.profile and os.path.exists(a.profile) else {}
    run_id = gl.new_run_id("ops-upkeep")
    coord = coordination.make(profile, run_id)
    spark = SparkSession.builder.appName("gl-ops-upkeep").getOrCreate()
    if a.dry_run:
        maintain(spark, config, run_id, dry_run=True)
    elif coord.claim_housekeeping(every_hours=0):
        try:
            maintain(spark, config, run_id, renew=coord.renew_housekeeping)
        finally:
            coord.housekeeping_done(record=False)     # the retention pass keeps its own schedule
    else:
        print("=== Another run holds the housekeeping claim: exiting ===", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
