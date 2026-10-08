"""GL2.5d: the catalog scan in one Spark job: metrics -> symptoms -> scorecard.

One job instead of three keeps it to a single pod start, and s3 is measured
first so a freshly built hot partition is still inside the hot window.

Usage (via scripts/run-job.sh py gl_scan.py ...):
  gl_scan.py [--namespace glue.demo] [--no-scorecard] [--full] [--tables a,b] [--trace a,b]
  gl_scan.py --profile config/profile.json --group <name> [--shard i/N] [--full]

With --group the tables come from the profile's group selector (and shard);
the run claims (group, shard) first and exits quietly if another run holds it,
claims each table before measuring it, does the store's retention pass and
the ops-table upkeep (ops_maintenance.py) only when it holds the housekeeping claim, and writes a run journal row
(glue.ops.run_journal) and one coverage row per table (glue.ops.coverage).

--trace prints every step for the named tables (lines start with "TRACE <table> |");
without --tables only the traced tables are scanned. A partial scan becomes the
latest scan: run a full make gl-scan before make gl-plan.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession

import coordinator as coordination
import gl_common as gl
import gltrace
import ops_maintenance
import probes
import profiles
from detect_symptoms import run_detect
from scan_metrics import run_scan
from scorecard import run_scorecard

HERE = os.path.dirname(os.path.abspath(__file__))

JOURNAL_DDL = """
    run_id STRING, target STRING, group_name STRING, shard STRING, scan_id STRING,
    started_at TIMESTAMP, ended_at TIMESTAMP, status STRING, tables_matched BIGINT,
    tables_done BIGINT, tables_skipped BIGINT, tables_failed BIGINT, housekeeping BOOLEAN,
    rule_version STRING, note STRING"""


import state_store as ss  # noqa: E402

ss.declare_log("run_journal", JOURNAL_DDL, "started_at")


def journal(log, row):
    """One run journal row, written at once (a run that dies still leaves it)."""
    log.append("run_journal", [row])
    log.flush()


def group_tables(spark, profile, group, shard):
    """The group's tables in this shard, as {namespace: [short names]}."""
    def list_tables(ns):
        return [r.tableName for r in spark.sql(f"SHOW TABLES IN {ns}").collect()]

    def props_of(full):
        return probes.table_info(spark, full).get("properties") or {}
    out = {}
    for full in profiles.select(profile, group, list_tables, props_of, shard):
        ns, short = full.rsplit(".", 1)
        out.setdefault(ns, []).append(short)
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--namespace", default="glue.demo")
    p.add_argument("--config", default=os.path.join(HERE, "config", "health.json"))
    p.add_argument("--expectations", default=os.path.join(HERE, "config", "expectations.json"))
    p.add_argument("--priority", default="s3_hot_partition",
                   help="comma-separated tables to measure first")
    p.add_argument("--no-scorecard", action="store_true")
    p.add_argument("--full", action="store_true", help="measure every table, even unchanged ones")
    p.add_argument("--tables", default="", help="comma-separated table names: scan only these")
    p.add_argument("--trace", default="",
                   help="comma-separated table names to trace step by step (TRACE lines); "
                        "without --tables only these tables are scanned")
    p.add_argument("--profile", default="", help="profile JSON (with --group)")
    p.add_argument("--group", default="", help="run one group of the profile")
    p.add_argument("--shard", default="0/1", help="i/N: this run's share of the group")
    a = p.parse_args()
    if a.group:
        return run_group(a)

    config = gl.load_config(a.config)
    traced = [t.strip() for t in a.trace.split(",") if t.strip()]
    tables = [t.strip() for t in a.tables.split(",") if t.strip()] or traced
    gltrace.enable(traced)
    spark = SparkSession.builder.appName("gl25-scan").getOrCreate()
    scan_id = run_scan(spark, a.namespace, config, tables=tables, report=False, full=a.full,
                       priority=[t.strip() for t in a.priority.split(",") if t.strip()])
    run_detect(spark, scan_id, config)
    if not a.no_scorecard:
        run_scorecard(spark, scan_id, config, gl.load_config(a.expectations), partial=bool(tables))
    if not tables:                     # a single full run does the housekeeping itself
        ops_maintenance.maintain(spark, config, scan_id)
    spark.stop()


def run_group(a):
    import symptom_rules
    config = gl.load_config(a.config)
    profile = profiles.load_profile(a.profile or os.path.join(HERE, "config", "profile.json"))
    shard = profiles.parse_shard(a.shard)
    shard_s = f"{shard[0]}/{shard[1]}"
    gcfg = profile["groups"][a.group]
    run_id = gl.new_run_id(f"run-{a.group}")
    coord = coordination.make(profile, run_id)
    spark = SparkSession.builder.appName(f"gl-group-{a.group}").getOrCreate()
    log = ss.make_log_sink(spark, config)
    started = probes.now_utc()
    base = {"run_id": run_id, "target": profile["target"], "group_name": a.group, "shard": shard_s,
            "started_at": started, "rule_version": symptom_rules.RULE_VERSION}
    if not coord.claim_group(a.group, shard_s, ttl_s=int(gcfg.get("claim_hours", 3) * 3600)):
        print(f"=== Group {a.group} shard {shard_s}: another run holds it; exiting ===", flush=True)
        journal(log, dict(base, ended_at=probes.now_utc(), status="skipped: group held"))
        spark.stop()
        return
    try:
        by_ns = group_tables(spark, profile, a.group, shard)
        matched = sum(len(v) for v in by_ns.values())
        print(f"=== Group {a.group} shard {shard_s} ({profile['target']}): {matched} tables, run {run_id} ===",
              flush=True)
        hk = coord.claim_housekeeping(float(gcfg.get("housekeeping_every_hours", 6)),
                                      ttl_s=int(float(gcfg.get("housekeeping_claim_minutes", 60)) * 60))
        housekeeping = hk
        print(f"  housekeeping (retention pass, ops-table upkeep) in this run: {hk}", flush=True)
        done = skipped = failed = 0
        scan_ids = []
        for ns, tables in sorted(by_ns.items()):
            run = {"run_id": run_id, "target": profile["target"], "group": a.group, "shard": shard_s}
            scan_id = run_scan(spark, ns, config, tables=tables, report=False, full=a.full, coord=coord, run=run,
                               housekeeping=housekeeping, priority=[t for t in a.priority.split(",") if t])
            scan_ids.append(scan_id)
            run_detect(spark, scan_id, config)
            if not a.no_scorecard:
                run_scorecard(spark, scan_id, config, gl.load_config(a.expectations), partial=True)
            if housekeeping:                        # still holding the claim: upkeep of the ops tables
                ttl = int(float(gcfg.get("housekeeping_claim_minutes", 60)) * 60)
                ops_maintenance.maintain(spark, config, run_id, renew=lambda: coord.renew_housekeeping(ttl))
                coord.housekeeping_done()
            housekeeping = False                    # once per run, with the first namespace
            for r in ss.make_log_sink(spark, config).rows("coverage", eq={"scan_id": scan_id}):
                done += r["status"] == "done"
                skipped += r["status"] == "claimed_elsewhere"
                failed += r["status"] == "failed"
        journal(log, dict(base, scan_id=",".join(scan_ids), ended_at=probes.now_utc(), status="ok",
                            tables_matched=matched, tables_done=done, tables_skipped=skipped,
                            tables_failed=failed, housekeeping=hk and bool(scan_ids)))
        print(f"=== Group {a.group} shard {shard_s}: {done} done, {skipped} held by other runs, "
              f"{failed} failed of {matched} ===", flush=True)
    except Exception as e:
        journal(log, dict(base, ended_at=probes.now_utc(), status="failed", note=f"{type(e).__name__}: {e}"[:500]))
        raise
    finally:
        coord.release_group(a.group, shard_s)
    spark.stop()


if __name__ == "__main__":
    main()
