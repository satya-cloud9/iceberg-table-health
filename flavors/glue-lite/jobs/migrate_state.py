"""Copy the advisor's state (and optionally its logs) from the Iceberg tables in
glue.ops into Postgres, once, before switching state.backend / logs.backend to
postgres (make gl-pg-migrate [LOGS=1] [REPLACE=1]).

State kinds are copied as the Iceberg store reads them (latest kinds: the newest
row per key; counters: their rows, summed per bucket on the way in), so a
Postgres run continues exactly where the Iceberg runs left off: action history,
ledger watermarks and commit history, partition state and facts, freed files.

A kind that already has rows in Postgres is skipped (counters would double),
unless --replace empties it first. Logs (--logs) are appended as they are; run
them once, or with --replace to start the Postgres copy over.

Nothing is changed on the Iceberg side. At the end the row counts per kind are
compared (Iceberg rows read vs Postgres rows stored).
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gl_common as gl
import pg_store
import state_store as ss



def _stored(conn, schema, table):
    r = conn.run("SELECT to_regclass(:t) IS NOT NULL", {"t": f"{schema}.{table}"})
    if not (r and r[0][0]):
        return 0
    return int(conn.run(f"SELECT count(*) FROM {schema}.{table}")[0][0])


def copy_state(spark, config, replace=False, kinds=None):
    src = ss.IcebergStateStore(spark, gl.OPS_NAMESPACE)
    dst = pg_store.PostgresStateStore(config)
    conn, schema = dst.conn, dst.schema
    out = []
    for kind in kinds or ss.KINDS:
        k = ss.KINDS[kind]
        t0 = time.time()
        rows = [{c: ss._utc_naive(v) for c, v in r.items()} for r in src._load(kind)]
        before = _stored(conn, schema, k.table)
        if before and not replace:
            out.append((kind, len(rows), before, "skipped: Postgres already has rows (--replace to copy again)"))
            continue
        if before:
            conn.run(f"TRUNCATE {schema}.{k.table}")
        if rows:
            dst._ddl(kind)
            with pg_store._Tx(conn):
                dst._write(kind, rows)
        after = _stored(conn, schema, k.table)
        keys = len({tuple(pg_store._key_text(r.get(c)) for c in dst._keys(k)) for r in rows})
        note = "ok" if after == keys else f"MISMATCH: {keys} distinct keys read, {after} stored"
        out.append((kind, len(rows), after, f"{note} ({time.time() - t0:.1f}s)"))
    return out


def copy_logs(spark, config, replace=False):
    ss.declare_all()
    sink = pg_store.PostgresLogSink(config)
    conn, schema = sink.conn, sink.schema
    out = []
    for kind in sorted(ss.LOG_KINDS):
        t = f"{gl.OPS_NAMESPACE}.{kind}"
        if not spark.catalog.tableExists(t):
            out.append((kind, 0, 0, "no Iceberg table"))
            continue
        before = _stored(conn, schema, kind)
        if before and not replace:
            out.append((kind, None, before, "skipped: Postgres already has rows (--replace to copy again)"))
            continue
        if before:
            conn.run(f"TRUNCATE {schema}.{kind}")
        t0 = time.time()
        cols = [c for c, _ in pg_store.columns(ss.LOG_KINDS[kind]["ddl"])]
        have = set(spark.table(t).columns)
        sel = ", ".join(c if c in have else f"NULL AS {c}" for c in cols)
        n = 0
        sink._ensure(kind)
        batch = []
        for r in spark.sql(f"SELECT {sel} FROM {t}").toLocalIterator():
            batch.append({c: ss._utc_naive(v) for c, v in r.asDict().items()})
            if len(batch) >= 5000:
                with pg_store._Tx(conn):
                    sink._insert(kind, batch)
                n += len(batch)
                batch = []
        if batch:
            with pg_store._Tx(conn):
                sink._insert(kind, batch)
            n += len(batch)
        after = _stored(conn, schema, kind)
        out.append((kind, n, after, ("ok" if after == n else "MISMATCH") + f" ({time.time() - t0:.1f}s)"))
    return out


def report(title, rows):
    print(f"\n=== {title} ===", flush=True)
    print(f"  {'kind':22} {'iceberg':>9} {'postgres':>9}  result", flush=True)
    for kind, read, stored, note in rows:
        print(f"  {kind:22} {'' if read is None else read:>9} {stored:>9}  {note}", flush=True)


def main():
    from pyspark.sql import SparkSession
    here = os.path.dirname(os.path.abspath(__file__))
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default=os.path.join(here, "config", "health.json"))
    p.add_argument("--logs", action="store_true", help="copy the logs too")
    p.add_argument("--replace", action="store_true", help="empty a Postgres table before copying into it")
    a = p.parse_args()
    config = gl.load_config(a.config)
    spark = SparkSession.builder.appName("gl-pg-migrate").getOrCreate()
    try:
        state = copy_state(spark, config, a.replace)
        report(f"State: {gl.OPS_NAMESPACE} (Iceberg) -> Postgres", state)
        bad = [r for r in state if r[3].startswith("MISMATCH")]
        if a.logs:
            logs = copy_logs(spark, config, a.replace)
            report(f"Logs: {gl.OPS_NAMESPACE} (Iceberg) -> Postgres", logs)
            bad += [r for r in logs if r[3].startswith("MISMATCH")]
        print("\n" + ("MIGRATION OK" if not bad else f"MIGRATION HAD {len(bad)} MISMATCH(ES)") +
              ". Iceberg tables unchanged. Use Postgres per run (STATE_BACKEND=postgres LOGS_BACKEND=postgres) "
              "or set state.backend / logs.backend in config/health.json.", flush=True)
        sys.exit(1 if bad else 0)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
