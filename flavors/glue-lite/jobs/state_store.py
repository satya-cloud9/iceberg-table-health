"""Migration step 1: the StateStore and LogSink interfaces.

State is what the next run needs to continue correctly; a log only loses
history if it's lost (design doc, "State and log stores"). Callers talk to
these two interfaces and never write SQL against the ops tables for state, so
the backend can change without touching them: Iceberg tables (IcebergStateStore,
IcebergLogSink, the default) or Postgres (pg_store.py), chosen by state.backend /
logs.backend through make_state_store / make_log_sink. Claims are in DynamoDB.

  StateStore
    get(kind, key)                        one current item, or None
    range(kind, prefix, start, end, limit) items under a key prefix, in sort-key
                                          order; start / end bound the first sort
                                          field, both inclusive
    put(kind, item)                       write (state kinds: the new current item;
                                          append kinds: a new item)
    increment(kind, item, field)          add item[field] to a counter
    delete(kind, key)                     remove an item (merge kinds)
    expire(kind, days) / compact(kind)    retention: old items out, one item per key
    transaction()                         group one table's writes (context manager)
    claim(...)                            step 2 (profiles, groups, claims)
    flush()                               make buffered writes durable
  LogSink
    append(kind, rows) / flush()

Every write is visible to get / range in the same run straight away (read your
own writes), so callers keep no "pending" lists of their own.

Kinds (the record types; keys always start with table_uuid, the target comes
with profiles in step 2):

  kind              mode     key             sort                    today's table
  ledger_state      latest   table_uuid      -                       ledger_state
  activity_state    latest   table_uuid      -                       activity_state
  partition_state   merge    table_uuid, pk  -                       partition_state
  commit            append   table_uuid      ts_ms, snapshot_id      snapshot_log
  commit_partition  append   table_uuid      ts_ms, snapshot_id, pk  partition_activity
  gap_hist          counter  table_uuid      day, bucket_max_min     commit_gap_hist
  late_hist         counter  table_uuid      day, bucket_max_h       lateness_hist

Sort keys are (ts_ms, snapshot_id) rather than snapshot_id so "commits since T"
is a range on DynamoDB too.

IcebergStateStore (this step) keeps today's ops tables. preload(kind, start,
uuids, max_rows) reads a kind in one query, bounded to a time window and the
run's tables, and drops the bulk read when more than max_rows come back (it
reads max_rows + 1, so no separate count; tables then load one by one). get / range are served from memory; a range
reaching further back than what's loaded re-reads that one table. Writes are
buffered and flushed once per scan (one append per table, one MERGE and one
DELETE for partition_state). The DynamoDB backend will range per table instead.
"""
import contextlib
import random
import re
import time
from datetime import datetime, timezone

INF = float("inf")


class Kind:
    def __init__(self, table, mode, key=("table_uuid",), sort=(), ident=(), version="updated_at",
                 field=None, age=None):
        self.table, self.mode, self.key, self.sort = table, mode, key, sort
        self.age = age                # (column, "ts" | "day"): what expire() compares
        self.ident = ident            # append kinds: what makes two items the same record
        self.version = version        # latest kinds: newest wins by this column
        self.field = field            # counter kinds: the counted column


KINDS = {
    "ledger_state": Kind("ledger_state", "latest"),
    "activity_state": Kind("activity_state", "latest"),
    "partition_state": Kind("partition_state", "merge", key=("table_uuid", "partition_key")),
    "commit": Kind("snapshot_log", "append", sort=("ts_ms", "snapshot_id"), ident=("snapshot_id",),
                   age=("committed_at", "ts")),
    "commit_partition": Kind("partition_activity", "append", sort=("ts_ms", "snapshot_id", "partition_key"),
                             ident=("snapshot_id", "partition_key"), age=("committed_at", "ts")),
    "gap_hist": Kind("commit_gap_hist", "counter", sort=("day", "bucket_max_min"), field="gaps",
                     age=("day", "day")),
    "late_hist": Kind("lateness_hist", "counter", sort=("day", "bucket_max_h"), field="batches",
                      age=("day", "day")),
    # files an expiry freed, deleted once their grace has passed (freed_files.py)
    "freed_file": Kind("freed_files", "merge", key=("table_uuid", "path")),
    # what the next run needs from this one (scan_state.py)
    "table_state": Kind("table_state", "latest"),
    "partition_facts": Kind("partition_facts", "merge", key=("table_uuid", "partition_key")),
    "action_state": Kind("action_state", "latest"),
}

LOG_AGE = {"incremental_check": "checked_at"}

# Log kinds: the record shape and time column of everything the advisor appends
# (declared by the module that writes it). A sink creates the store for a kind
# from its declaration; the JSON-lines sink (step 3c) partitions by the time column.
LOG_KINDS = {}
# set on log tables when the sink creates them (upkeep keeps them so later)
LOG_TABLE_PROPERTIES = {"write.metadata.delete-after-commit.enabled": "true",
                        "write.metadata.previous-versions-max": "20"}


# where each log kind is declared: a process that reads or expires a kind it did
# not write itself (plan reads symptoms, upkeep expires every kind) loads it from here
LOG_HOMES = {"table_metrics": "scan_metrics", "partition_metrics": "scan_metrics", "coverage": "scan_metrics",
             "symptoms": "detect_symptoms", "incremental_check": "ledger", "actions": "plan",
             "scorecard": "scorecard", "run_journal": "gl_scan", "ops_maintenance": "ops_maintenance"}


def log_kind(kind):
    """A log kind's declaration, importing the module that declares it if needed
    (None for an unknown kind)."""
    if kind not in LOG_KINDS and kind in LOG_HOMES:
        import importlib
        importlib.import_module(LOG_HOMES[kind])
    return LOG_KINDS.get(kind)


def declare_all():
    """Every known log kind declared (for jobs that handle all of them)."""
    for kind in LOG_HOMES:
        log_kind(kind)
    return sorted(LOG_KINDS)


def declare_log(kind, ddl, ts, partition=""):
    """ddl: column list; ts: the record's time column; partition: Iceberg
    partition clause for the Iceberg sink ('' = unpartitioned)."""
    LOG_KINDS[kind] = {"ddl": ddl, "ts": ts, "partition": partition}
    LOG_AGE.setdefault(kind, ts)


def backend_of(config, what):
    """state.backend / logs.backend from the config, GL_STATE_BACKEND / GL_LOGS_BACKEND winning."""
    import os
    env = os.environ.get(f"GL_{what.upper()}_BACKEND")
    b = env or str(((config or {}).get(what) or {}).get("backend", "iceberg"))
    if b not in ("iceberg", "postgres"):
        raise ValueError(f"unknown {what}.backend {b!r} (iceberg or postgres)")
    return b


def make_log_sink(spark, config=None, ops=None):
    """The configured log sink (logs.backend: iceberg, the default, or postgres)."""
    import gl_common as gl
    if backend_of(config, "logs") == "postgres":
        import pg_store
        return pg_store.PostgresLogSink(config)
    return IcebergLogSink(spark, ops or gl.OPS_NAMESPACE)


def make_state_store(spark, config=None, ops=None):
    """The configured state store (state.backend: iceberg, the default, or postgres)."""
    import gl_common as gl
    if backend_of(config, "state") == "postgres":
        import pg_store
        return pg_store.PostgresStateStore(config)
    return IcebergStateStore(spark, ops or gl.OPS_NAMESPACE)


def _age_sql(col, how, days):
    if how == "day":
        return f"{col} < date_sub(current_date(), {int(days)})"
    return f"{col} < current_timestamp() - INTERVAL {int(days)} DAYS"


def _sortable(v):
    """None sorts last (an open-ended bucket); dates and numbers as themselves."""
    return (1, 0) if v is None else (0, v)


def _norm_bucket(b):
    if b is None:
        return None
    b = float(b)
    return int(b) if b.is_integer() else b


def _newer_or_same(a, b):
    if b is None:
        return True
    if a is None:
        return False
    return a >= b


_TS = re.compile(r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}(?::\d{2})?)(?:\.(\d+))?\s*(Z|[+-]\d{2}(?::?\d{2})?)?$")


def iso_text(s):
    """An ISO timestamp as the strict form every Python 3 fromisoformat accepts
    (YYYY-MM-DDTHH:MM:SS.ffffff+HH:MM). Python 3.10 (the Spark image) rejects
    fractions that are not 3 or 6 digits and a 'Z'; Postgres trims trailing zeros
    (.91719) and may print +00."""
    m = _TS.match(str(s).strip())
    if not m:
        raise ValueError(f"not an ISO timestamp: {s!r}")
    day, hms, frac, tz = m.groups()
    if len(hms) == 5:
        hms += ":00"
    frac = (frac or "0")[:6].ljust(6, "0")
    if tz in (None, ""):
        tz = ""
    elif tz == "Z":
        tz = "+00:00"
    else:
        tz = tz.replace(":", "")
        tz = f"{tz[:3]}:{tz[3:5] or '00'}"
    return f"{day}T{hms}.{frac}{tz}"


def parse_ts(s):
    """An ISO timestamp string -> datetime (aware when it carries an offset)."""
    return datetime.fromisoformat(iso_text(s))


def _utc_naive(v):
    """collect() returns timestamps as naive datetimes in the driver's local zone;
    the advisor's convention (and the Postgres sink's) is naive UTC."""
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).replace(tzinfo=None)
    return v


def day_of(ts_ms):
    """The UTC day of a commit time (what to_date(committed_at) gives with the UTC session)."""
    return datetime.fromtimestamp(ts_ms / 1000.0, timezone.utc).date()


class StateStore:
    """The interface. Backends implement _load_kind / _load_prefix / _write."""

    def get(self, kind, key):
        raise NotImplementedError

    def range(self, kind, prefix, start=None, end=None, limit=None):
        raise NotImplementedError

    def put(self, kind, item):
        raise NotImplementedError

    def increment(self, kind, item, field=None):
        raise NotImplementedError

    @contextlib.contextmanager
    def transaction(self):
        """Group one table's writes. Iceberg: writes are buffered until flush()
        anyway, so a crash before the flush loses the whole scan's writes and the
        next scan redoes them from the old watermarks. DynamoDB will commit the
        group in one TransactWriteItems (chunked past 100 items, watermark last)."""
        yield self

    def claim(self, *a, **k):
        raise NotImplementedError("claims come with profiles and groups (migration step 2)")

    def expire(self, kind, older_than_days):
        """Drop items older than N days (append and counter kinds)."""
        raise NotImplementedError

    def delete(self, kind, key):
        raise NotImplementedError

    def compact(self, kind):
        """Backend housekeeping: one item per key for latest kinds (no-op where puts overwrite)."""
        return 0

    def flush(self):
        raise NotImplementedError


_UNSET = object()


def _covers(cover, start):
    """Does a load from `cover` (None = all history) include everything from `start`?"""
    return cover is None or (start is not None and start >= cover)


class MemoryState(StateStore):
    """Read-your-writes buffering and the in-memory cache, shared by backends.

    _cache[kind][prefix] = {"rows": [...]}   for append / counter kinds (prefix = (table_uuid,))
    _cache[kind][key]    = row               for latest / merge kinds (key tuple)

    What is cached is tracked as coverage: a bulk preload covers every table
    from a start (None = all history), or only a list of tables; a per-table
    load covers one table from a start. A range that asks for more than is
    covered loads that one table again from the earlier start, so a window
    never changes an answer, it only decides how much is read up front.
    """

    def __init__(self):
        self._cache = {k: {} for k in KINDS}
        self._kind_cover = {k: _UNSET for k in KINDS}    # bulk preload of every table: its start
        self._prefix_cover = {k: {} for k in KINDS}      # per table: the start it was loaded from
        self._pending = {k: [] for k in KINDS}
        self._deletes = {k: [] for k in KINDS}
        self.stats = {"queries": 0, "gets": 0, "ranges": 0, "puts": 0, "deletes": 0,
                      "table_loads": 0, "preload_skipped": []}

    # backend hooks
    def _load(self, kind, start=None, uuids=None, limit=None):
        """-> rows of the kind (latest kinds: the latest per key), from `start` on
        (first sort field), for `uuids` (None = every table), at most `limit`."""
        return []

    def _write(self, kind, rows):
        pass

    def _delete(self, kind, keys):
        pass

    # loading
    def preload(self, kind, start=None, uuids=None, max_rows=None):
        """Read a kind in one query (optionally from `start`, only `uuids`). With
        max_rows, a kind larger than that is not preloaded: tables then load
        one by one as they're used. -> True when preloaded."""
        if self._kind_cover[kind] is not _UNSET and _covers(self._kind_cover[kind], start) and uuids is None:
            return True
        # one query: read up to max_rows + 1; more than max_rows back = too big,
        # dropped, and tables load one by one instead (no separate count query)
        rows = self._load(kind, start, uuids, limit=(max_rows + 1) if max_rows else None)
        if max_rows and len(rows) > max_rows:
            self.stats["preload_skipped"].append((kind, f"> {max_rows}"))
            return False
        if uuids is None:
            self._cache[kind] = {}
            self._fill(kind, rows)
            self._kind_cover[kind] = start
            self._prefix_cover[kind] = {}
        else:
            got = {}
            for r in rows:
                got.setdefault(r["table_uuid"], []).append(r)
            for u in uuids:
                self._fill(kind, got.get(u, []), prefix=(u,))
                self._prefix_cover[kind][(u,)] = start
        return True

    def _fill(self, kind, rows, prefix=None, newest=False):
        """newest=True: rows this run just wrote, which replace what's cached (their
        timestamps are timezone-aware, stored ones as Spark returns them)."""
        k = KINDS[kind]
        cache = self._cache[kind]
        if prefix is not None:
            if k.mode in ("latest", "merge"):
                for key in [key for key in cache if key[:len(prefix)] == prefix]:
                    del cache[key]
            else:
                cache.pop(prefix, None)
        for r in rows:
            if k.mode in ("latest", "merge"):
                key = tuple(r[c] for c in k.key)
                old = cache.get(key)
                if newest or k.mode == "merge" or old is None or _newer_or_same(r.get(k.version), old.get(k.version)):
                    cache[key] = r
            else:
                if kind in ("gap_hist", "late_hist"):
                    r[k.sort[1]] = _norm_bucket(r.get(k.sort[1]))
                cache.setdefault((r["table_uuid"],), {"rows": []})["rows"].append(r)

    def _covered(self, kind, prefix, start):
        pc = self._prefix_cover[kind].get(prefix, _UNSET)
        if pc is not _UNSET:
            return _covers(pc, start)
        kc = self._kind_cover[kind]
        return kc is not _UNSET and _covers(kc, start)

    def _ensure(self, kind, prefix, start=None):
        if KINDS[kind].mode in ("latest", "merge"):
            start = None
        if self._covered(kind, prefix, start):
            return
        self.stats["table_loads"] += 1
        self._fill(kind, self._load(kind, start, [prefix[0]]), prefix=prefix)
        self._prefix_cover[kind][prefix] = start

    # reads
    def get(self, kind, key):
        self.stats["gets"] += 1
        key = tuple(key)
        k = KINDS[kind]
        for r in reversed(self._pending[kind]):
            if tuple(r[c] for c in k.key) == key:
                return r
        self._ensure(kind, key[:1])
        return self._cache[kind].get(key)

    def range(self, kind, prefix, start=None, end=None, limit=None):
        """Items under the prefix in sort order. start / end bound the first sort
        field (inclusive); start also decides how much is loaded."""
        self.stats["ranges"] += 1
        prefix = tuple(prefix)
        k = KINDS[kind]
        self._ensure(kind, prefix[:1], start)
        if k.mode in ("latest", "merge"):
            got = {key: r for key, r in self._cache[kind].items() if key[:len(prefix)] == prefix}
            for r in self._pending[kind]:
                key = tuple(r[c] for c in k.key)
                if key[:len(prefix)] == prefix:
                    got[key] = r
            rows = list(got.values())
        else:
            stored = (self._cache[kind].get(prefix[:1]) or {}).get("rows", [])
            seen = {tuple(r[c] for c in k.ident) for r in stored} if k.ident else set()
            rows = list(stored) + [r for r in self._pending[kind]
                                   if r["table_uuid"] == prefix[0]
                                   and (not k.ident or tuple(r[c] for c in k.ident) not in seen)]
        if k.sort and (start is not None or end is not None):
            f = k.sort[0]
            rows = [r for r in rows if (start is None or (r.get(f) is not None and r[f] >= start))
                    and (end is None or (r.get(f) is not None and r[f] <= end))]
        if k.sort:
            rows.sort(key=lambda r: tuple(_sortable(r.get(c)) for c in k.sort))
        return rows[:limit] if limit else rows

    def items(self, kind):
        """Every item of a latest / merge kind, with this run's pending puts and
        deletes applied: [(key, item)]. Reads the whole kind once if not preloaded."""
        k = KINDS[kind]
        assert k.mode in ("latest", "merge"), kind
        if self._kind_cover[kind] is _UNSET:
            self.preload(kind)
        got = dict(self._cache[kind])
        for r in self._pending[kind]:
            got[tuple(r[c] for c in k.key)] = r
        return list(got.items())

    # writes
    def put(self, kind, item):
        self.stats["puts"] += 1
        self._pending[kind].append(dict(item))

    def increment(self, kind, item, field=None):
        k = KINDS[kind]
        assert k.mode == "counter", kind
        self.put(kind, item)                  # a delta row: Iceberg appends it, DynamoDB will ADD

    def delete(self, kind, key):
        """Remove one item of a merge kind (applied at flush)."""
        assert KINDS[kind].mode == "merge", kind
        key = tuple(key)
        self.stats["deletes"] += 1
        self._deletes[kind].append(key)
        k = KINDS[kind]
        self._cache[kind].pop(key, None)
        self._pending[kind] = [r for r in self._pending[kind] if tuple(r[c] for c in k.key) != key]

    def flush(self):
        for kind, keys in self._deletes.items():
            if keys:
                self._delete(kind, keys)
            self._deletes[kind] = []
        for kind, rows in self._pending.items():
            if rows:
                self._write(kind, rows)
                k = KINDS[kind]
                # keep serving what was just written without a re-read
                if self._kind_cover[kind] is not _UNSET or k.mode in ("latest", "merge"):
                    self._fill(kind, rows, newest=True)
                else:
                    for r in rows:
                        p = (r["table_uuid"],)
                        if p in self._prefix_cover[kind]:
                            self._cache[kind].setdefault(p, {"rows": []})["rows"].append(r)
            self._pending[kind] = []

    def _forget(self, kind):
        self._cache[kind] = {}
        self._kind_cover[kind] = _UNSET
        self._prefix_cover[kind] = {}


def _q(v):
    return "'" + str(v).replace("'", "''") + "'"


CONFLICT_MARKERS = ("CommitFailedException", "ValidationException", "conflicting files",
                    "Found conflicting", "concurrent", "Cannot commit")


def retry_on_conflict(fn, what, attempts=5, sleep=time.sleep):
    """Run a row-level write (MERGE, DELETE, INSERT OVERWRITE) again when a
    concurrent run committed to the same table first. Appends need no retry:
    Iceberg retries them itself. -> fn's result; re-raises other errors."""
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:                       # py4j wraps the Java exception
            text = f"{type(e).__name__}: {e}"
            if i == attempts - 1 or not any(m in text for m in CONFLICT_MARKERS):
                raise
            print(f"  ({what}: concurrent commit, retry {i + 1}/{attempts - 1})", flush=True)
            sleep(min(2 ** i, 15) + random.random())


def superseded_filters(k, newest, chunk=200):
    """DELETE conditions for compact(): per key, its rows older than the
    newest version read (newest: rows with the key columns and newest_us).
    Literal bounds only, so Iceberg can tell which files a DELETE touches."""
    conds = [" AND ".join([f"{c} = {_q(r[c])}" for c in k.key]
                          + [f"{k.version} < timestamp_micros({int(r['newest_us'])})"]) for r in newest]
    return ["(" + ") OR (".join(conds[i:i + chunk]) + ")" for i in range(0, len(conds), chunk)]


class IcebergStateStore(MemoryState):
    iceberg = True        # Iceberg tables in the ops namespace (created by each module's ensure_*)

    """Today's glue.ops tables behind the interface."""

    def __init__(self, spark, ops):
        super().__init__()
        self.spark, self.ops = spark, ops
        self._exists_cache = {}

    def _rows(self, sql):
        self.stats["queries"] += 1
        return [r.asDict() for r in self.spark.sql(sql).collect()]

    def _exists(self, kind):
        if kind not in self._exists_cache:      # one catalog call per kind per run
            self._exists_cache[kind] = self.spark.catalog.tableExists(f"{self.ops}.{KINDS[kind].table}")
        return self._exists_cache[kind]

    def _where(self, kind, start, uuids):
        k = KINDS[kind]
        conds = []
        if uuids is not None:
            conds.append("s.table_uuid IN (" + (", ".join(_q(u) for u in uuids) or "NULL") + ")")
        if start is not None and k.sort:
            f = k.sort[0]
            if f == "ts_ms":           # the committed_at bound lets Iceberg skip old day partitions
                conds.append(f"s.ts_ms >= {int(start)} AND (s.committed_at IS NULL "
                             f"OR s.committed_at >= timestamp_millis({int(start)}))")
            else:
                conds.append(f"s.{f} >= DATE '{start}'")
        return ("WHERE " + " AND ".join(conds)) if conds else ""

    def _select(self, kind, where=""):
        k = KINDS[kind]
        t = f"{self.ops}.{k.table}"
        if k.mode == "latest":
            return (f"SELECT * FROM (SELECT s.*, row_number() OVER (PARTITION BY {', '.join(k.key)} "
                    f"ORDER BY {k.version} DESC) AS rn FROM {t} s {where}) WHERE rn = 1")
        return f"SELECT * FROM {t} s {where}"

    def _load(self, kind, start=None, uuids=None, limit=None):
        if not self._exists(kind):
            return []
        rows = self._rows(self._select(kind, self._where(kind, start, uuids))
                          + (f" LIMIT {int(limit)}" if limit else ""))
        for r in rows:
            r.pop("rn", None)
        return rows

    def _write(self, kind, rows):
        from scan_metrics import as_row
        t = f"{self.ops}.{KINDS[kind].table}"
        sch = self.spark.table(t).schema
        if KINDS[kind].mode == "merge":
            self._merge(kind, rows)
        else:
            self.spark.createDataFrame([as_row(r, sch) for r in rows], sch).writeTo(t).append()

    def _delete(self, kind, keys):
        k = KINDS[kind]
        by_table = {}
        for key in keys:
            by_table.setdefault(key[0], []).append(key[1])
        for u, rest in by_table.items():
            for i in range(0, len(rest), 500):           # bounded statements (freed files come in thousands)
                sql = (f"DELETE FROM {self.ops}.{k.table} WHERE table_uuid = {_q(u)} "
                       f"AND {k.key[1]} IN ({', '.join(_q(x) for x in rest[i:i + 500])})")
                retry_on_conflict(lambda: self.spark.sql(sql), f"delete from {k.table}")

    def expire(self, kind, older_than_days):
        k = KINDS[kind]
        if k.age and self._exists(kind):
            sql = f"DELETE FROM {self.ops}.{k.table} WHERE {_age_sql(*k.age, older_than_days)}"
            retry_on_conflict(lambda: self.spark.sql(sql), f"expire {k.table}")
            self._forget(kind)               # re-read on next use

    def compact(self, kind):
        """Latest kinds: drop the rows a newer row for the same key supersedes.

        Reads each key's newest version, then a row-level DELETE of that key's
        rows older than it (in chunks). A row another run appends meanwhile is
        newer than what was read, so it is never removed, and Iceberg checks
        each DELETE against files committed since it started (a conflict is
        retried). Rewriting the whole table from rows read a moment earlier
        could drop such a row, and that table would resume from an older
        position."""
        k = KINDS[kind]
        if k.mode != "latest" or not self._exists(kind):
            return 0
        t = f"{self.ops}.{k.table}"
        keys = ", ".join(k.key)
        rows = self._rows(f"SELECT {keys}, unix_micros(max({k.version})) AS newest_us, count(*) AS n "
                          f"FROM {t} GROUP BY {keys} HAVING count(*) > 1")
        for cond in superseded_filters(k, rows):
            sql = f"DELETE FROM {t} WHERE {cond}"
            retry_on_conflict(lambda: self.spark.sql(sql), f"compact {k.table}")
        if rows:
            self._forget(kind)
        return sum(int(r["n"]) - 1 for r in rows)

    def _merge(self, kind, rows):
        """Upsert by key (one MERGE per flush)."""
        from scan_metrics import as_row
        k = KINDS[kind]
        t = f"{self.ops}.{k.table}"
        sch = self.spark.table(t).schema
        last = {}
        for r in rows:                         # one row per key, the newest put
            last[tuple(r[c] for c in k.key)] = r
        self.spark.createDataFrame([as_row(r, sch) for r in last.values()], sch) \
            .createOrReplaceTempView("gl_state_merge")
        on = " AND ".join(f"t.{c} = u.{c}" for c in k.key)
        retry_on_conflict(lambda: self.spark.sql(f"""
            MERGE INTO {t} t USING gl_state_merge u ON {on}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *"""), f"merge into {k.table}")


class LogSink:
    """Append-only records (L1-L8 in the design). Buffered; one append per kind
    at flush. Iceberg (glue.ops.<kind>) or Postgres (pg_store.PostgresLogSink)."""

    def append(self, kind, rows):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError

    def expire(self, kind, older_than_days):
        raise NotImplementedError

    def rows(self, kind, eq=None, order_desc=None, limit=None):
        """Logged rows of a kind (dicts): eq = {column: value or [values]} (equality /
        IN only), newest first by the order_desc column, at most limit. For the
        few readers that need a log back (a standalone detect or scorecard, plan
        --scan-id); [] when nothing was logged yet."""
        raise NotImplementedError


class IcebergLogSink(LogSink):
    iceberg = True

    def rows(self, kind, eq=None, order_desc=None, limit=None):
        t = f"{self.ops}.{kind}"
        if not self.spark.catalog.tableExists(t):
            return []
        where = []
        for c, v in (eq or {}).items():
            vals = list(v) if isinstance(v, (list, tuple, set)) else [v]
            if not vals:
                return []
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", c):
                raise ValueError(f"bad column {c!r}")
            where.append(f"{c} IN ({', '.join(_q(x) for x in vals)})")
        sql = (f"SELECT * FROM {t}" + (" WHERE " + " AND ".join(where) if where else "")
               + (f" ORDER BY {order_desc} DESC NULLS LAST" if order_desc else "")
               + (f" LIMIT {int(limit)}" if limit else ""))
        return [{k: _utc_naive(v) for k, v in r.asDict().items()} for r in self.spark.sql(sql).collect()]

    def __init__(self, spark, ops):
        self.spark, self.ops = spark, ops
        self._pending = {}
        self._ready = set()

    def _ensure(self, kind):
        """Create the kind's table (or add new columns) from its declaration, once per sink."""
        if kind in self._ready or log_kind(kind) is None:
            return
        import gl_common as gl
        d, t = LOG_KINDS[kind], f"{self.ops}.{kind}"
        part = f"PARTITIONED BY ({d['partition']})" if d["partition"] else ""
        props = ", ".join(f"'{k}'='{v}'" for k, v in LOG_TABLE_PROPERTIES.items())
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {t} ({d['ddl']}) USING iceberg {part} TBLPROPERTIES ({props})")
        gl.ensure_columns(self.spark, t, d["ddl"])
        self._ready.add(kind)

    def append(self, kind, rows):
        self._pending.setdefault(kind, []).extend(dict(r) for r in rows)

    def pending(self, kind):
        return list(self._pending.get(kind, []))

    def expire(self, kind, older_than_days):
        if not self.spark.catalog.tableExists(f"{self.ops}.{kind}"):
            return
        sql = f"DELETE FROM {self.ops}.{kind} WHERE {_age_sql(LOG_AGE[kind], 'ts', older_than_days)}"
        retry_on_conflict(lambda: self.spark.sql(sql), f"expire {kind}")

    def flush(self):
        from scan_metrics import as_row
        for kind, rows in self._pending.items():
            if rows:
                self._ensure(kind)
                t = f"{self.ops}.{kind}"
                sch = self.spark.table(t).schema
                self.spark.createDataFrame([as_row(r, sch) for r in rows], sch).writeTo(t).append()
        self._pending = {}
