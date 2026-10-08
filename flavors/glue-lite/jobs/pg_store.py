"""Postgres backends for the state store and the log sink (design: "Backends").

Chosen with state.backend / logs.backend = "postgres" (or GL_STATE_BACKEND /
GL_LOGS_BACKEND); the Iceberg backends stay the default. Only the Spark driver
talks to Postgres; executors never do.

Two ways to reach the database, same SQL:

  mode "driver"    pg8000 over TCP (homelab Postgres pod, Aurora from EKS)
  mode "data_api"  the RDS Data API over HTTPS with IAM (Aurora Serverless v2
                   from Glue: no VPC connection, no driver, no password)

Tables live in one schema (postgres.schema, default "advisor"):

  state kinds   one table per kind (named like the Iceberg table): the key
                columns (JSON text of each key value), a few typed columns for
                ranges and retention (s0_num / s0_date: the first sort field,
                age_at, ver_ms, n), and the item itself as jsonb. Writes are
                upserts: latest kinds keep the newest version, merge kinds
                overwrite, append kinds ignore a record already stored,
                counters add. compact() has nothing to do.
  log kinds     one typed table per declared log kind (declare_log), columns
                from its DDL, indexed on its time column; appends only.

Every value goes in as a parameter (no SQL built from data), with casts in the
SQL for jsonb / timestamptz / date, so both drivers send plain strings and
numbers. Reads come back as JSON text (item::text, to_jsonb(row)::text) and are
decoded here, so both drivers return the same values: timestamps as naive UTC
datetimes and dates as dates, as Spark returns them in a UTC session.
"""
import json
import os
import re
from datetime import date, datetime, timezone

import state_store as ss

PAGE = 1000            # rows per read page by default; each connection sets its own (page)
WRITE_CHUNK = 100      # rows per multi-row INSERT

_CONN = {}             # one connection per process and settings


# --- settings ----------------------------------------------------------------
def settings(config=None):
    """The postgres block of the config with GL_PG_* environment overrides."""
    pg = dict((config or {}).get("postgres") or {})
    env = os.environ.get
    for key, var in (("mode", "GL_PG_MODE"), ("host", "GL_PG_HOST"), ("port", "GL_PG_PORT"),
                     ("database", "GL_PG_DATABASE"), ("user", "GL_PG_USER"), ("schema", "GL_PG_SCHEMA"),
                     ("resource_arn", "GL_PG_RESOURCE_ARN"), ("secret_arn", "GL_PG_SECRET_ARN")):
        if env(var):
            pg[key] = env(var)
    pg.setdefault("mode", "driver")
    pg.setdefault("port", 5432)
    pg.setdefault("database", "advisor")
    pg.setdefault("user", "advisor")
    pg.setdefault("schema", "advisor")
    pg["password"] = env(pg.get("password_env") or "GL_PG_PASSWORD") or pg.get("password")
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", str(pg["schema"])):
        raise ValueError(f"postgres.schema {pg['schema']!r}: lowercase letters, digits and _ only")
    return pg


def connect(config=None):
    """The process's connection for these settings (opened once)."""
    pg = settings(config)
    ident = (pg["mode"], pg.get("host"), str(pg.get("port")), pg.get("database"), pg.get("resource_arn"))
    if ident not in _CONN:
        if pg["mode"] == "driver":
            _CONN[ident] = DriverConn(pg)
        elif pg["mode"] == "data_api":
            _CONN[ident] = DataApiConn(pg)
        else:
            raise ValueError(f"unknown postgres.mode {pg['mode']!r} (driver or data_api)")
    return _CONN[ident], pg["schema"]


# --- connections ----------------------------------------------------------------
class DriverConn:
    """pg8000 (pure Python). run() takes :name parameters."""

    page = 5000

    def __init__(self, pg):
        import pg8000.native
        if not pg.get("host"):
            raise ValueError("postgres.host (or GL_PG_HOST) is not set")
        self.c = pg8000.native.Connection(pg["user"], host=pg["host"], port=int(pg["port"]),
                                          database=pg["database"], password=pg.get("password"),
                                          timeout=60, application_name="iceberg-advisor")
        self.c.run("SET TIME ZONE 'UTC'")
        self.in_tx = False

    def run(self, sql, params=None):
        return self.c.run(sql, **(params or {})) or []

    def run_many(self, sql, param_list):
        for p in param_list:
            self.c.run(sql, **p)

    def begin(self):
        self.c.run("BEGIN")
        self.in_tx = True

    def commit(self):
        self.c.run("COMMIT")
        self.in_tx = False

    def rollback(self):
        if self.in_tx:
            self.c.run("ROLLBACK")
        self.in_tx = False


class DataApiConn:
    """The RDS Data API (boto3 rds-data). Same :name parameters. A response is
    capped at 1 MB, so reads page in 200 rows (partition facts run ~1-3 KB each)."""

    page = 200

    def __init__(self, pg, client=None):
        for k in ("resource_arn", "secret_arn"):
            if not pg.get(k):
                raise ValueError(f"postgres.{k} (or GL_PG_{k.upper()}) is needed for mode data_api")
        if client is None:
            import boto3
            client = boto3.client("rds-data")
        self.client, self.pg, self.tx = client, pg, None

    def _args(self):
        a = {"resourceArn": self.pg["resource_arn"], "secretArn": self.pg["secret_arn"],
             "database": self.pg["database"]}
        if self.tx:
            a["transactionId"] = self.tx
        return a

    @staticmethod
    def _param(name, v):
        if v is None:
            return {"name": name, "value": {"isNull": True}}
        if isinstance(v, bool):
            return {"name": name, "value": {"booleanValue": v}}
        if isinstance(v, int):
            return {"name": name, "value": {"longValue": v}}
        if isinstance(v, float):
            return {"name": name, "value": {"doubleValue": v}}
        return {"name": name, "value": {"stringValue": str(v)}}

    @staticmethod
    def _field(f):
        if f.get("isNull"):
            return None
        for k in ("stringValue", "longValue", "doubleValue", "booleanValue"):
            if k in f:
                return f[k]
        return None

    def run(self, sql, params=None):
        out = self.client.execute_statement(
            sql=sql, parameters=[self._param(k, v) for k, v in (params or {}).items()], **self._args())
        return [[self._field(f) for f in rec] for rec in out.get("records", [])]

    def run_many(self, sql, param_list):
        sets = [[self._param(k, v) for k, v in p.items()] for p in param_list]
        for i in range(0, len(sets), 200):
            self.client.batch_execute_statement(sql=sql, parameterSets=sets[i:i + 200], **self._args())

    def begin(self):
        a = self._args()
        a.pop("transactionId", None)
        self.tx = self.client.begin_transaction(**a)["transactionId"]

    def commit(self):
        if self.tx:
            self.client.commit_transaction(resourceArn=self.pg["resource_arn"],
                                           secretArn=self.pg["secret_arn"], transactionId=self.tx)
        self.tx = None

    def rollback(self):
        if self.tx:
            self.client.rollback_transaction(resourceArn=self.pg["resource_arn"],
                                             secretArn=self.pg["secret_arn"], transactionId=self.tx)
        self.tx = None


class _Tx:
    """BEGIN ... COMMIT around a block (rollback on error)."""

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        self.conn.begin()
        return self.conn

    def __exit__(self, et, ev, tb):
        if et is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        return False


# --- values ---------------------------------------------------------------------
def to_ms(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return int(v.timestamp() * 1000)
    if isinstance(v, date):
        return int(datetime(v.year, v.month, v.day, tzinfo=timezone.utc).timestamp() * 1000)
    return to_ms(ss.parse_ts(v))


def _enc(v):
    if isinstance(v, datetime):
        return {"$iso": (v if v.tzinfo else v.replace(tzinfo=timezone.utc)).astimezone(timezone.utc).isoformat()}
    if isinstance(v, date):
        return {"$d": v.isoformat()}
    if hasattr(v, "__float__") and not isinstance(v, (int, float, bool)):
        return float(v)                        # Decimal
    return str(v)


def dumps(item):
    return json.dumps(item, default=_enc, sort_keys=True)


def _dec(o):
    if "$iso" in o:
        return ss.parse_ts(o["$iso"]).astimezone(timezone.utc).replace(tzinfo=None)
    if "$d" in o:
        return date.fromisoformat(o["$d"])
    return o


def loads(s):
    if s is None:
        return None
    if isinstance(s, (dict, list)):            # pg8000 may decode json itself
        s = json.dumps(s)
    return json.loads(s, object_hook=_dec)


def _ts_text(v):
    """A timestamp parameter: ISO text with an explicit UTC offset."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        v = datetime.fromtimestamp(v / 1000.0, timezone.utc)
    if isinstance(v, str):
        v = ss.parse_ts(v)
    if isinstance(v, date) and not isinstance(v, datetime):
        v = datetime(v.year, v.month, v.day)
    if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return v.astimezone(timezone.utc).isoformat()


def _date_text(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return str(v)[:10]


def _key_text(v):
    """A key column value: the JSON of the value, so None, ints and strings stay distinct."""
    return dumps(ss._norm_bucket(v) if isinstance(v, float) else v)


# --- state store ------------------------------------------------------------------
class PostgresStateStore(ss.MemoryState):
    """The state store on Postgres (see the module docstring)."""

    iceberg = False

    def __init__(self, config=None, conn=None, schema=None):
        super().__init__()
        if conn is None:
            conn, schema = connect(config)
        self.conn, self.schema = conn, schema or "advisor"
        self._ready = set()

    # layout per kind
    @staticmethod
    def _keys(k):
        if k.mode in ("latest", "merge"):
            return tuple(k.key)
        if k.mode == "append":
            return ("table_uuid",) + tuple(k.ident)
        return ("table_uuid",) + tuple(k.sort)              # counter: one row per bucket

    def _t(self, kind):
        return f"{self.schema}.{ss.KINDS[kind].table}"

    def _ddl(self, kind):
        if kind in self._ready:
            return
        k, t = ss.KINDS[kind], self._t(kind)
        keys = self._keys(k)
        cols = ", ".join(f"{c} text NOT NULL" for c in keys)
        self.conn.run(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        self.conn.run(f"""CREATE TABLE IF NOT EXISTS {t} ({cols}, s0_num double precision, s0_date date,
                          age_at timestamptz, ver_ms bigint, n bigint, item jsonb NOT NULL,
                          written_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY ({', '.join(keys)}))""")
        if k.sort:
            self.conn.run(f"CREATE INDEX IF NOT EXISTS {k.table}_s0 ON {t} (table_uuid, s0_num, s0_date)")
        if k.age:
            self.conn.run(f"CREATE INDEX IF NOT EXISTS {k.table}_age ON {t} (age_at)")
        self._ready.add(kind)

    def _exists(self, kind):
        r = self.conn.run("SELECT to_regclass(:t) IS NOT NULL", {"t": self._t(kind)})
        return bool(r and r[0][0])

    # backend hooks (MemoryState)
    def _load(self, kind, start=None, uuids=None, limit=None):
        self.stats["queries"] += 1
        if kind not in self._ready and not self._exists(kind):
            return []
        k, t = ss.KINDS[kind], self._t(kind)
        where, params = [], {}
        if uuids is not None:
            if not uuids:
                return []
            names = []
            for i, u in enumerate(uuids):
                params[f"u{i}"] = _key_text(u)
                names.append(f":u{i}")
            where.append(f"table_uuid IN ({', '.join(names)})")
        if start is not None and k.sort:
            if isinstance(start, (int, float)):
                where.append("s0_num >= :s0")
                params["s0"] = float(start)
            else:
                where.append("s0_date >= CAST(:s0 AS date)")
                params["s0"] = _date_text(start)
        sql = (f"SELECT item::text, n FROM {t}" + (" WHERE " + " AND ".join(where) if where else "")
               + f" ORDER BY {', '.join(self._keys(k))}")
        rows, off = [], 0
        while True:
            size = getattr(self.conn, "page", PAGE)
            page = min(size, limit - len(rows)) if limit else size
            got = self.conn.run(f"{sql} LIMIT {int(page)} OFFSET {int(off)}", params)
            for item_text, n in got:
                item = loads(item_text)
                if k.mode == "counter":
                    item[k.field] = int(n or 0)
                rows.append(item)
            off += len(got)
            if len(got) < page or (limit and len(rows) >= limit):
                return rows

    def _row(self, k, r):
        keys = self._keys(k)
        p = {f"k{i}": _key_text(r.get(c)) for i, c in enumerate(keys)}
        s0 = r.get(k.sort[0]) if k.sort else None
        p["s0n"] = float(s0) if isinstance(s0, (int, float)) and not isinstance(s0, bool) else None
        p["s0d"] = _date_text(s0) if isinstance(s0, date) else None
        age = r.get(k.age[0]) if k.age else None
        p["age"] = _ts_text(age) if age is not None else None
        p["ver"] = to_ms(r.get(k.version)) if k.mode == "latest" else None
        p["n"] = int(r.get(k.field) or 0) if k.mode == "counter" else None
        p["item"] = dumps(r)
        return p

    def _write(self, kind, rows):
        self._ddl(kind)
        k, t = ss.KINDS[kind], self._t(kind)
        keys = self._keys(k)
        # one row per key and flush (ON CONFLICT cannot touch a row twice in one statement)
        by_key = {}
        for r in rows:
            key = tuple(_key_text(r.get(c)) for c in keys)
            if k.mode == "counter" and key in by_key:
                merged = dict(by_key[key])
                merged[k.field] = int(merged.get(k.field) or 0) + int(r.get(k.field) or 0)
                by_key[key] = merged
            elif k.mode == "append" and key in by_key:
                continue
            elif k.mode == "latest" and key in by_key and to_ms(r.get(k.version) or 0) < to_ms(by_key[key].get(k.version) or 0):
                continue
            else:
                by_key[key] = r
        if k.mode == "append":
            action = "DO NOTHING"
        elif k.mode == "counter":
            action = f"DO UPDATE SET n = {t}.n + EXCLUDED.n, written_at = now()"
        else:
            sets = "s0_num = EXCLUDED.s0_num, s0_date = EXCLUDED.s0_date, age_at = EXCLUDED.age_at, " \
                   "ver_ms = EXCLUDED.ver_ms, item = EXCLUDED.item, written_at = now()"
            cond = f" WHERE {t}.ver_ms IS NULL OR {t}.ver_ms <= EXCLUDED.ver_ms" if k.mode == "latest" else ""
            action = f"DO UPDATE SET {sets}{cond}"
        cols = list(keys) + ["s0_num", "s0_date", "age_at", "ver_ms", "n", "item"]
        items = list(by_key.values())
        for i in range(0, len(items), WRITE_CHUNK):
            params, values = {}, []
            for j, r in enumerate(items[i:i + WRITE_CHUNK]):
                p = self._row(k, r)
                for name, v in p.items():
                    params[f"{name}_{j}"] = v
                ph = [f":k{c}_{j}" for c in range(len(keys))]
                ph += [f":s0n_{j}", f"CAST(:s0d_{j} AS date)", f"CAST(:age_{j} AS timestamptz)",
                       f":ver_{j}", f":n_{j}", f"CAST(:item_{j} AS jsonb)"]
                values.append("(" + ", ".join(ph) + ")")
            self.conn.run(f"INSERT INTO {t} ({', '.join(cols)}) VALUES {', '.join(values)} "
                          f"ON CONFLICT ({', '.join(keys)}) {action}", params)

    def _delete(self, kind, keys):
        self._ddl(kind)
        k, t = ss.KINDS[kind], self._t(kind)
        names = self._keys(k)
        for i in range(0, len(keys), WRITE_CHUNK):
            conds, params = [], {}
            for j, key in enumerate(keys[i:i + WRITE_CHUNK]):
                parts = []
                for c, v in enumerate(key):
                    params[f"d{c}_{j}"] = _key_text(v)
                    parts.append(f"{names[c]} = :d{c}_{j}")
                conds.append("(" + " AND ".join(parts) + ")")
            self.conn.run(f"DELETE FROM {t} WHERE {' OR '.join(conds)}", params)

    def flush(self):
        """All of this flush in one transaction: a crash leaves the store as it was."""
        if not any(self._pending.values()) and not any(self._deletes.values()):
            return
        for kind in [kd for kd, v in self._pending.items() if v] + [kd for kd, v in self._deletes.items() if v]:
            self._ddl(kind)                  # DDL before the transaction
        with _Tx(self.conn):
            super().flush()

    def expire(self, kind, older_than_days):
        k = ss.KINDS[kind]
        if not k.age or not self._exists(kind):
            return
        if k.age[1] == "day":
            cond = "age_at < CAST(current_date - CAST(:d AS integer) AS timestamptz)"
        else:
            cond = "age_at < now() - make_interval(days => CAST(:d AS integer))"
        self.conn.run(f"DELETE FROM {self._t(kind)} WHERE {cond}", {"d": int(older_than_days)})
        self._forget(kind)

    def compact(self, kind):
        return 0                                # upserts keep one row per key


# --- log sink -------------------------------------------------------------------------
PG_TYPES = {"STRING": "text", "BIGINT": "bigint", "INT": "integer", "INTEGER": "integer", "DOUBLE": "double precision",
            "FLOAT": "real", "BOOLEAN": "boolean", "TIMESTAMP": "timestamptz", "DATE": "date"}


def columns(ddl):
    """[(name, SPARK TYPE)] of a declared log DDL."""
    out = []
    for part in ddl.split(","):
        bits = part.split()
        if len(bits) >= 2:
            out.append((bits[0], bits[1].upper()))
    return out


class PostgresLogSink(ss.LogSink):
    """Typed append-only tables, one per declared log kind."""

    iceberg = False

    def __init__(self, config=None, conn=None, schema=None):
        if conn is None:
            conn, schema = connect(config)
        self.conn, self.schema = conn, schema or "advisor"
        self._pending, self._ready = {}, set()

    def _cols(self, kind):
        d = ss.log_kind(kind)
        if d is None:
            raise ValueError(f"log kind {kind!r} is not declared (state_store.declare_log / LOG_HOMES)")
        return columns(d["ddl"])

    def _ensure(self, kind):
        if kind in self._ready:
            return
        cols, t = self._cols(kind), f"{self.schema}.{kind}"
        for _, typ in cols:
            if typ not in PG_TYPES:
                raise ValueError(f"{kind}: column type {typ} has no Postgres mapping")
        self.conn.run(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        self.conn.run(f"CREATE TABLE IF NOT EXISTS {t} ({', '.join(f'{c} {PG_TYPES[ty]}' for c, ty in cols)})")
        for c, ty in cols:                       # columns added to a declaration later
            self.conn.run(f"ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {c} {PG_TYPES[ty]}")
        ts = ss.LOG_KINDS[kind]["ts"]
        self.conn.run(f"CREATE INDEX IF NOT EXISTS {kind}_ts ON {t} ({ts})")
        if any(c == "scan_id" for c, _ in cols):
            self.conn.run(f"CREATE INDEX IF NOT EXISTS {kind}_scan ON {t} (scan_id)")
        self._ready.add(kind)

    def append(self, kind, rows):
        self._pending.setdefault(kind, []).extend(dict(r) for r in rows)

    def pending(self, kind):
        return list(self._pending.get(kind, []))

    @staticmethod
    def _value(v, typ):
        if v is None:
            return None
        if typ == "TIMESTAMP":
            return _ts_text(v)
        if typ == "DATE":
            return _date_text(v)
        if typ in ("BIGINT", "INT", "INTEGER"):
            return int(v)
        if typ in ("DOUBLE", "FLOAT"):
            return float(v)
        if typ == "BOOLEAN":
            return bool(v)
        return v if isinstance(v, str) else (dumps(v) if isinstance(v, (dict, list)) else str(v))

    def _insert(self, kind, rows):
        cols, t = self._cols(kind), f"{self.schema}.{kind}"
        casts = {"TIMESTAMP": "timestamptz", "DATE": "date"}
        for i in range(0, len(rows), WRITE_CHUNK):
            params, values = {}, []
            for j, r in enumerate(rows[i:i + WRITE_CHUNK]):
                ph = []
                for c, (name, typ) in enumerate(cols):
                    params[f"c{c}_{j}"] = self._value(r.get(name), typ)
                    ph.append(f"CAST(:c{c}_{j} AS {casts[typ]})" if typ in casts else f":c{c}_{j}")
                values.append("(" + ", ".join(ph) + ")")
            self.conn.run(f"INSERT INTO {t} ({', '.join(n for n, _ in cols)}) VALUES {', '.join(values)}", params)

    def flush(self):
        todo = {k: v for k, v in self._pending.items() if v}
        if not todo:
            return
        for kind in todo:
            self._ensure(kind)
        with _Tx(self.conn):
            for kind, rows in todo.items():
                self._insert(kind, rows)
        self._pending = {}

    def expire(self, kind, older_than_days):
        if ss.log_kind(kind) is None:
            return
        self._ensure(kind)
        ts = ss.LOG_KINDS[kind]["ts"]
        self.conn.run(f"DELETE FROM {self.schema}.{kind} WHERE {ts} < now() - make_interval(days => CAST(:d AS integer))",
                      {"d": int(older_than_days)})

    def rows(self, kind, eq=None, order_desc=None, limit=None):
        """Logged rows of a kind: eq = {column: value or [values]}, newest first by
        order_desc, at most limit. Values typed as Spark returns them."""
        cols = dict(self._cols(kind))
        r = self.conn.run("SELECT to_regclass(:t) IS NOT NULL", {"t": f"{self.schema}.{kind}"})
        if not (r and r[0][0]):
            return []
        where, params = [], {}
        for i, (c, v) in enumerate((eq or {}).items()):
            if c not in cols:
                raise ValueError(f"{kind} has no column {c}")
            vals = v if isinstance(v, (list, tuple, set)) else [v]
            if not vals:
                return []
            names = []
            for j, x in enumerate(vals):
                params[f"e{i}_{j}"] = self._value(x, cols[c])
                names.append(f"CAST(:e{i}_{j} AS {PG_TYPES[cols[c]]})" if cols[c] in ("TIMESTAMP", "DATE")
                             else f":e{i}_{j}")
            where.append(f"{c} IN ({', '.join(names)})")
        if order_desc and order_desc not in cols:
            raise ValueError(f"{kind} has no column {order_desc}")
        sql = (f"SELECT to_jsonb(t)::text FROM {self.schema}.{kind} t"
               + (" WHERE " + " AND ".join(where) if where else "")
               + (f" ORDER BY {order_desc} DESC NULLS LAST" if order_desc else ""))
        out, off = [], 0
        while True:
            size = getattr(self.conn, "page", PAGE)
            page = min(size, limit - len(out)) if limit else size
            got = self.conn.run(f"{sql} LIMIT {int(page)} OFFSET {int(off)}", params)
            for (text,) in got:
                out.append(self._typed(json.loads(text) if isinstance(text, str) else text, cols))
            off += len(got)
            if len(got) < page or (limit and len(out) >= limit):
                return out

    @staticmethod
    def _typed(d, cols):
        out = {}
        for c, typ in cols.items():
            v = d.get(c)
            if v is not None:
                if typ == "TIMESTAMP":
                    v = ss.parse_ts(v).astimezone(timezone.utc).replace(tzinfo=None)
                elif typ == "DATE":
                    v = date.fromisoformat(v)
                elif typ in ("DOUBLE", "FLOAT"):
                    v = float(v)
            out[c] = v
        return out
