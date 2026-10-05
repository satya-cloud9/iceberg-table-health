"""--trace: step-by-step output for chosen tables, to read alongside the code.

Every line starts with "TRACE <table> |" so it can be grepped out of the job
log. Off unless enable() was called, so normal scans print nothing extra.

What is traced (see scan-execution-paths.md for the order):
  scan     table_info, the path taken (full / reused / failed) and why, the SQL
           each probe ran with the row it returned, every partition row, the
           retained / orphan due decisions, the table row written
  ledger   family 1 (ingest event, new snapshots, check and result), family 2
           (each new (snapshot, partition) row with its label, partition state
           changes), family 3 (learned windows)
  detect   each evaluation (today / learned windows / partition holds / new
           findings): per partition the values and every decision, per table
           rule the values against its threshold, then each family's diff
  score    the scorecard verdict

Pure Python; no Spark import.
"""
import json
import textwrap

TABLES = set()
_current = None
_label = ""


def enable(tables):
    for t in tables or []:
        t = t.strip()
        if t:
            TABLES.add(t)


def matches(table):
    return bool(table) and (table in TABLES or table.rsplit(".", 1)[-1] in TABLES)


def begin(table):
    """Trace this table from now on (None or an untraced table: stop)."""
    global _current
    _current = table if matches(table) else None


def active():
    return _current is not None


def variant(label):
    """Name of the evaluation running now (today, learned_windows, ...)."""
    global _label
    _label = label or ""


def _fmt(v):
    if isinstance(v, float):
        return f"{v:.4g}"
    if isinstance(v, (dict, list)):
        return json.dumps(v, default=str, sort_keys=True)[:400]
    return str(v)


def log(step, msg="", **kv):
    if _current is None:
        return
    short = _current.rsplit(".", 1)[-1]
    tag = f"{step}[{_label}]" if _label and step.startswith("rule") else step
    extra = "  ".join(f"{k}={_fmt(v)}" for k, v in kv.items())
    print(f"TRACE {short} | {tag}: {msg}{'  ' if msg and extra else ''}{extra}", flush=True)


def sql(text, result=None):
    """The SQL a probe ran (dedented) and, when given, what it returned."""
    if _current is None:
        return
    short = _current.rsplit(".", 1)[-1]
    body = textwrap.dedent(text).strip("\n")
    print(f"TRACE {short} | sql:\n" + textwrap.indent(body, "    "), flush=True)
    if result is not None:
        print(f"TRACE {short} | sql -> {_fmt(result)}", flush=True)


def rows(step, items, keys):
    """One line per row (dicts or Spark Rows), selected keys only."""
    if _current is None:
        return
    for r in items:
        d = r if isinstance(r, dict) else r.asDict()
        log(step, **{k: d.get(k) for k in keys})
