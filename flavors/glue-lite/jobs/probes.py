"""GL2.5b metric inventory: measure an Iceberg table from its metadata only.

Every metric here comes from Iceberg metadata tables (.files, .partitions,
.snapshots, .manifests, .metadata_log_entries, .all_files) or the table's own
metadata (spec, sort order, properties) -- no data file is read. Metric IDs
match the inventory table in the roadmap (F*, P*, M*, C*, W*).

Two outputs per table:
  partition_metrics(...) -> one row per partition   (F1-F8)
  table_metrics(...)     -> one dict for the table  (P1-P4, M1-M5, M7, M8, C1-C3, W1, W2)
"""
import json
import math
from datetime import datetime, timezone

import gltrace as tr

# Table properties that drive write-configuration findings (W1).
WATCHED_PROPS = [
    "format-version",
    "write.distribution-mode",
    "write.spark.fanout.enabled",
    "write.target-file-size-bytes",
    "write.delete.mode",
    "write.update.mode",
    "write.merge.mode",
    "write.parquet.compression-codec",
    "write.metadata.metrics.default",
    "write.metadata.delete-after-commit.enabled",
    "write.metadata.previous-versions-max",
    "write.object-storage.enabled",
    "commit.manifest-merge.enabled",
    "commit.manifest.min-count-to-merge",
    "history.expire.max-snapshot-age-ms",
    "history.expire.min-snapshots-to-keep",
    "advisor.mode",
]


def table_info(spark, table):
    """Spec, sort order and properties via the Iceberg Java API (py4j)."""
    info = {"partitioned": True, "spec": None, "spec_id": None, "sort_order": None,
            "sort_order_id": None, "sort_defined": False, "format_version": None,
            "properties": {}, "error": None, "uuid": None, "partition_fields": [],
            "metadata_location": None, "metadata_json_bytes": None}
    try:
        jt = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
        spec = jt.spec()
        info["spec"] = spec.toString().replace("\n", " ")
        info["spec_id"] = spec.specId()
        info["partitioned"] = not spec.isUnpartitioned()
        schema = jt.schema()
        for pf in spec.fields().toArray():
            src = pf.sourceId()
            info["partition_fields"].append({
                "name": str(pf.name()), "transform": str(pf.transform().toString()),
                "source": str(schema.findColumnName(src)), "source_type": str(schema.findType(src))})
        so = jt.sortOrder()
        info["sort_order"] = so.toString().replace("\n", " ")
        info["sort_order_id"] = so.orderId()
        info["sort_defined"] = not so.isUnsorted()
        props = jt.properties()
        info["properties"] = {str(k): str(props.get(k)) for k in props.keySet().toArray()}
        try:
            info["metadata_location"] = str(jt.operations().current().metadataFileLocation())
        except Exception:
            info["metadata_location"] = None
        if info["metadata_location"]:
            try:                     # M35: one file-size call on the current metadata.json
                info["metadata_json_bytes"] = int(jt.io().newInputFile(info["metadata_location"]).getLength())
            except Exception:
                info["metadata_json_bytes"] = None
        try:
            info["uuid"] = str(jt.operations().current().uuid())
        except Exception:
            info["uuid"] = None
        try:
            info["format_version"] = int(jt.operations().current().formatVersion())
        except Exception:  # not every Table implementation exposes operations()
            fv = info["properties"].get("format-version")
            info["format_version"] = int(fv) if fv else None
    except Exception as e:  # keep scanning other tables even if one can't be loaded
        info["error"] = f"{type(e).__name__}: {e}"[:500]
    return info


def partition_metrics(spark, table, info, cfg, table_writer_minutes="compute"):
    """F1-F8: one row per partition (delete files included).

    F6 per partition needs all_entries (every retained snapshot's manifests).
    If the table's last writer commit is already outside the hot window, no
    partition can be hot, so that read is skipped and every partition gets
    the table-level age (a lower bound on its own)."""
    if table_writer_minutes == "compute":
        table_writer_minutes = writer_minutes(spark, table)
    hot = float(cfg.get("hot_partition_minutes", 15))
    per_partition_age = table_writer_minutes is not None and table_writer_minutes < hot
    target = int(cfg["target_file_bytes"])
    small = int(target * float(cfg["small_file_ratio"]))
    oversized = int(target * float(cfg["oversized_file_ratio"]))
    pk = "to_json(partition)" if info["partitioned"] else "'{}'"
    spec_id = info["spec_id"] if info["spec_id"] is not None else -1
    sort_id = info["sort_order_id"] if info["sort_order_id"] is not None else -1
    p_join = (f"LEFT JOIN (SELECT to_json(partition) AS partition_key, last_updated_at "
              f"FROM {table}.partitions) p ON a.partition_key = p.partition_key"
              if info["partitioned"] else
              f"LEFT JOIN (SELECT '{{}}' AS partition_key, max(committed_at) AS last_updated_at "
              f"FROM {table}.snapshots) p ON a.partition_key = p.partition_key")
    # F6 counts writer commits only. Iceberg's last_updated_at moves on any
    # commit, including our own compaction ('replace'), which would make a
    # just-fixed partition look hot. Files added by non-replace snapshots
    # still in history give the last writer write; if none are left (history
    # expired), the last write is older than every retained snapshot and the
    # partition is not hot (minutes = NULL).
    epk = "to_json(e.data_file.partition)" if info["partitioned"] else "'{}'"
    if per_partition_age:
        w_join = (f"LEFT JOIN (SELECT {epk} AS partition_key, max(s.committed_at) AS last_write "
                  f"FROM {table}.all_entries e JOIN {table}.snapshots s ON e.snapshot_id = s.snapshot_id "
                  f"WHERE e.status = 1 AND s.operation <> 'replace' GROUP BY 1) w "
                  f"ON a.partition_key = w.partition_key")
        age_sql = ("CAST((unix_timestamp(current_timestamp()) - unix_timestamp(w.last_write)) / 60.0 "
                   "AS DOUBLE)")
    else:
        w_join = ""
        age_sql = ("CAST(NULL AS DOUBLE)" if table_writer_minutes is None
                   else f"CAST({table_writer_minutes} AS DOUBLE)")
    tr.log("partition_metrics", "age per partition from all_entries" if per_partition_age else
           "every partition gets the table's writer age (last writer commit outside the hot window)",
           writer_minutes=table_writer_minutes, hot_window_min=hot, small_below=small, oversized_above=oversized)
    q = f"""
        WITH f AS (
            SELECT {pk} AS partition_key, content, file_size_in_bytes, record_count,
                   spec_id, sort_order_id
            FROM {table}.files
        ),
        a AS (
            SELECT partition_key,
                concat_ws(',', sort_array(collect_set(CAST(spec_id AS STRING)))) AS spec_ids,
                sum(CASE WHEN content = 0 THEN 1 ELSE 0 END)                    AS data_files,
                sum(CASE WHEN content = 1 THEN 1 ELSE 0 END)                    AS delete_files_pos,
                sum(CASE WHEN content = 2 THEN 1 ELSE 0 END)                    AS delete_files_eq,
                sum(CASE WHEN content = 0 THEN record_count ELSE 0 END)         AS records,
                sum(CASE WHEN content <> 0 THEN record_count ELSE 0 END)        AS delete_records,
                sum(CASE WHEN content = 0 THEN file_size_in_bytes ELSE 0 END)   AS data_bytes,
                percentile_approx(CASE WHEN content = 0 THEN file_size_in_bytes END,
                                  array(0.1, 0.5, 0.9))                         AS pct,
                sum(CASE WHEN content = 0 AND file_size_in_bytes < {small} THEN 1 ELSE 0 END)     AS small_files,
                sum(CASE WHEN content = 0 AND file_size_in_bytes > {oversized} THEN 1 ELSE 0 END) AS oversized_files,
                sum(CASE WHEN content = 0 AND (file_size_in_bytes < {small} OR file_size_in_bytes > {oversized})
                         THEN file_size_in_bytes ELSE 0 END)                                AS rewrite_bytes,
                sum(CASE WHEN content = 0 AND spec_id <> {spec_id} THEN 1 ELSE 0 END)             AS files_old_spec,
                sum(CASE WHEN content = 0 AND sort_order_id = {sort_id} THEN 1 ELSE 0 END)        AS files_current_sort
            FROM f GROUP BY partition_key
        )
        SELECT a.partition_key, spec_ids,
               CAST(data_files AS BIGINT)        AS data_files,
               CAST(delete_files_pos AS BIGINT)  AS delete_files_pos,
               CAST(delete_files_eq AS BIGINT)   AS delete_files_eq,
               CAST(records AS BIGINT)           AS records,
               CAST(delete_records AS BIGINT)    AS delete_records,
               CAST(data_bytes AS BIGINT)        AS data_bytes,
               CAST(data_bytes / greatest(data_files, 1) AS BIGINT) AS avg_file_bytes,
               CAST(pct[0] AS BIGINT) AS p10_file_bytes,
               CAST(pct[1] AS BIGINT) AS p50_file_bytes,
               CAST(pct[2] AS BIGINT) AS p90_file_bytes,
               CAST(small_files AS BIGINT)       AS small_files,
               CAST(oversized_files AS BIGINT)   AS oversized_files,
               CAST(rewrite_bytes AS BIGINT)     AS rewrite_bytes,
               CAST(greatest(1, ceil(data_bytes / {target})) AS BIGINT)               AS ideal_files,
               CAST(data_files - greatest(1, ceil(data_bytes / {target})) AS BIGINT)  AS excess_files,
               CAST(files_old_spec AS BIGINT)     AS files_old_spec,
               CAST(files_current_sort AS BIGINT) AS files_current_sort,
               p.last_updated_at,
               {age_sql} AS minutes_since_update,
               CAST({target} AS BIGINT) AS target_file_bytes
        FROM a {p_join} {w_join}
        ORDER BY a.partition_key
    """
    tr.sql(q)
    return spark.sql(q)


def _one(spark, sql):
    r = spark.sql(sql).collect()[0]
    tr.sql(sql, r.asDict())
    return r


def column_types(spark, table):
    return {f.name: f.dataType.simpleString() for f in spark.table(table).schema.fields}


def _pruning(spark, table, info, columns):
    """C1/C3 per declared filter column, from per-file min/max bounds.

    Within each partition with >= 2 data files: expected files touched by a
    uniform point lookup ~= sum(file range width) / partition range width.
    efficiency = 1 - (expected - 1) / (files - 1): 1 = ranges don't overlap
    (sorted), 0 = every file spans the whole range (no skipping possible).
    Table value = file-weighted average across partitions.
    """
    out = {}
    if not columns:
        return out
    types = column_types(spark, table)
    pk = "to_json(partition)" if info["partitioned"] else "'{}'"
    for col in columns:
        t = types.get(col)
        if t is None:
            out[col] = {"status": "missing column"}
            continue
        if t in ("int", "bigint", "smallint", "tinyint", "double", "float") or t.startswith("decimal"):
            conv = "CAST({} AS DOUBLE)"
        elif t.startswith("timestamp"):
            conv = "CAST({} AS DOUBLE)"
        elif t == "date":
            conv = "CAST(unix_date({}) AS DOUBLE)"
        else:
            out[col] = {"status": f"type {t} not measured"}
            continue
        lb = conv.format(f"readable_metrics.`{col}`.lower_bound")
        ub = conv.format(f"readable_metrics.`{col}`.upper_bound")
        r = _one(spark, f"""
            WITH f AS (
                SELECT {pk} AS pk, {lb} AS lb, {ub} AS ub
                FROM {table}.files WHERE content = 0
            ),
            stats AS (SELECT count(*) AS files, count(lb) AS with_bounds FROM f),
            p AS (
                SELECT pk, count(*) AS n, min(lb) AS mn, max(ub) AS mx
                FROM f WHERE lb IS NOT NULL GROUP BY pk HAVING count(*) >= 2
            ),
            e AS (
                SELECT p.pk, p.n,
                       CASE WHEN p.mx = p.mn THEN 1.0
                            ELSE greatest(0.0, least(1.0,
                                 1 - (sum((f.ub - f.lb) / (p.mx - p.mn)) - 1) / (p.n - 1)))
                       END AS eff
                FROM p JOIN f ON f.pk = p.pk AND f.lb IS NOT NULL
                GROUP BY p.pk, p.n, p.mn, p.mx
            )
            SELECT (SELECT files FROM stats) AS files,
                   (SELECT with_bounds FROM stats) AS with_bounds,
                   sum(eff * n) / sum(n) AS efficiency,
                   count(*) AS partitions_measured
            FROM e
        """)
        if not r.files:
            out[col] = {"status": "no data files"}
        elif r.with_bounds == 0:
            out[col] = {"status": "no column stats"}               # C3
        else:
            out[col] = {"status": "ok",
                        "efficiency": None if r.efficiency is None else round(float(r.efficiency), 3),
                        "partitions_measured": int(r.partitions_measured),
                        "stats_coverage": round(r.with_bounds / r.files, 3)}
    return out


def writer_minutes(spark, table):
    """Minutes since the last commit by a writer (anything but 'replace',
    i.e. not compaction), from snapshots alone. None if none is left."""
    r = _one(spark, f"""
        SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(max(committed_at))) / 60.0 AS m
        FROM {table}.snapshots WHERE operation <> 'replace'""")
    return None if r.m is None else float(r.m)


def snapshot_metrics(spark, table, cfg, data_bytes):
    """M1-M4, M7, W2: everything that comes from snapshots and the metadata
    log, i.e. from metadata.json alone. Cheap, so a reused scan of an
    unchanged table still refreshes these (their time windows move)."""
    recent = int(cfg["recent_commits"])
    m = {"data_bytes": data_bytes}
    # M1-M4: snapshots and commit pattern
    s = _one(spark, f"""
        SELECT count(*) AS n,
               (unix_timestamp(current_timestamp()) - unix_timestamp(min(committed_at))) / 3600.0 AS oldest_h,
               sum(CASE WHEN committed_at >= current_timestamp() - INTERVAL 1 HOUR THEN 1 ELSE 0 END) AS c1h,
               sum(CASE WHEN committed_at >= current_timestamp() - INTERVAL 24 HOURS THEN 1 ELSE 0 END) AS c24h
        FROM {table}.snapshots
    """)
    m["snapshots"] = int(s.n)
    m["oldest_snapshot_age_h"] = round(float(s.oldest_h), 2) if s.oldest_h is not None else None
    m["commits_1h"] = int(s.c1h or 0)
    m["commits_24h"] = int(s.c24h or 0)
    c = _one(spark, f"""
        SELECT avg(CAST(summary['added-data-files'] AS DOUBLE))        AS files,
               avg(CAST(summary['added-files-size'] AS DOUBLE))        AS bytes,
               avg(CAST(summary['changed-partition-count'] AS DOUBLE)) AS parts
        FROM (SELECT summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT {recent})
    """)
    m["avg_added_files_per_commit"] = round(float(c.files), 2) if c.files is not None else None
    m["avg_added_bytes_per_commit"] = round(float(c.bytes), 0) if c.bytes is not None else None
    m["avg_changed_partitions_per_commit"] = round(float(c.parts), 2) if c.parts is not None else None

    # W2: rewrite churn from overwrite commits (copy-on-write MERGE/UPDATE/
    # DELETE, INSERT OVERWRITE, full refreshes). Compaction is 'replace' and
    # merge-on-read row deltas delete no data files, so neither counts.
    # Share = data files a commit removed / data files the table had before it.
    w = _one(spark, f"""
        WITH o AS (
            SELECT committed_at,
                   CAST(coalesce(summary['deleted-data-files'], '0') AS DOUBLE) AS del_files,
                   CAST(coalesce(summary['added-data-files'], '0') AS DOUBLE)   AS add_files,
                   CAST(summary['total-data-files'] AS DOUBLE)                  AS total_files,
                   CAST(coalesce(summary['removed-files-size'], '0') AS DOUBLE) AS removed_bytes
            FROM (SELECT committed_at, operation, summary FROM {table}.snapshots
                  ORDER BY committed_at DESC LIMIT {recent})
            WHERE operation = 'overwrite' AND CAST(coalesce(summary['deleted-data-files'], '0') AS INT) > 0
        )
        SELECT count(*) AS n,
               avg(del_files / greatest(total_files - add_files + del_files, 1)) AS share,
               sum(CASE WHEN committed_at >= current_timestamp() - INTERVAL 24 HOURS THEN 1 ELSE 0 END) AS n24,
               sum(CASE WHEN committed_at >= current_timestamp() - INTERVAL 24 HOURS
                        THEN removed_bytes ELSE 0 END) AS bytes24
        FROM o
    """)
    m["overwrite_commits_recent"] = int(w.n or 0)
    m["avg_overwrite_rewrite_share"] = round(float(w.share), 3) if w.share is not None else None
    m["overwrite_commits_24h"] = int(w.n24 or 0)
    m["rewritten_bytes_24h"] = int(w.bytes24 or 0)
    m["table_turnover_24h"] = (round(m["rewritten_bytes_24h"] / m["data_bytes"], 2)
                               if m["data_bytes"] else None)

    # M7: metadata.json versions still tracked
    m["metadata_versions"] = int(_one(spark, f"SELECT count(*) AS n FROM {table}.metadata_log_entries").n)

    m.pop("data_bytes", None)
    return m


def table_metrics(spark, table, info, cfg, pm_rows, retained=True):
    """P1-P4, M1-M5, M7, M8, C1-C3, W1 for one table; pm_rows = partition metric rows."""
    target = int(cfg["target_file_bytes"])
    undersized_limit = target * float(cfg["undersized_partition_ratio"])
    recent = int(cfg["recent_commits"])
    m = {}

    # P1-P4: partition-level aggregates
    sizes = sorted(int(r.data_bytes or 0) for r in pm_rows)
    m["partitions"] = len(pm_rows)
    m["data_files"] = sum(int(r.data_files or 0) for r in pm_rows)
    m["delete_files"] = sum(int((r.delete_files_pos or 0) + (r.delete_files_eq or 0)) for r in pm_rows)
    m["records"] = sum(int(r.records or 0) for r in pm_rows)
    m["data_bytes"] = sum(sizes)
    m["avg_file_bytes"] = m["data_bytes"] // m["data_files"] if m["data_files"] else 0
    ideal = sum(int(r.ideal_files or 0) for r in pm_rows)
    m["read_amplification"] = round(m["data_files"] / ideal, 2) if ideal else None
    m["partitions_with_excess"] = sum(1 for r in pm_rows if (r.excess_files or 0) >= 1)
    m["excess_files_total"] = sum(max(0, int(r.excess_files or 0)) for r in pm_rows)
    if sizes:
        median = sizes[len(sizes) // 2] if len(sizes) % 2 else (sizes[len(sizes) // 2 - 1] + sizes[len(sizes) // 2]) / 2
        m["skew_ratio"] = round(sizes[-1] / median, 2) if median else None
        top_n = max(1, math.ceil(len(sizes) * 0.01))
        m["top1pct_share"] = round(sum(sizes[-top_n:]) / m["data_bytes"], 3) if m["data_bytes"] else None
        m["undersized_partition_share"] = round(sum(1 for s in sizes if s < undersized_limit) / len(sizes), 3)
    else:
        m["skew_ratio"] = m["top1pct_share"] = m["undersized_partition_share"] = None

    m.update(snapshot_metrics(spark, table, cfg, m["data_bytes"]))

    # M5: manifests of the current snapshot (data manifests only)
    mf = _one(spark, f"""
        SELECT count(*) AS n, avg(length) AS avg_len
        FROM {table}.manifests WHERE content = 0
    """)
    m["data_manifests"] = int(mf.n)
    m["avg_manifest_bytes"] = round(float(mf.avg_len), 0) if mf.avg_len is not None else None

    # M8: bytes only old snapshots reference (storage you pay for but can't query).
    # Reads the manifests of every retained snapshot; the scan runs it less often.
    if not retained:
        m["retained_bytes"] = m["retained_metadata_bytes"] = None
    else:
        # M36: manifests only old snapshots reference (manifest lists are not
        # counted: their sizes need one file-size call per snapshot)
        r = _one(spark, f"""
            SELECT
              (SELECT coalesce(sum(len), 0) FROM
                 (SELECT path, max(length) AS len FROM {table}.all_manifests GROUP BY path)) -
              (SELECT coalesce(sum(length), 0) FROM {table}.manifests) AS retained
        """)
        m["retained_metadata_bytes"] = max(0, int(r.retained or 0))
        r = _one(spark, f"""
            SELECT
              (SELECT coalesce(sum(sz), 0) FROM
                 (SELECT file_path, max(file_size_in_bytes) AS sz FROM {table}.all_files GROUP BY file_path)) -
              (SELECT coalesce(sum(file_size_in_bytes), 0) FROM {table}.files) AS retained
        """)
        m["retained_bytes"] = int(r.retained or 0)

    # C1-C3: clustering on declared filter columns
    cols = list(cfg.get("filter_columns") or [])
    pruning = _pruning(spark, table, info, cols)
    effs = [v["efficiency"] for v in pruning.values() if v.get("efficiency") is not None]
    m["filter_columns"] = ",".join(cols)
    m["pruning_json"] = json.dumps(pruning, sort_keys=True)
    m["min_pruning_efficiency"] = min(effs) if effs else None
    m["sort_order_defined"] = bool(info["sort_defined"])
    m["sort_order"] = info["sort_order"]

    # W1: table metadata and write configuration
    props = info["properties"]
    m["format_version"] = info["format_version"]
    m["partition_spec"] = info["spec"]
    m["current_spec_id"] = info["spec_id"]
    m["distribution_mode"] = props.get("write.distribution-mode")
    m["write_delete_mode"] = props.get("write.delete.mode")
    m["write_update_mode"] = props.get("write.update.mode")
    m["write_merge_mode"] = props.get("write.merge.mode")
    m["manifest_merge_enabled"] = props.get("commit.manifest-merge.enabled")
    m["properties_json"] = json.dumps({k: props[k] for k in WATCHED_PROPS if k in props}, sort_keys=True)
    m["target_file_bytes"] = target
    m["load_error"] = info["error"]
    m["table_uuid"] = info.get("uuid")
    m["partition_fields_json"] = json.dumps(info.get("partition_fields") or [])
    m["target_source"] = cfg.get("target_source")
    return m


def _norm_uri(path):
    """s3a://b/k and s3n://b/k name the same object as s3://b/k."""
    p = str(path)
    for scheme in ("s3a://", "s3n://"):
        if p.startswith(scheme):
            return "s3://" + p[len(scheme):]
    return p


def orphan_metrics(spark, table, cfg):
    """O1: objects under the table location that no retained snapshot or
    metadata version references, older than orphan_min_age_minutes (younger
    ones may belong to a commit still in flight). Lists the location through
    the table's FileIO (S3 prefix listing), so it costs one listing of every
    object under the table plus the all_files / all_manifests reads; run it
    sparingly on big tables."""
    jvm = spark._jvm
    jt = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    cur = jt.operations().current()
    prefix = str(jt.location()).rstrip("/") + "/"          # trailing '/': not sibling tables
    refs = {_norm_uri(cur.metadataFileLocation())}
    for sql in (f"SELECT file_path AS p FROM {table}.all_files",
                f"SELECT path AS p FROM {table}.all_manifests",
                f"SELECT manifest_list AS p FROM {table}.snapshots",
                f"SELECT file AS p FROM {table}.metadata_log_entries"):
        refs.update(_norm_uri(r.p) for r in spark.sql(sql).collect() if r.p)
    for getter in ("statisticsFiles", "partitionStatisticsFiles"):
        try:
            for sf in getattr(cur, getter)().toArray():
                refs.add(_norm_uri(sf.path()))
        except Exception:
            pass
    cutoff_ms = (now_utc().timestamp() - float(cfg.get("orphan_min_age_minutes", 4320)) * 60) * 1000
    listed = orphans = orphan_bytes = meta_json = 0
    sample = []
    it = jt.io().listPrefix(prefix).iterator()
    while it.hasNext():
        f = it.next()
        listed += 1
        loc = _norm_uri(f.location())
        if loc.endswith(".metadata.json") and "/metadata/" in loc:
            meta_json += 1                   # every metadata.json version still in storage (A9 check)
        if loc in refs or f.createdAtMillis() > cutoff_ms:
            continue
        orphans += 1
        orphan_bytes += int(f.size())
        if len(sample) < 5:
            sample.append(loc[len(prefix):] if loc.startswith(prefix) else loc)
    return {"orphan_files": orphans, "orphan_bytes": orphan_bytes, "listed_objects": listed,
            "orphan_sample": json.dumps(sample), "metadata_json_files": meta_json}


def now_utc():
    # Timezone-aware on purpose: Spark converts naive datetimes using the
    # driver's local zone, which shifts the stored instant on non-UTC hosts.
    return datetime.now(timezone.utc)
