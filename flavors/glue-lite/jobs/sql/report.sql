-- GL2 demo report: the before/after story from the ops tables.
-- Compares the two most recent health runs, and the latest 'before' and
-- 'after' benchmark runs, so it stays correct across repeated demos.

-- 1. File counts per partition: previous health run vs latest.
WITH runs AS (
    SELECT run_id, row_number() OVER (ORDER BY max(measured_at) DESC) AS rn
    FROM glue.ops.table_health
    WHERE table_name = 'glue.demo.events'
    GROUP BY run_id
)
SELECT b.partition_key,
       b.data_files                      AS files_before,
       a.data_files                      AS files_after,
       round(b.avg_file_bytes / 1024, 1) AS avg_kb_before,
       round(a.avg_file_bytes / 1024, 1) AS avg_kb_after,
       a.needs_compaction                AS still_flagged
FROM glue.ops.table_health b
JOIN glue.ops.table_health a ON a.partition_key = b.partition_key
WHERE b.run_id = (SELECT run_id FROM runs WHERE rn = 2)
  AND a.run_id = (SELECT run_id FROM runs WHERE rn = 1)
ORDER BY b.partition_key;

-- 2. What each compaction did (latest 20 partitions).
SELECT started_at, partition_key AS day, files_before, files_after,
       duration_s, rewritten_files, added_files, failed_files
FROM glue.ops.compaction_runs
ORDER BY started_at DESC
LIMIT 20;

-- 3. Median read time per query: latest 'before' run vs latest 'after' run.
WITH runs AS (
    SELECT label, run_id, max(measured_at) AS at
    FROM glue.ops.read_benchmarks GROUP BY label, run_id
),
latest AS (
    SELECT label, max_by(run_id, at) AS run_id FROM runs GROUP BY label
)
SELECT b.day, b.query_name,
       b.files_in_partition AS files_before,
       a.files_in_partition AS files_after,
       b.median_ms          AS ms_before,
       a.median_ms          AS ms_after,
       round(b.median_ms / a.median_ms, 2) AS speedup
FROM glue.ops.read_benchmarks b
JOIN glue.ops.read_benchmarks a ON a.day = b.day AND a.query_name = b.query_name
WHERE b.run_id = (SELECT run_id FROM latest WHERE label = 'before')
  AND a.run_id = (SELECT run_id FROM latest WHERE label = 'after')
ORDER BY b.day, b.query_name;
