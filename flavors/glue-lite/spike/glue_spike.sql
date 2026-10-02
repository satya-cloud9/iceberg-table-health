-- Glue spike: can Iceberg's GlueCatalog commit against Floci's Glue emulation?
-- Every statement here exercises a different Glue/S3 call:
--   CREATE NAMESPACE  -> glue:CreateDatabase
--   CREATE TABLE      -> glue:CreateTable + S3 metadata write
--   INSERT (x2)       -> S3 data write + glue:UpdateTable with a version check
--                        (the conditional swap every Iceberg commit relies on)
--   metadata queries  -> S3 manifest reads through the catalog

CREATE NAMESPACE IF NOT EXISTS glue.demo;

DROP TABLE IF EXISTS glue.demo.spike_events PURGE;

CREATE TABLE glue.demo.spike_events (
    event_id    STRING,
    event_type  STRING,
    occurred_at TIMESTAMP,
    customer_id INT
)
USING iceberg
PARTITIONED BY (days(occurred_at));

INSERT INTO glue.demo.spike_events VALUES
    ('evt-1', 'signup',    TIMESTAMP '2026-09-01 10:00:00', 1),
    ('evt-2', 'page_view', TIMESTAMP '2026-09-01 11:00:00', 2),
    ('evt-3', 'purchase',  TIMESTAMP '2026-09-02 09:30:00', 3);

INSERT INTO glue.demo.spike_events VALUES
    ('evt-4', 'logout',    TIMESTAMP '2026-09-02 18:00:00', 3),
    ('evt-5', 'signup',    TIMESTAMP '2026-09-03 08:15:00', 4);

-- Expect 5 rows.
SELECT count(*) AS row_count FROM glue.demo.spike_events;

-- Expect 2 'append' snapshots (one per INSERT).
SELECT snapshot_id, operation FROM glue.demo.spike_events.snapshots ORDER BY committed_at;

-- Expect 3 day partitions; this is the metadata the Advisor will score.
SELECT partition, record_count, file_count FROM glue.demo.spike_events.partitions;
