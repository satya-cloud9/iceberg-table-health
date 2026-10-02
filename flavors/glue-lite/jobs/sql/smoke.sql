-- GL0 in-cluster smoke test: the same checks as the host spike, but run as a
-- SparkApplication so it also proves pod -> Floci networking, the image and
-- the Spark Operator. Uses its own table so it never touches spike_events.

CREATE NAMESPACE IF NOT EXISTS glue.demo;

DROP TABLE IF EXISTS glue.demo.smoke_events PURGE;

CREATE TABLE glue.demo.smoke_events (
    event_id    STRING,
    event_type  STRING,
    occurred_at TIMESTAMP,
    customer_id INT
)
USING iceberg
PARTITIONED BY (days(occurred_at));

INSERT INTO glue.demo.smoke_events VALUES
    ('evt-1', 'signup',    TIMESTAMP '2026-09-01 10:00:00', 1),
    ('evt-2', 'page_view', TIMESTAMP '2026-09-01 11:00:00', 2),
    ('evt-3', 'purchase',  TIMESTAMP '2026-09-02 09:30:00', 3);

INSERT INTO glue.demo.smoke_events VALUES
    ('evt-4', 'logout',    TIMESTAMP '2026-09-02 18:00:00', 3),
    ('evt-5', 'signup',    TIMESTAMP '2026-09-03 08:15:00', 4);

-- Expect 5.
SELECT count(*) AS row_count FROM glue.demo.smoke_events;

-- Expect 2 'append' snapshots.
SELECT snapshot_id, operation FROM glue.demo.smoke_events.snapshots ORDER BY committed_at;

-- Expect 3 partitions.
SELECT partition, record_count, file_count FROM glue.demo.smoke_events.partitions;
