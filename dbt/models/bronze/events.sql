{#- M0 smoke model: proves dbt -> Trino -> Nessie -> S3/MinIO writes a
    day-partitioned Iceberg table. Generates 7 days x 1,000 synthetic events.
    Later milestones reuse this table as the fragmentation target. -#}
{{ config(
    materialized='table',
    properties={
      "partitioning": "ARRAY['day(occurred_at)']",
      "format": "'PARQUET'"
    }
) }}

with days as (
    select d from unnest(sequence(0, 6)) as t(d)
),
seq as (
    select n from unnest(sequence(1, 1000)) as t(n)
)

select
    concat('evt-', cast(d as varchar), '-', cast(n as varchar))      as event_id,
    element_at(array['signup', 'page_view', 'purchase', 'logout'],
               (n % 4) + 1)                                           as event_type,
    cast(date_add('second', n * 60,
             date_add('day', d, timestamp '2026-09-01 00:00:00'))
         as timestamp(6))                                             as occurred_at,
    n % 50                                                            as customer_id
from days
cross join seq
