{{ config(materialized='table') }}

-- received_at is the API-generated timestamp, interpreted as Korean local time.
-- Keep every observation and every date, including zero-only groups.
select
    received_at::date as received_date,
    station_name,
    subway_id,
    direction,
    count(*) as arrival_record_count,
    count(*) filter (where arrival_seconds = 0) as zero_arrival_count,
    count(*) filter (where arrival_seconds > 0) as positive_arrival_count,
    count(*) filter (where arrival_seconds is null) as null_arrival_count,
    count(*) filter (where arrival_seconds < 0) as negative_arrival_count,
    count(*) filter (where arrival_seconds = 0)::numeric
        / nullif(count(*), 0) as zero_arrival_rate,
    avg(arrival_seconds) as avg_arrival_seconds,
    avg(arrival_seconds) filter (where arrival_seconds > 0)
        as avg_positive_arrival_seconds,
    sum(arrival_seconds) as arrival_seconds_sum,
    coalesce(sum(arrival_seconds) filter (where arrival_seconds > 0), 0)
        as positive_arrival_seconds_sum
from {{ ref('stg_subway_arrival') }}
group by received_at::date, station_name, subway_id, direction
