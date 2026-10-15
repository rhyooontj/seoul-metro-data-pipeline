-- Independently aggregate raw, bypassing staging; compare both directions.
with expected as (
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
    from {{ source('raw', 'subway_arrival') }}
    group by received_at::date, station_name, subway_id, direction
), actual as (
    select received_date, station_name, subway_id, direction,
           arrival_record_count, zero_arrival_count, positive_arrival_count,
           null_arrival_count, negative_arrival_count, zero_arrival_rate,
           avg_arrival_seconds, avg_positive_arrival_seconds,
           arrival_seconds_sum, positive_arrival_seconds_sum
    from {{ ref('mart_arrival_quality_daily') }}
)
(select * from expected except all select * from actual)
union all
(select * from actual except all select * from expected)
