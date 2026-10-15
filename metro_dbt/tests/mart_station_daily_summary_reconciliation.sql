-- Compare every aggregate directly with raw data, including anomalous API times.
with expected as (
    select
        received_at::date as received_date,
        station_name,
        direction,
        train_line,
        count(*) as arrival_record_count,
        round(avg(arrival_seconds), 2) as avg_arrival_seconds,
        min(arrival_seconds) as min_arrival_seconds,
        max(arrival_seconds) as max_arrival_seconds,
        max(received_at) as latest_received_at
    from {{ source('raw', 'subway_arrival') }}
    group by received_at::date, station_name, direction, train_line
), actual as (
    select
        received_date,
        station_name,
        direction,
        train_line,
        arrival_record_count,
        avg_arrival_seconds,
        min_arrival_seconds,
        max_arrival_seconds,
        latest_received_at
    from {{ ref('mart_station_daily_summary') }}
)
(select * from expected except all select * from actual)
union all
(select * from actual except all select * from expected)
