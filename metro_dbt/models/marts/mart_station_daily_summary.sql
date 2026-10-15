{{ config(materialized='table') }}

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
from {{ ref('stg_subway_arrival') }}
group by
    received_at::date,
    station_name,
    direction,
    train_line
