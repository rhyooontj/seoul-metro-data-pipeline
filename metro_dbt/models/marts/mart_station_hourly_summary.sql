{{ config(materialized='table') }}

select
    received_at::date as received_date,
    extract(hour from received_at)::integer as received_hour,
    station_name,
    direction,

    count(*) as arrival_record_count,

    round(avg(arrival_seconds), 2) as avg_arrival_seconds,

    min(arrival_seconds) as min_arrival_seconds,
    max(arrival_seconds) as max_arrival_seconds,

    max(received_at) as latest_received_at

from {{ ref('stg_subway_arrival') }}

group by
    received_at::date,
    extract(hour from received_at),
    station_name,
    direction