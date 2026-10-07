{{ config(severity='warn') }}

select
    subway_id,
    station_id,
    station_name,
    direction,
    train_line,
    train_no,
    arrival_seconds,
    arrival_message,
    current_station,
    received_at,
    count(*) as duplicate_count
from {{ source('raw', 'subway_arrival') }}
group by
    subway_id,
    station_id,
    station_name,
    direction,
    train_line,
    train_no,
    arrival_seconds,
    arrival_message,
    current_station,
    received_at
having count(*) > 1
