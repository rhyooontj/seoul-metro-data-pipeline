select
    id as arrival_id,
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
from {{ source('raw', 'subway_arrival') }}