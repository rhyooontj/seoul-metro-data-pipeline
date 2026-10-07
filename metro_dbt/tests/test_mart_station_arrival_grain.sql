select
    station_name,
    direction,
    train_line,
    count(*) as grain_row_count
from {{ ref('mart_station_arrival_summary') }}
group by station_name, direction, train_line
having count(*) > 1
