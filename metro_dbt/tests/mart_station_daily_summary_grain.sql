-- Each date/station/direction/train-line combination must be unique.
select
    received_date,
    station_name,
    direction,
    train_line
from {{ ref('mart_station_daily_summary') }}
group by received_date, station_name, direction, train_line
having count(*) > 1
