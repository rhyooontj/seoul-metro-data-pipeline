select received_date, station_name, subway_id, direction
from {{ ref('mart_arrival_quality_daily') }}
group by received_date, station_name, subway_id, direction
having count(*) > 1
