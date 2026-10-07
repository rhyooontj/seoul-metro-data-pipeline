select *
from {{ ref('mart_station_arrival_summary') }}
where arrival_count <= 0
   or min_arrival_seconds > avg_arrival_seconds
   or avg_arrival_seconds > max_arrival_seconds
