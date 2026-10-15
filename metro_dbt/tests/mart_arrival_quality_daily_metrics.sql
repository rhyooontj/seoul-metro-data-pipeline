-- Conditional NULLs are part of the metric contract, not missing groups.
select *
from {{ ref('mart_arrival_quality_daily') }}
where arrival_record_count <= 0
   or zero_arrival_count < 0
   or positive_arrival_count < 0
   or null_arrival_count < 0
   or negative_arrival_count < 0
   or arrival_record_count != zero_arrival_count + positive_arrival_count
                              + null_arrival_count + negative_arrival_count
   or zero_arrival_rate < 0
   or zero_arrival_rate > 1
   or zero_arrival_rate is distinct from
       zero_arrival_count::numeric / nullif(arrival_record_count, 0)
   or (positive_arrival_count = 0 and
       (avg_positive_arrival_seconds is not null or positive_arrival_seconds_sum != 0))
   or (positive_arrival_count > 0 and
       (avg_positive_arrival_seconds is null or avg_positive_arrival_seconds <= 0
        or positive_arrival_seconds_sum <= 0
        or avg_positive_arrival_seconds is distinct from
           positive_arrival_seconds_sum::numeric / positive_arrival_count))
   or (arrival_record_count = null_arrival_count and
       (avg_arrival_seconds is not null or arrival_seconds_sum is not null))
   or (arrival_record_count > null_arrival_count and
       (avg_arrival_seconds is null or arrival_seconds_sum is null
        or avg_arrival_seconds is distinct from
           arrival_seconds_sum::numeric / (arrival_record_count - null_arrival_count)))
