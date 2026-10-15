-- No hardcoded production counts: compare both requested scopes to live raw.
with expected as (
    select 'all' as scope, count(*) as n
    from {{ source('raw', 'subway_arrival') }}
    union all
    select 'since_20260930', count(*)
    from {{ source('raw', 'subway_arrival') }}
    where received_at >= timestamp '2026-09-30 00:00:00'
), actual as (
    select 'all' as scope, coalesce(sum(arrival_record_count), 0) as n
    from {{ ref('mart_arrival_quality_daily') }}
    union all
    select 'since_20260930', coalesce(sum(arrival_record_count), 0)
    from {{ ref('mart_arrival_quality_daily') }}
    where received_date >= date '2026-09-30'
)
select expected.scope, expected.n as expected_count, actual.n as actual_count
from expected join actual using (scope)
where expected.n != actual.n
