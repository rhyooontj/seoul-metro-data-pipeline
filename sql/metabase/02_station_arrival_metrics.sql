-- 목적: 역별 API 도착 예정 시간 전체 수치 평균, 양수 조건부 평균, 0초 비율.
-- 기간: API 생성일 기준 2026-09-30 당일 포함. 시간 단위: 초, 비율: 0~1.
-- 역명별로 호선과 방향을 통합하므로 호선 구성/양수 제공 범위의 영향을 받는다.
-- subway_ids는 실제 호선 ID 목록이다. train_line으로 호선을 추정하지 않는다.
-- AVG(그룹 평균), AVG(그룹 비율)을 사용하지 않는다. 양수 표본 없음은 NULL.
select
    station_name,
    string_agg(distinct subway_id, ', ' order by subway_id) as subway_ids,
    sum(arrival_record_count) as arrival_record_count,
    sum(zero_arrival_count) as zero_arrival_count,
    sum(positive_arrival_count) as positive_arrival_count,
    sum(null_arrival_count) as null_arrival_count,
    sum(negative_arrival_count) as negative_arrival_count,
    sum(zero_arrival_count)::numeric / nullif(sum(arrival_record_count), 0)
        as zero_arrival_rate,
    sum(arrival_seconds_sum) as arrival_seconds_sum,
    sum(positive_arrival_seconds_sum) as positive_arrival_seconds_sum,
    sum(arrival_seconds_sum)::numeric
        / nullif(sum(arrival_record_count - null_arrival_count), 0)
        as avg_arrival_seconds,
    sum(positive_arrival_seconds_sum)::numeric
        / nullif(sum(positive_arrival_count), 0)
        as avg_positive_arrival_seconds
from analytics.mart_arrival_quality_daily
where received_date >= date '2026-09-30'
group by station_name
order by station_name;
