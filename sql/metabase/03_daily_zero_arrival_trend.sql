-- 목적: API 생성일별 도착 예정 시간 0초 비율과 표본 수 추이.
-- received_date는 API 수신 날짜라는 프로젝트 명칭을 쓰지만 수집 날짜가 아니다.
-- 기간: 2026-09-30 당일 포함, 이후 전체. 비율: 0~1, 건수: API 관측 레코드.
-- 없는 날짜를 0건/0%로 채우지 않는다. 수집 공백과 실제 0%를 구분한다.
select
    received_date,
    sum(arrival_record_count) as arrival_record_count,
    sum(zero_arrival_count) as zero_arrival_count,
    sum(positive_arrival_count) as positive_arrival_count,
    sum(null_arrival_count) as null_arrival_count,
    sum(negative_arrival_count) as negative_arrival_count,
    sum(zero_arrival_count)::numeric / nullif(sum(arrival_record_count), 0)
        as zero_arrival_rate
from analytics.mart_arrival_quality_daily
where received_date >= date '2026-09-30'
group by received_date
order by received_date;
