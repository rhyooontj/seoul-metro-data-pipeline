-- 목적: 호선별 API 도착 예정 시간 0초 비율과 관측 레코드 수.
-- 기간: API 생성일(received_date)을 KST로 해석, 2026-09-30 당일 포함.
-- 비율은 0~1. Metabase에서 백분율로 표시하며 비율을 단순 평균하지 않는다.
-- 호선 매핑 출처(2026-10-12 확인): 서울시 공식 realtimeStationArrival 명세
-- https://data.seoul.go.kr/dataList/openApiView.do?infId=OA-12764&srvType=A
-- 미매핑 ID도 LEFT JOIN으로 보존한다. train_line은 호선으로 사용하지 않는다.
with line_mapping(subway_id, subway_name) as (
    values
        ('1001', '1호선'), ('1002', '2호선'), ('1003', '3호선'),
        ('1004', '4호선'), ('1005', '5호선'), ('1006', '6호선'),
        ('1007', '7호선'), ('1008', '8호선'), ('1009', '9호선'),
        ('1063', '경의중앙선'), ('1065', '공항철도'),
        ('1075', '수인분당선'), ('1077', '신분당선')
), totals as (
    select
        subway_id,
        sum(arrival_record_count) as arrival_record_count,
        sum(zero_arrival_count) as zero_arrival_count,
        sum(positive_arrival_count) as positive_arrival_count,
        sum(null_arrival_count) as null_arrival_count,
        sum(negative_arrival_count) as negative_arrival_count
    from analytics.mart_arrival_quality_daily
    where received_date >= date '2026-09-30'
    group by subway_id
)
select
    t.subway_id,
    coalesce(m.subway_name, '미확인 호선 (' || t.subway_id || ')') as subway_name,
    m.subway_id is not null as line_mapping_verified,
    t.arrival_record_count,
    t.zero_arrival_count,
    t.positive_arrival_count,
    t.null_arrival_count,
    t.negative_arrival_count,
    t.zero_arrival_count::numeric / nullif(t.arrival_record_count, 0)
        as zero_arrival_rate
from totals t
left join line_mapping m on t.subway_id = m.subway_id
order by zero_arrival_rate desc, t.subway_id;
