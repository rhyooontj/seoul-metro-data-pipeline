# Metabase 도착정보 품질 시각화용 SQL

이번 단계는 SQL 파일 작성과 읽기 전용 실행 검증이다. 기존 dbt 모델, PostgreSQL 데이터, Metabase의 기존 5개 차트는 변경하지 않았다. SQL은 Metabase native SQL 질문에 복사해 사용할 수 있는 독립적인 PostgreSQL SELECT다. 이번 작업에서는 질문을 저장하거나 대시보드에 추가하지 않았다.

## 분석 범위와 계산 계약

세 SQL 모두 `analytics.mart_arrival_quality_daily`를 사용하고 `received_date >= DATE '2026-09-30'`으로 필터한다. 9월 30일 당일을 포함하며 상한은 없다. 현재 표본의 최종 날짜는 2026-10-12다. 향후 **dbt Mart가 갱신된 이후** 새 날짜가 자동 포함된다. Airflow가 중지된 현재 상태에서는 실시간 갱신을 보장하지 않는다.

`received_date`는 `received_at::date`로 만든 날짜다. 프로젝트에서는 API 수신 날짜로 불리지만 공식적으로 원래 필드는 **API 도착정보 생성 시각**이다. 한국 현지 시각으로 해석한 API 생성일이며 파이프라인 수집 날짜와 다르다.

- 0초 비율: `SUM(zero_arrival_count) / SUM(arrival_record_count)`.
- 전체 수치 평균: `SUM(arrival_seconds_sum) / SUM(arrival_record_count - null_arrival_count)`.
- 양수값 평균: `SUM(positive_arrival_seconds_sum) / SUM(positive_arrival_count)`.
- 각 분모에 `NULLIF(..., 0)`을 적용한다. 평균과 비율의 단순 평균을 사용하지 않는다.
- 양수 표본이 없으면 양수 평균은 NULL이다. 행을 삭제하거나 0으로 채우지 않는다.
- 평균·비율은 SQL에서 반올림하지 않는다. 시간은 초, 비율은 **0~1**이다.
- 관측 건수는 API 응답 레코드 수다. 반복 관측·미래 시각 이상값을 포함하고 실제 운행 횟수나 승객 대기시간을 뜻하지 않는다.

## 실제 Mart 출력 컬럼 및 타입

2026-10-12 PostgreSQL `information_schema.columns` 조회 결과다. SQL 선언만으로 추측하지 않았다.

| 컬럼 | 실제 PostgreSQL 타입 |
|---|---|
| `received_date` | `date` |
| `station_name` | `varchar(100)` |
| `subway_id` | `varchar(20)` |
| `direction` | `varchar(20)` |
| `arrival_record_count` | `bigint` |
| `zero_arrival_count` | `bigint` |
| `positive_arrival_count` | `bigint` |
| `null_arrival_count` | `bigint` |
| `negative_arrival_count` | `bigint` |
| `zero_arrival_rate` | `numeric`（고정 precision/scale 없음） |
| `avg_arrival_seconds` | `numeric`（고정 precision/scale 없음） |
| `avg_positive_arrival_seconds` | `numeric`（고정 precision/scale 없음） |
| `arrival_seconds_sum` | `bigint` |
| `positive_arrival_seconds_sum` | `bigint` |

PostgreSQL에서 `SUM(bigint)` 결과는 `numeric`이다. 따라서 차트 SQL이 출력하는 합계 건수·합계 시간은 Mart 원래 컬럼과 달리 numeric으로 전달된다. 건수는 정수 형식(소수점 0자리), 평균은 소수점 2자리, 비율은 백분율 2자리로 표시하도록 권장한다. 호선 코드는 문자열 식별자로 유지한다.

## 1. 호선별 API 도착 예정 시간 0초 비율 및 관측 건수

파일: [01_line_zero_arrival_rate.sql](01_line_zero_arrival_rate.sql)

집계는 `subway_id`로만 수행한다. `train_line`은 사용하지 않는다. 호선명은 [서울시 공식 API 명세](https://data.seoul.go.kr/dataList/openApiView.do?infId=OA-12764&srvType=A)의 다음 코드만 매핑했다(2026-10-12 확인).

| 코드 | 호선명 | 코드 | 호선명 |
|---|---|---|---|
| 1001 | 1호선 | 1008 | 8호선 |
| 1002 | 2호선 | 1009 | 9호선 |
| 1003 | 3호선 | 1063 | 경의중앙선 |
| 1004 | 4호선 | 1065 | 공항철도 |
| 1005 | 5호선 | 1075 | 수인분당선 |
| 1006 | 6호선 | 1077 | 신분당선 |
| 1007 | 7호선 | | |

현재 관측된 13개 ID가 모두 이 공식 매핑에 있다. 향후 다른 ID가 들어오면 `LEFT JOIN`으로 행을 유지하고 `미확인 호선 (코드)` 및 `line_mapping_verified=false`를 표시한다. 서울시 명세에 다른 코드가 있더라도 이번 표본에 없는 코드를 추정해 추가하지 않았다.

권장 카드 이름: **호선별 API 도착 예정 시간 0초 비율·관측 건수**.

- 기본 시각화: 콤보 차트. 건수는 막대, 0초 비율은 선 또는 점으로 표시한다.
- X축: `subway_name`, 이름 **호선**. 호선은 범주이므로 점 사이의 선을 시간 추세로 해석하지 않는다.
- 왼쪽 Y축: `arrival_record_count`, 이름 **API 관측 레코드 수(건)**.
- 오른쪽 Y축: `zero_arrival_rate`, 이름 **0초 비율(%)**. 수치 범위 0~1을 Percent 형식으로 표시한다.
- 상세 표: 코드, 전체·0초·양수·NULL·음수 건수, 0초 비율, 매핑 검증 여부를 함께 제공한다. 비율과 건수를 같은 축에 놓거나 전체 건수와 0초 건수를 중복 누적하지 않는다.

이번 결과에서 8호선·경의중앙선·공항철도·수인분당선·신분당선은 100% 0초다. 이것은 해당 수집 역·기간의 표본 특성이며 노선 전체의 보편적 규칙이나 오류율을 의미하지 않는다. 0초를 “도착 완료”로 표시하지 않는다.

## 2. 역별 API 도착 예정 시간 수치 평균 비교와 0초 비율

파일: [02_station_arrival_metrics.sql](02_station_arrival_metrics.sql)

역명으로 날짜·호선·방향을 합친다. `subway_ids`에 실제 관측된 호선 ID 목록을 함께 반환한다. 전체 평균과 양수 평균은 각각 수치 합계와 해당 표본 수로 재집계했다.

권장 카드 이름: **역별 API 도착 예정 시간 수치 평균·0초 비율**.

- 기본 시각화: **표**. 역, 호선 ID 목록, 전체·양수 관측 수, 전체 평균(초), 양수 평균(초), 0초 비율(%)을 함께 표시한다. 18개 역의 세 지표와 서로 다른 분모를 가장 명확히 확인할 수 있다.
- 추가 시각화 선택: 콤보 차트. 두 평균은 나란한 막대, 0초 비율은 오른쪽 축의 선 또는 점으로 표시한다. 평균 막대를 누적하지 않는다.
- X축: `station_name`, 이름 **역**.
- 왼쪽 Y축: `avg_arrival_seconds`, `avg_positive_arrival_seconds`, 이름 **API 도착 예정 시간 수치 평균(초)**. 범례는 **전체 수치 평균(0초 포함)**, **양수값 평균(0초 제외)**.
- 오른쪽 Y축: `zero_arrival_rate`, 이름 **0초 비율(%)**, Percent 형식.
- NULL 양수 평균: 표에서는 값 없는 셀과 설명 **양수 시간 표본 없음**으로 해석한다. SQL에서 문자열로 바꾸지 않고 numeric NULL을 유지한다. 선 차트를 선택할 경우 결측값 대체는 **Nothing**으로 설정하고 0 대체·선형 보간을 사용하지 않는다.

이번 18개 역 결과에는 모두 양수 표본이 있어 양수 평균 NULL인 역은 없다. 하지만 Mart 자체의 양수 표본 없는 그룹은 유지되고, 그 그룹만 입력으로 사용한 같은 역별 SQL에서도 NULL 평균이 유지되는 것을 확인했다.

주의: 강남의 양수 평균은 이번 표본에서 2호선만 반영하고 신분당선은 반영하지 않는다. 잠실도 8호선이 양수 평균에 기여하지 않는다. 역별 수치 차이를 실제 평균 대기시간 또는 열차 운행 성능 순위로 해석하지 않는다. 호선별 상세 비교가 필요하면 별도 질문에서 `station_name, subway_id`를 집계키로 사용해야 하며 현재 역명 합산 결과에 호선 필터를 사후 적용하는 것으로 대체할 수 없다.

## 3. API 생성일별 도착 예정 시간 0초 비율 추이

파일: [03_daily_zero_arrival_trend.sql](03_daily_zero_arrival_trend.sql)

날짜별 모든 역·호선·방향의 0초 건수와 전체 관측 수를 합한 뒤 비율을 계산한다.

권장 카드 이름: **API 생성일별 도착 예정 시간 0초 비율 추이**. 수신 날짜라는 기존 프로젝트 표현을 사용하면 설명에 API 생성일임을 명시한다.

- 기본 시각화: 선 차트. 일별 표본 수를 함께 보려면 콤보 차트로 확장한다.
- X축: `received_date`, 이름 **API 생성일(KST 해석)**, Timeseries/일 단위.
- 비율 Y축: `zero_arrival_rate`, 이름 **0초 비율(%)**, Percent 형식, 원시 수치 0~1 범위.
- 콤보 차트에서는 왼쪽 Y축에 `arrival_record_count` 막대(**API 관측 레코드 수(건)**), 오른쪽 Y축에 비율 선을 배치한다.
- 일별 관측 수와 0초 건수를 상세 표에도 표시한다. 없는 날짜는 이번 SQL에서 생성하지 않으며, 화면에서 표시하더라도 결측을 0%나 보간값으로 채우지 않는다.

9월 30일은 5건, 10월 10일은 69건이다. 다른 날짜와 같은 표본 규모로 해석하면 안 된다. 현재 날짜 마지막 구간도 일 전체를 대표한다고 보장되지 않는다. 호선 구성·수집 공백·API 생성 시각 이상이 비율 추이에 영향을 줄 수 있다.

## Metabase 설정과 기존 5개 차트 유지 제안

위의 세 질문을 별도 **데이터 품질** 대시보드에 추가하는 구성을 우선 권장한다. 기존 대시보드의 5개 차트는 그대로 유지한다. 기존 대시보드에 추가하는 방식을 선택하면 기존 카드 5개를 유지한 채 품질 카드 3개를 별도 영역에 배치한다.

이번 단계의 교체 제안은 없다. 기존 역별 평균 차트와 새 평균 비교 표를 나란히 평가한 뒤 향후 교체 여부를 별도 결정할 수 있다. 기존 5개 카드의 이름·ID·SQL·필터를 이번 작업에서 인증된 Metabase 조회로 확인하지 않았으므로 특정 카드 ID에 대한 교체 지시를 작성하지 않았다.

SQL에는 고정 시작 날짜와 그룹 키만 있다. 아직 Metabase 변수나 대시보드 필터 연결을 추가하지 않았다. 기간·호선·방향 필터가 필요하면 **합산 전 Mart 입력 WHERE**에 적용하도록 별도 계획한다. 특히 역별 SQL에서 방향과 호선은 결과에 개별 차원으로 남아 있지 않으므로, 결과 표를 필터하는 것만으로 해당 분모를 재계산할 수 없다.

차트별로 사용할 숫자 계열만 선택한다. 건수·합계·비율·평균이 모두 반환된다는 이유로 모든 numeric 컬럼을 한 그래프에 그리지 않는다. 비율 0.32906은 화면에서 32.91%이며, SQL에 100을 곱한 뒤 Percent 형식을 다시 적용하지 않는다. Metabase가 제공하는 콤보·좌우 축·결측값 설정은 [공식 시각화 문서](https://www.metabase.com/docs/latest/questions/visualizations/line-bar-and-area-charts)를 확인했다. 실제 설치 버전의 UI와 설정 저장은 이번 작업에서 검증하지 않았다.

## 읽기 전용 SQL 검증 결과

검증일: 2026-10-12. 모든 SQL을 PostgreSQL에 실제 실행했고, 독립적인 원본 직접 재집계와 비교했다. `default_transaction_read_only=on`, `REPEATABLE READ, READ ONLY`를 사용했다. dbt 실행은 하지 않았다.

| 파일 | 반환 행 수 | 관측 합계 | 0초 합계 | 양수 합계 | 원본 직접 재집계 차이 |
|---|---:|---:|---:|---:|---:|
| 호선별 | 13 | 31,982 | 10,524 | 21,458 | 0 |
| 역별 | 18 | 31,982 | 10,524 | 21,458 | 0 |
| 날짜별 | 13 | 31,982 | 10,524 | 21,458 | 0 |

- 원본 전체는 31,987건, 0초 10,527건, 양수 21,460건이며 기존 결과와 일치한다.
- Mart 자체의 모든 키·지표도 원본과 양방향 재집계 차이 0이다.
- 분석 기간 전체 0초 비율은 **32.9060%**, 전체 평균은 **239.2166초**, 양수 평균은 **356.5395초**, 양수 합계는 **7,650,625초**다.
- 0초 비율 범위·범주 건수 합·누락 그룹 없음·양수 표본 부재 시 NULL을 검증했다.
- 원본, 신규 Mart, 기존 Mart 3개의 행 수·내용 체크섬은 작업 전후 동일하며 새 연결에서도 재확인했다. 기존 dbt 모델·테스트 파일 SHA-256도 동일하다.
- 원본 기존 체크섬은 `defec9f4b54d89fcf40ad4752c847920`으로 동일하다. Airflow DAG는 `is_paused=True`이며 Metabase 변경·Git commit·push는 하지 않았다.

증거 파일: [validation_results.json](validation_results.json), 각 SQL과 같은 이름의 CSV 3개. 원본 행 전체나 접속 비밀번호는 결과 파일에 저장하지 않는다. 검증 결과 JSON의 Decimal은 정밀도를 유지하기 위해 문자열로 직렬화했으며 실제 SQL 타입은 numeric이다.

재현:

```powershell
.venv/Scripts/python.exe sql/metabase/verify_queries.py
```

이 스크립트의 31,987·31,982 기대값은 이번 고정 스냅샷 검증용이다. 향후 데이터가 적재되면 기대값을 갱신해야 한다. 차트 SQL에는 건수 기대값이 하드코딩돼 있지 않다.

남은 문제는 0초의 원천 의미 미확정, 노선별 양수 ETA 제공 범위 차이, 미래·반복 생성 시각, 날짜별 표본 수 차이, API 생성 시각과 실제 수집 시각의 분리다. 이 SQL은 이러한 관측을 삭제하거나 보정하지 않고 가시화한다. 실제 Metabase 화면에서의 NULL 표시·축·필터 연결 및 저장은 후속 작업으로 남아 있다.
