서울 지하철 실시간 도착정보 데이터 파이프라인 및 BI 대시보드
서울시 지하철 실시간 도착정보 Open API를 활용해 데이터를 수집하고, PostgreSQL 적재, dbt 데이터 마트 구축, Apache Airflow 작업 자동화, Metabase 시각화까지 연결한 개인 데이터 엔지니어링 프로젝트입니다.
단순히 데이터를 저장하는 데서 끝내지 않고, 배치 중복 적재 방지, 데이터 정합성 검사, 원본 품질 진단, GitHub Actions 기반 자동 테스트를 구현했습니다.
> **프로젝트 검증 기준:** 2026년 10월 15일 / 누적 원본 31,987건 / 18개 역 / dbt 모델 5개 / dbt 테스트 45개 / Metabase 대시보드 2개
>
> Airflow에는 30분 주기 스케줄이 설정되어 있으나, 현재 추가 수집을 중단하기 위해 DAG를 일시 중지한 상태입니다. 따라서 실시간 갱신 중인 공개 서비스는 아닙니다.
---
1. 프로젝트 개요
서울시 Open API의 지하철 도착정보를 주기적으로 수집하고, 여러 역에서 발생한 관측 데이터를 분석 가능한 구조로 만드는 것을 목표로 했습니다.
주요 구현 범위는 다음과 같습니다.
Python으로 18개 역의 API 응답 수집 및 JSON 저장
원본 필드 변환과 PostgreSQL 적재
Airflow DAG를 통한 `Extract → Transform → Load → dbt run → dbt test` 작업 연결
dbt 기반 Staging 및 역별, 시간별, 일별, 품질 분석 Mart 구성
SQL 검증 및 Metabase 대시보드 제작
GitHub Actions로 Python, PostgreSQL, dbt 통합 검증 자동화
핵심 결과
항목	검증 결과
수집 대상 역	18개
PostgreSQL 원본 관측 레코드	31,987건
적재 이력에 기록된 배치	149개
dbt 분석 모델	5개 모두 성공
dbt 데이터 품질 테스트	44 PASS, 1 WARN, 0 ERROR
Python 단위 테스트	42개 통과
Metabase 대시보드	2개, 총 8개 차트
위 수치는 검증 시점의 스냅샷이며, 운영 기간 전체에 걸친 무중단 수집이나 모든 오류의 부재를 뜻하지 않습니다.
---
2. 데이터 수집 대상
데이터 출처
출처: 서울시 지하철 실시간 도착정보 Open API
API: `realtimeStationArrival`
수집 단위: 역별 API 요청 결과의 도착정보 레코드
수집 대상: 18개 역
수집 스케줄: 30분 간격으로 설정
대상 역
```text
강남, 잠실, 홍대입구, 신도림, 사당, 건대입구,
고속터미널, 교대, 시청, 을지로입구, 종로3가,
동대문역사문화공원, 왕십리, 합정, 신촌, 여의도,
공덕, 선릉
```
`src/extract.py`는 역별 응답을 수집하면서 성공 및 실패한 역을 구분하여 기록합니다. 요청 실패 시 오류 유형을 남기되, API 키가 포함될 수 있는 전체 URL은 로그에 출력하지 않도록 처리했습니다.
---
3. 데이터 구성
Raw JSON
API 원본 응답의 도착정보 목록과 수집 배치 메타데이터를 JSON 파일로 저장했습니다.
```text
data/raw/subway_batch_YYYYMMDD_HHMMSS.json
```
주요 항목은 `collected_at`, `success_station_count`, `failed_station_count`, `success_stations`, `failed_stations`, `realtimeArrivalList`입니다.
Processed JSON
API 응답을 분석과 DB 적재에 사용할 필드로 변환했습니다.
```text
data/processed/subway_batch_YYYYMMDD_HHMMSS_cleaned.json
```
컬럼	설명
`subway_id`	호선 식별 코드
`station_id`	역 식별 코드
`station_name`	역명
`direction`	상행 및 하행 등 방향
`train_line`	API가 제공하는 열차 행선지 정보
`train_no`	열차 번호
`arrival_seconds`	API가 제공한 도착 예정 시간 수치, 초
`arrival_message`	도착 안내 메시지
`current_station`	현재 열차 위치 관련 메시지
`received_at`	API의 `recptnDt`를 변환한 시각
시각 구분: `collected_at`은 수집 프로그램이 기록한 배치 시각이며, `received_at`은 API의 원본 시각입니다. 두 값을 동일한 실제 수집 시각으로 취급하지 않았습니다.
---
4. 데이터 전처리 및 PostgreSQL 적재
Extract
`src/extract.py`에서 18개 역을 순회하며 API에 요청하고 Raw JSON을 생성했습니다. 응답 구조와 상태값을 검증하며, 일시적인 요청 오류에 대해서는 제한된 재시도를 수행합니다.
Transform
`src/transform.py`에서 API 필드명을 정리하고 `barvlDt`를 정수형 도착 예정 시간으로, `recptnDt`를 날짜 및 시간 형식으로 변환했습니다. 수집 파일과 정제 파일은 분리 보관합니다.
Load
`src/load.py`에서 정제 JSON을 `public.subway_arrival`에 적재합니다. `public.pipeline_load_history`의 배치 식별자 기본키를 이용해 같은 정제 파일을 다시 처리할 때 중복 적재하지 않도록 구현했습니다.
```text
Processed JSON
     |
     v
배치 ID 확인
     |
     +-- 이미 처리된 배치 --> 적재 건수 0, 종료
     |
     +-- 신규 배치 --> 도착정보 INSERT
                           |
                           v
                  적재 이력과 함께 COMMIT
```
실제 검증에서는 이미 적재된 88건짜리 배치를 재실행했을 때 신규 적재 0건을 확인했습니다. 이후 원본 31,987건, 이력 149개가 모두 유지됐습니다.
이 방식은 같은 배치 파일의 재실행 방지이며, 서로 다른 배치에서 동일한 API 레코드가 반복 관측되는 현상까지 제거하지는 않습니다.
---
5. dbt 데이터 모델링
PostgreSQL 원본 데이터를 dbt Staging과 Mart로 분리해 분석 목적에 맞게 집계했습니다.
모델	구분	집계 기준	검증 당시 행 수
`stg_subway_arrival`	View	원본 도착정보 정리	31,987
`mart_station_arrival_summary`	Table	역, 방향, 행선지	398
`mart_station_daily_summary`	Table	API 날짜, 역, 방향, 행선지	2,745
`mart_station_hourly_summary`	Table	API 날짜와 시간, 역, 방향	8,539
`mart_arrival_quality_daily`	Table	API 날짜, 역, 호선 ID, 방향	877
주요 집계값에는 관측 건수, 도착 예정 시간 수치의 평균 및 최솟값과 최댓값, 최근 API 시각이 포함됩니다.
품질 Mart에는 전체 관측 건수와 `0초`, 양수, 음수, NULL 건수, 0초 비율, 0초 포함 평균, 양수값만의 조건부 평균을 별도로 계산했습니다.
집계 정합성: 역별, 일별, 시간별 Mart의 관측 건수 합계가 각각 원본 31,987건과 일치했고, 원본 직접 재집계와 비교한 집계값 차이는 0건이었습니다.
---
6. 데이터 품질 분석 결과
이번 프로젝트에서 집중적으로 확인한 품질 이슈는 도착 예정 시간 0초 값, 반복된 API 레코드, 원본 시각의 이상값입니다.
도착 예정 시간 0초
2026년 9월 30일 이후 API 시각을 기준으로 한 분석 결과는 다음과 같습니다.
지표	결과
분석 대상 관측 레코드	31,982건
`arrival_seconds = 0`	10,524건
0초 비율	약 32.91%
양수값 관측 레코드	21,458건
0초 포함 전체 수치 평균	약 239.22초
양수값만의 수치 평균	약 356.54초
0초가 전체 관측의 약 3분의 1을 차지했고, 일부 호선에서는 표본 전체가 0초였습니다. 이에 따라 전체 평균과 양수값 평균을 분리했습니다.
다만 `0초`가 실제 도착을 의미하는지, 공급 API에서 시간을 제공하지 않는 상태인지는 확정할 수 없습니다. 따라서 0초를 오류나 결측으로 일괄 처리하지 않았고, 두 평균 모두 실제 승객 대기시간이라고 설명하지 않았습니다.
동일한 업무 레코드의 반복 관측
ID를 제외한 주요 업무 필드가 완전히 같은 레코드가 8개 그룹 확인됐습니다. 관련 레코드는 48건, 각 그룹의 첫 관측을 제외한 수는 40건입니다.
서로 다른 Raw 배치에서 같은 API 값이 반복된 증거가 확인되어, 이를 동일 배치 재적재 오류로 단정하거나 삭제하지 않았습니다. 해당 항목은 dbt에서 WARN으로 관리합니다.
API 시각 이상값
2026년 10월 12일 진단 당시 수집 시각보다 미래에 해당했던 API 시각 값 12건을 확인했습니다. 원본 Raw JSON에도 해당 시각이 존재해, ETL 변환 과정에서 새로 생성된 값으로 판단하지 않았습니다.
이는 당시 진단 결과이며 현재 시각 기준으로도 미래값이라는 뜻은 아닙니다. 원본을 수정하지 않고 품질 분석 문서에 근거와 영향 범위를 기록했습니다.
상세 검증 자료: 데이터 품질 진단 및 관리 기준
---
7. 데이터 파이프라인
전체 흐름은 다음과 같습니다.
```text
서울시 지하철 실시간 도착정보 API
                |
                v
       Python Extract
       (18개 역 요청)
                |
                v
          Raw JSON
         data/raw/
                |
                v
       Python Transform
                |
                v
        Processed JSON
       data/processed/
                |
                v
       Python Load + 적재 이력
                |
                v
       PostgreSQL 17
       public.subway_arrival
                |
                v
       dbt Staging View
                |
                v
       dbt Mart Tables
                |
                +------------------+
                |                  |
                v                  v
        dbt Quality Tests      Metabase
                                   |
                                   v
                            BI Dashboards

Airflow: Extract → Transform → Load → dbt run → dbt test
GitHub Actions: Python 테스트 + 임시 PostgreSQL/dbt 통합 테스트
```
Airflow가 파이프라인 작업의 선후 관계를 관리하고, GitHub Actions가 소스 코드 변경 시 별도의 검증 환경을 구성하는 역할을 맡습니다.
---
8. Airflow 자동화 및 Docker 운영
Airflow DAG
`dags/subway_pipeline.py`는 다음과 같이 구성했습니다.
설정	내용
DAG ID	`seoul_subway_pipeline`
스케줄	`*/30 * * * *`, 30분 주기
`catchup`	`False`
`max_active_runs`	`1`
`extract` 실패 재시도	추가 1회, 5분 후
작업 순서	Extract → Transform → Load → dbt run → dbt test
동시 DAG 실행을 하나로 제한해 dbt 테이블 갱신 충돌 가능성을 줄이고, 수집 작업에는 제한적인 재시도를 설정했습니다.
영구 저장
PostgreSQL: Docker named volume `postgres_data`
Airflow 메타데이터 및 설정: 외부 볼륨 `seoul-metro-airflow-state`
Airflow 작업 로그: 외부 볼륨 `seoul-metro-airflow-logs`
Metabase 애플리케이션 DB: 외부 볼륨 `seoul-metro-metabase-data`
기존 운영 환경에서 Airflow DAG 실행 기록 448건, 작업 로그 2,215개, Metabase 대시보드 2개가 컨테이너 재생성 및 연결 후 보존되는 것을 확인했습니다.
과거 Airflow 시작 시 내부 API 서버 준비 전 작업 실행으로 실패한 사례도 분석했습니다. 다만 해당 시작 순서 문제가 모든 상황에서 완전히 해소됐다고 검증한 것은 아니므로 개발 환경의 운영상 주의사항으로 관리합니다.
---
9. 데이터 검증
데이터 적재 및 모델 생성 과정에서 다음 내용을 검증했습니다.
원본 테이블 기본키 및 핵심 필드의 NULL 여부
Staging과 원본의 레코드 건수 일치 여부
Mart 집계키 중복 여부
원본과 역별, 일별, 시간별 Mart의 건수 및 집계값 일치 여부
0초, 양수, 음수, NULL 건수의 합계 검증
양수값 평균 및 0초 비율 계산 검증
동일 파일 재실행 시 중복 적재 방지 여부
완전 중복 업무 레코드의 별도 경고 관리
실제 dbt 실행 결과
```text
dbt run
PASS=5 WARN=0 ERROR=0 TOTAL=5

dbt test
PASS=44 WARN=1 ERROR=0 TOTAL=45
```
WARN 1건은 기존 원본에 존재하는 완전 중복 업무 레코드 8개 그룹을 감지한 결과입니다. 이를 숨기거나 성공으로 치환하지 않았습니다.
원본이 수정되지 않았는지도 전체 행 수와 체크섬을 비교해 별도로 확인했습니다.
---
10. Metabase BI 대시보드
PostgreSQL의 dbt Mart를 Metabase와 연결해 메인 분석 대시보드 1개와 데이터 품질 대시보드 1개, 총 8개 차트를 구성했습니다.
메인 분석 대시보드
기존 dbt Mart를 이용해 역별 및 시간대별 도착정보 집계 결과를 확인할 수 있도록 5개 차트를 구성했습니다.
데이터 품질 대시보드
품질 Mart를 기반으로 3개 차트를 구성했습니다.
호선별 0초 비율 및 관측 건수
역별 도착 예정 시간 수치 평균 비교와 0초 비율
API 생성일별 0초 비율 추이
품질 차트에서는 비율을 `0~1`의 원시값으로 계산한 뒤 화면에서 백분율로 표시하고, 여러 집계 그룹의 비율을 단순 평균하지 않았습니다.
대시보드는 로컬 Metabase 환경에서 확인했습니다. 외부에 공개된 실시간 서비스 주소는 없습니다.
시각화에 사용한 SQL과 검증 기록: Metabase SQL
---
11. GitHub Actions CI
`.github/workflows/ci.yml`은 Pull Request와 `main` 브랜치 Push에서 실행됩니다.
Python 및 설정 검증
Git 변경 범위의 공백 오류 검사
Python 문법 검사
Mock 기반 단위 테스트
`.env` 파일과 일부 비밀정보 경계 검사
Docker Compose 구성 검사
PostgreSQL 및 dbt 통합 검증
CI 실행마다 독립적인 PostgreSQL 17 컨테이너와 볼륨 생성
실제 테이블 초기화 SQL의 실행 순서 및 스키마 검증
CI 전용 합성 데이터 7건 적재
`dbt parse → dbt run → dbt test` 실행
의도된 중복 데이터 WARN을 포함한 테스트 결과 검증
CI 종료 시 해당 실행이 생성한 컨테이너 및 볼륨만 정리
운영 API 키, 실제 PostgreSQL 데이터, 로컬 `.env`는 CI에서 사용하지 않습니다. 합성 데이터와 테스트용 자격증명만 사용해 운영 데이터와 검증 환경을 분리했습니다.
GitHub Actions 실행 기록
---
12. 사용 기술
Data Engineering
Python
SQL
Apache Airflow 3.3.2
dbt Core 및 dbt-postgres
Database
PostgreSQL 17
psycopg
BI
Metabase
Infrastructure 및 Testing
Docker 및 Docker Compose
Git 및 GitHub
GitHub Actions
Python unittest
Visual Studio Code
---
13. 프로젝트 구조
```text
seoul-metro-data-pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml
├── dags/
│   └── subway_pipeline.py
├── src/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── metro_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_subway_arrival.sql
│   │   └── marts/
│   │       ├── mart_station_arrival_summary.sql
│   │       ├── mart_station_daily_summary.sql
│   │       ├── mart_station_hourly_summary.sql
│   │       ├── mart_arrival_quality_daily.sql
│   │       ├── schema.yml
│   │       └── arrival_quality_schema.yml
│   └── tests/
│       └── [데이터 품질 검증 SQL]
├── sql/
│   ├── 000_create_subway_arrival.sql
│   ├── 001_create_pipeline_load_history.sql
│   └── metabase/
│       ├── 01_line_zero_arrival_rate.sql
│       ├── 02_station_arrival_metrics.sql
│       ├── 03_daily_zero_arrival_trend.sql
│       └── README.md
├── docs/
│   ├── data_quality.md
│   ├── diagnose_zero_arrivals.py
│   ├── verify_arrival_quality_mart.py
│   └── zero_arrival_analysis/
│       └── [분석 및 검증 결과 파일]
├── scripts/
│   ├── ci_verify_schema.py
│   └── ci_verify_dbt_results.py
├── tests/
│   └── [Python 자동 테스트]
├── Dockerfile.airflow
├── docker-compose.yml
├── requirements-ci.txt
├── .gitignore
└── README.md
```
`data/raw/`, `data/processed/`, `.env` 및 로컬 Docker 볼륨의 실제 데이터는 GitHub 저장소에 포함하지 않습니다.
---
14. 실행 및 검증 방법
환경변수
저장소 루트에 Git으로 추적하지 않는 `.env` 파일을 준비합니다.
```dotenv
SEOUL_API_KEY=<YOUR_SEOUL_API_KEY>
POSTGRES_DB=metro_db
POSTGRES_USER=<YOUR_POSTGRES_USER>
POSTGRES_PASSWORD=<YOUR_POSTGRES_PASSWORD>
```
신규 로컬 환경에서 Docker 준비
현재 `docker-compose.yml`은 Airflow와 Metabase의 외부 볼륨이 미리 생성되어 있다고 가정합니다. 신규 환경에서는 먼저 다음 볼륨을 만듭니다.
```cmd
docker volume create seoul-metro-airflow-state
docker volume create seoul-metro-airflow-logs
docker volume create seoul-metro-metabase-data
```
그다음 Compose 설정을 검사하고 실행합니다.
```cmd
docker compose config --quiet
docker compose up -d --build
docker compose ps
```
Airflow: `http://localhost:8080`
Metabase: `http://localhost:3000`
PostgreSQL: `localhost:5432`
새 볼륨은 비어 있으므로 기존 31,987건, 대시보드 또는 실행 기록이 자동 복원되는 것은 아닙니다. 이 수치들은 본 프로젝트에서 검증한 별도의 로컬 환경 결과입니다.
실제 API 수집은 키와 DAG 상태를 확인한 뒤 의도적으로 실행해야 합니다. 현재 검증된 로컬 운영 환경에서는 DAG를 일시 중지해 추가 수집을 멈춘 상태입니다.
테스트
```cmd
python -m pip install -r requirements-ci.txt
python -B -m unittest discover -s tests -p "test_*.py"
```
실제 PostgreSQL에 대한 dbt 실행은 DB 연결 대상과 기존 데이터를 확인한 후 수행합니다. GitHub Actions는 별도의 임시 PostgreSQL에서 통합 테스트를 실행하도록 구성되어 있습니다.
---
15. 데이터 사용 시 유의사항
이 데이터는 API 응답 관측 레코드이며 고유 열차 도착 사건이나 실제 운행 횟수와 같지 않습니다.
API의 `recptnDt`는 파이프라인 수집 시각과 구분해야 합니다. 원본 문자열에 시간대가 명시되어 있지 않아 프로젝트에서는 KST로 해석했습니다.
30분 간격 수집은 설정된 스케줄입니다. PC가 꺼져 있거나 API 요청이 실패한 구간까지 연속 수집됐다고 주장하지 않습니다.
`arrival_seconds = 0`은 실제 도착, 오류, 결측 중 하나로 확정하지 않았습니다.
서로 다른 배치에 동일 API 레코드가 반복될 수 있습니다. 배치 단위 멱등성과 원본 값의 반복 관측은 구분해서 관리합니다.
CI 전용 합성 데이터는 실제 지하철 관측 자료가 아닙니다.
Airflow `standalone` 및 Metabase 내장 애플리케이션 DB를 사용한 로컬 실습 환경입니다. 별도의 운영 보안 구성과 고가용성을 갖춘 상용 서비스로 소개하지 않습니다.
Docker 볼륨은 원본 데이터와 대시보드를 포함할 수 있으므로 `docker compose down -v` 등 볼륨 삭제 명령을 데이터 초기화 수단으로 사용하지 않습니다.
---
16. 프로젝트 핵심
이번 프로젝트에서는 API 데이터를 단순히 수집하는 데서 끝내지 않고 다음 과정을 하나의 흐름으로 구축했습니다.
```text
18개 역 API 수집
→ Raw JSON 보존
→ 필드 변환 및 정제
→ PostgreSQL 배치 적재
→ 배치 재실행 중복 방지
→ dbt Staging 및 Mart 생성
→ 데이터 정합성 및 품질 검증
→ Metabase 대시보드
→ GitHub Actions 자동 테스트
```
프로젝트를 진행하면서 특히 저장된 데이터가 원본과 일치하는지, 동일 배치를 다시 실행해도 데이터가 중복되지 않는지, 이상값을 임의로 제거하지 않고 설명할 수 있는지를 중점적으로 검증했습니다.
이를 통해 수집, 저장, 모델링, 품질 검사, 시각화, 테스트가 연결된 데이터 엔지니어링 파이프라인을 구현했습니다.