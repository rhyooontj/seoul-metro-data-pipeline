# Seoul Metro Data Pipeline

## CI

GitHub Actions의 `.github/workflows/ci.yml`은 PR과 `main` push에서 실행됩니다.
`static_and_unit` job은 변경 범위의 whitespace, Python 문법, mock 기반 unittest와
`docker compose config --quiet`를 검사합니다. DAG는 import하지 않고 compile만 합니다.
`postgres_dbt_integration` job은 run별 고유 컨테이너와 빈 volume에서 PostgreSQL 17을
시작하고, 실제 `000` 및 `001` SQL을 read-only mount하여 공식 entrypoint 초기화를 검증합니다.
TCP healthcheck가 통과한 뒤 컬럼·PK·시퀀스·초기 빈 테이블을 읽기 전용으로 확인합니다.

CI 전용 synthetic fixture는 7행과 5개 mart grain을 포함합니다. 동일 업무 행 2개는
새로운 id를 받아 exact duplicate WARN을 재현합니다. 이후 `dbt parse → dbt run → dbt test`를
실행하고 일반 테스트 PASS와 중복 테스트 WARN을 결과 artifact로 확인합니다.
결과 검사는 테스트 총수를 고정하지 않고 manifest의 활성 테스트와 실행 결과를 대조합니다.
`dbt test` 세션은 read-only이며 실패 행을 DB에 저장하지 않습니다.
성공·실패에 관계없이 이름과 소유 label이 일치하는 CI 컨테이너·volume만 정리합니다.

운영 API, 운영 DB, GitHub 운영 secret과 `.env`는 사용하지 않습니다.
workflow의 DB 자격증명과 API 키 형태의 값은 CI에서만 사용하는 synthetic 값입니다.
fixture는 운영 DB에 실행하면 안 됩니다. 실제 entrypoint 초기화와 dbt run/fixture INSERT는
오직 일회성 CI DB에서만 허용합니다.

로컬에서 저장소 루트와 별도 Python 환경을 사용해 다음 검사를 실행할 수 있습니다.
로컬 `.env`를 읽지 않도록 테스트 프로세스에만 dotenv 비활성화를 적용합니다.

```powershell
python -m pip install -r requirements-ci.txt
git diff --check
python -B -c "import unittest; from unittest.mock import patch; patch('dotenv.load_dotenv', return_value=False).start(); result=unittest.TextTestRunner().run(unittest.defaultTestLoader.discover('tests', pattern='test_*.py')); raise SystemExit(not result.wasSuccessful())"
```

CI의 unittest 명령은 `python -B -m unittest discover -s tests -p "test_*.py"`입니다.
PR의 whitespace 검사는 PR base와 HEAD를 비교하고, push는 이전 commit과 HEAD를 비교합니다.
비교 기준이 없는 최초 push 등에서는 전체 tracked tree를 검사합니다.
로컬의 `git diff --check`는 아직 commit하지 않은 변경을 확인하는 용도입니다.
PostgreSQL/dbt 통합 검증은 별도 임시 DB와 synthetic 환경변수를 준비한 경우에만 실행합니다.
`scripts/ci_verify_schema.py`는 `CI_SCHEMA_VERIFY=1`을 요구하며,
`scripts/ci_verify_dbt_results.py`는 기본적으로 `metro_dbt/target/run_results.json`과
같은 디렉터리의 `manifest.json`을 읽습니다.

## dbt 데이터 품질 검증

Airflow는 기존 `dbt run → dbt test` 순서로 모델 생성과 품질 검증을 수행합니다.
source의 `id`는 `not_null`과 `unique`로 PRIMARY KEY 의미를 검증합니다.
staging은 기존 ID 검증과 함께 역, 노선, 방향, 도착시간, 수신 시각의 핵심 필수값을 검증합니다.
mart는 필수값, `(station_name, direction, train_line)` grain의 유일성,
양수 도착 건수와 `최소 도착시간 ≤ 평균 도착시간 ≤ 최대 도착시간`을 검증합니다.

ID를 제외한 업무 컬럼 전체가 같은 exact duplicate는 `severity='warn'` 테스트로 관측합니다.
이 테스트는 중복 그룹을 반환하며 기존 데이터를 삭제하거나 수정하지 않습니다.
`store_failures`는 사용하지 않고, warning을 error로 승격하지 않는 실행 설정을 유지합니다.
동일 processed 파일 재처리를 막는 배치 멱등성과 서로 다른 배치의 semantic duplicate는
별도 문제이며, 현재 품질 테스트는 후자의 존재를 관측합니다.
freshness, 방향 허용값, 최종 수집량 조건과 deduplication은 별도 개선 과제로 보류합니다.

서울시 지하철 도착정보를 30분마다 수집하고 PostgreSQL에 적재한 뒤,
dbt로 분석 모델을 생성하는 프로젝트입니다. Metabase는 분석 데이터를 조회합니다.

## 신규 환경 실행

Docker와 Docker Compose를 사용할 수 있는 환경에서 저장소 루트를 작업 디렉터리로 사용합니다.
로컬 `.env`에 다음 환경변수를 준비합니다. 실제 인증정보는 Git에 저장하지 않습니다.

- `SEOUL_API_KEY`
- `POSTGRES_DB` (현재 dbt source의 DB 이름인 `metro_db`와 일치해야 함)
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

다음 명령은 **신규 환경을 시작할 때** 사용합니다. 기존 운영 스택에 설정 변경을 반영하는 작업은
별도의 운영 계획에 따라 수행해야 합니다.

```powershell
docker compose config --quiet
docker compose up -d --build
docker compose ps
```

`docker compose config --quiet`는 값이 확장된 설정을 출력하지 않고 구성 유효성을 검사합니다.
서비스 포트는 PostgreSQL 5432, Airflow 8080, Metabase 3000입니다.

## PostgreSQL 최초 초기화

PostgreSQL 17은 `postgres_data` named volume을 `/var/lib/postgresql/data`에 사용합니다.
데이터 디렉터리가 비어 있는 최초 시작에서 PostgreSQL 사용자와 DB를 준비한 다음,
다음 SQL 파일을 `/docker-entrypoint-initdb.d`에서 파일명 순서로 실행합니다.

1. `sql/000_create_subway_arrival.sql`: `public.subway_arrival`과 ID 시퀀스 생성
2. `sql/001_create_pipeline_load_history.sql`: 배치 멱등성 추적 테이블 생성

두 파일만 개별 read-only bind mount합니다. `sql/` 디렉터리 전체를 mount하지 않으므로
향후 추가한 SQL이 의도하지 않게 초기 실행에 포함되지 않습니다.

`000`은 기존 운영 테이블의 컬럼 순서, 타입, varchar 길이, NULL 허용 여부와 PRIMARY KEY를
재현합니다. `id BIGSERIAL`은 bigint 컬럼과 `subway_arrival_id_seq`의 자동 증가 기본값을 만듭니다.
`001`은 `batch_id` PRIMARY KEY와 양수 `row_count` 제약을 만듭니다.
기존 SQL 파일의 `IF NOT EXISTS`는 이미 존재하는 테이블의 구조까지 검증하거나 변경하지 않습니다.

**초기화 SQL은 빈 데이터 volume에서만 실행됩니다. 기존 `postgres_data` volume에는
컨테이너를 다시 시작해도 재실행되지 않습니다.** SQL 파일을 추가하거나 수정하는 것만으로
기존 DB에 migration이 자동 적용되지는 않습니다.

초기화 중 SQL이 실패하면, 이미 데이터 디렉터리가 초기화되어 다음 시작에서 SQL이 자동으로
재시도되지 않을 수 있습니다. 신규 테스트 환경의 실패 원인을 확인하고 별도로 복구해야 합니다.

## PostgreSQL 준비 상태와 Airflow 시작 순서

PostgreSQL healthcheck는 컨테이너 내부에서 다음 TCP 준비 상태 검사를 수행합니다.

```sh
pg_isready -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

Compose 파일에서는 `$${POSTGRES_USER}`와 `$${POSTGRES_DB}`로 작성하여,
Compose가 호스트에서 값을 확장하지 않고 컨테이너의 셸이 환경변수를 사용하게 합니다.
검사 주기는 5초, 제한시간은 3초, 재시도 기준은 10회, 시작 유예시간은 30초입니다.
TCP 검사는 최초 초기화 중 Unix socket으로만 동작하는 임시 서버와 구분됩니다.
`pg_isready`는 연결 수락 준비 상태를 검사하며, 테이블 구조 검증이나 인증 검증을 대신하지 않습니다.

Airflow는 `depends_on.postgres.condition: service_healthy`에 따라 PostgreSQL이 healthy가 된 뒤
시작합니다. 기존 `extract → transform → load → dbt_run → dbt_test` 순서는 유지됩니다.
`analytics` 스키마의 모델은 이후 dbt 실행으로 생성됩니다. Metabase 의존성은 기존 설정을 유지합니다.
이 의존성은 시작 순서를 제어하며, 실행 중 DB 장애에 대한 Airflow 자동 재시작 정책은 아닙니다.

## 기존 운영 DB의 migration

기존 운영 DB는 현재 volume과 데이터를 그대로 유지합니다. 초기화 mount를 추가해도 기존 DB에
`000` 또는 `001`을 자동 재적용하지 않습니다. `load.py`도 CREATE TABLE을 실행하지 않습니다.

운영 DB migration은 다음 절차로 별도 수행합니다.

1. 대상 DB와 실제 schema, 실행 중인 작업, SQL 변경 내용을 읽기 전용으로 확인합니다.
2. migration을 검토하고 적용 승인을 받습니다.
3. 승인된 SQL만 명시적으로 실행하고 테이블·제약조건 및 기존 데이터 불변성을 확인합니다.
4. 신규 동작은 격리 환경에서 검증합니다.

`001_create_pipeline_load_history.sql`은 현재 운영 DB에 적용된 migration입니다.
이 파일을 임의로 재작성하지 않습니다. 향후 schema 변경은 별도 migration 파일로 관리하며,
현재 Compose의 최초 초기화 대상에도 명시적으로 포함할지 검토해야 합니다.

## 데이터 volume 보호

**`postgres_data` volume을 삭제하면 DB 데이터가 사라집니다. 운영 환경에서 임의로 삭제하지 마세요.**
운영 volume을 삭제하는 `docker compose down -v`나 `docker volume rm`을 초기화·migration 방법으로
사용하지 않습니다. 기존 데이터와 load history를 보존해야 합니다.

신규 초기화 검증은 별도 Compose project name, 별도 volume, 충돌하지 않는 컨테이너 이름과
포트 설정으로 수행합니다. 고정 `container_name`과 5432 포트가 있으므로 project name만 변경해서
현재 Compose 전체를 실행하는 것만으로는 격리가 보장되지 않습니다.
