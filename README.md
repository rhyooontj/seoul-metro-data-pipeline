# Seoul Metro Data Pipeline

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
