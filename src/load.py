import os
import json
from pathlib import Path

import psycopg
from dotenv import load_dotenv


load_dotenv()

DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")

PROCESSED_DIR = Path("data/processed")

ARRIVAL_COLUMNS = (
    "subway_id", "station_id", "station_name", "direction", "train_line",
    "train_no", "arrival_seconds", "arrival_message", "current_station",
    "received_at",
)

INSERT_ARRIVAL_SQL = """
    INSERT INTO public.subway_arrival (
        subway_id, station_id, station_name, direction, train_line, train_no,
        arrival_seconds, arrival_message, current_station, received_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

RESERVE_BATCH_SQL = """
    INSERT INTO public.pipeline_load_history (batch_id, source_file, row_count)
    VALUES (%s, %s, %s)
    ON CONFLICT (batch_id) DO NOTHING
    RETURNING batch_id
"""


class LoadHistoryUnavailable(RuntimeError):
    """The migration must be applied before using idempotent loading."""


def get_batch_id(file_path):
    """Use the basename, independent of directory and modification time."""
    return Path(file_path).name


def get_latest_processed_file():
    processed_files = list(PROCESSED_DIR.glob("*_cleaned.json"))

    if not processed_files:
        raise FileNotFoundError("data/processed 폴더에 정제된 JSON 파일이 없습니다.")

    latest_file = max(
        processed_files,
        key=lambda file: file.stat().st_mtime
    )

    return latest_file


def load_data(file_path, *, allow_legacy=False):
    """Load atomically; the CLI permits legacy loading until schema deployment.

    Once the history table exists, its PK arbitrates concurrent batch claims.
    A history row is inserted first but becomes visible only when all arrival
    inserts commit. Failed transactions roll back both the claim and arrivals.
    Previously loaded files are not automatically added to history.
    """
    file_path = Path(file_path)
    with open(file_path, "r", encoding="utf-8") as file:
        arrivals = json.load(file)

    # transform.py writes a top-level list, not an API response envelope.
    if not isinstance(arrivals, list) or not arrivals:
        raise ValueError("정제 데이터는 비어 있지 않은 list여야 합니다.")
    if any(
        not isinstance(arrival, dict)
        or any(column not in arrival for column in ARRIVAL_COLUMNS)
        for arrival in arrivals
    ):
        raise ValueError("정제 데이터의 필수 컬럼이 없거나 행 형식이 잘못되었습니다.")

    batch_id = get_batch_id(file_path)
    conn = psycopg.connect(
        host=DB_HOST,
        port=5432,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.pipeline_load_history')")
            history_exists = cur.fetchone()[0] is not None
            if not history_exists:
                if not allow_legacy:
                    raise LoadHistoryUnavailable(
                        "pipeline_load_history가 없습니다. SQL migration 적용이 필요합니다."
                    )
                print("경고: load history 미적용. 기존 방식으로 적재하며 멱등성은 비활성입니다.")
            else:
                # PostgreSQL waits for a concurrent conflicting transaction.
                # After its commit we skip; after its rollback we can claim.
                cur.execute(RESERVE_BATCH_SQL, (batch_id, file_path.name, len(arrivals)))
                if cur.fetchone() is None:
                    conn.commit()
                    print(f"이미 처리된 배치이므로 skip: {batch_id}")
                    return 0

            for arrival in arrivals:
                cur.execute(
                    INSERT_ARRIVAL_SQL,
                    tuple(arrival[column] for column in ARRIVAL_COLUMNS),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return len(arrivals)


def main():
    try:
        latest_file = get_latest_processed_file()
        print(f"사용 Processed 파일: {latest_file}")
        # Compatibility during the approved migration's pending deployment.
        # Once the table exists, duplicate protection activates automatically.
        inserted_count = load_data(latest_file, allow_legacy=True)
    except psycopg.Error:
        # Driver errors may contain connection details: never print the error.
        print("PostgreSQL 적재 실패: DB 트랜잭션 또는 연결 오류입니다.")
        return 1
    except (ValueError, OSError):
        print("PostgreSQL 적재 실패: 정제 파일이 없거나 유효한 데이터가 없습니다.")
        return 1

    if inserted_count:
        print(f"PostgreSQL 저장 완료: {inserted_count}건")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
