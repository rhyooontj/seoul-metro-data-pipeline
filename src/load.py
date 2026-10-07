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


def get_latest_processed_file():
    processed_files = list(PROCESSED_DIR.glob("*_cleaned.json"))

    if not processed_files:
        raise FileNotFoundError("data/processed 폴더에 정제된 JSON 파일이 없습니다.")

    latest_file = max(
        processed_files,
        key=lambda file: file.stat().st_mtime
    )

    return latest_file


def load_data(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        arrivals = json.load(file)

    with psycopg.connect(
        host=DB_HOST,
        port=5432,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    ) as conn:

        with conn.cursor() as cur:

            for arrival in arrivals:
                cur.execute(
                    """
                    INSERT INTO subway_arrival (
                        subway_id,
                        station_id,
                        station_name,
                        direction,
                        train_line,
                        train_no,
                        arrival_seconds,
                        arrival_message,
                        current_station,
                        received_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        arrival["subway_id"],
                        arrival["station_id"],
                        arrival["station_name"],
                        arrival["direction"],
                        arrival["train_line"],
                        arrival["train_no"],
                        arrival["arrival_seconds"],
                        arrival["arrival_message"],
                        arrival["current_station"],
                        arrival["received_at"],
                    ),
                )

    return len(arrivals)


if __name__ == "__main__":
    latest_file = get_latest_processed_file()

    print(f"사용 Processed 파일: {latest_file}")

    inserted_count = load_data(latest_file)

    print(f"PostgreSQL 저장 완료: {inserted_count}건")