import json
from datetime import datetime
from pathlib import Path


RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")


def get_latest_raw_file():
    raw_files = list(RAW_DIR.glob("*.json"))

    if not raw_files:
        raise FileNotFoundError("data/raw 폴더에 JSON 파일이 없습니다.")

    latest_file = max(raw_files, key=lambda file: file.stat().st_mtime)

    return latest_file


def transform_arrival(arrival):
    received_at = datetime.strptime(
        arrival["recptnDt"],
        "%Y-%m-%d %H:%M:%S"
    )

    cleaned_arrival = {
        "subway_id": arrival["subwayId"],
        "station_id": arrival["statnId"],
        "station_name": arrival["statnNm"],
        "direction": arrival["updnLine"],
        "train_line": arrival["trainLineNm"],
        "train_no": arrival["btrainNo"],
        "arrival_seconds": int(arrival["barvlDt"]),
        "arrival_message": arrival["arvlMsg2"],
        "current_station": arrival["arvlMsg3"],
        "received_at": received_at.isoformat(),
    }

    return cleaned_arrival


def transform_file(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    arrivals = data.get("realtimeArrivalList", [])

    cleaned_arrivals = []

    for arrival in arrivals:
        cleaned_arrival = transform_arrival(arrival)
        cleaned_arrivals.append(cleaned_arrival)

    return cleaned_arrivals


def save_processed_data(cleaned_arrivals, raw_file):
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    output_name = f"{raw_file.stem}_cleaned.json"
    output_path = PROCESSED_DIR / output_name

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(
            cleaned_arrivals,
            file,
            ensure_ascii=False,
            indent=2
        )

    return output_path


if __name__ == "__main__":
    latest_raw_file = get_latest_raw_file()

    print(f"사용 Raw 파일: {latest_raw_file}")

    cleaned_arrivals = transform_file(latest_raw_file)

    output_path = save_processed_data(
        cleaned_arrivals,
        latest_raw_file
    )

    print(f"정제 데이터 개수: {len(cleaned_arrivals)}")
    print(f"저장 완료: {output_path}")