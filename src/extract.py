import os
import json
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv


load_dotenv()

API_KEY = os.getenv("SEOUL_API_KEY")

RAW_DIR = Path("data/raw")

STATIONS = [
    "강남",
    "잠실",
    "홍대입구",
    "신도림",
    "사당",
    "건대입구",
    "고속터미널",
    "교대",
    "시청",
    "을지로입구",
    "종로3가",
    "동대문역사문화공원",
    "왕십리",
    "합정",
    "신촌",
    "여의도",
    "공덕",
    "선릉",
]


def get_station_arrival(station_name: str):
    url = (
        f"http://swopenapi.seoul.go.kr/api/subway/"
        f"{API_KEY}/json/realtimeStationArrival/0/5/{station_name}"
    )

    response = requests.get(url, timeout=10)
    response.raise_for_status()

    data = response.json()

    return data.get("realtimeArrivalList", [])


def collect_all_stations():
    all_arrivals = []
    success_stations = []
    failed_stations = []

    for station_name in STATIONS:
        try:
            arrivals = get_station_arrival(station_name)

            all_arrivals.extend(arrivals)
            success_stations.append(station_name)

            print(f"{station_name}: {len(arrivals)}건")

        except requests.RequestException as error:
            failed_stations.append(station_name)
            print(f"{station_name}: 요청 실패 - {error}")

        time.sleep(0.2)

    return all_arrivals, success_stations, failed_stations


def save_raw_data(all_arrivals, success_stations, failed_stations):
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    data = {
        "collected_at": now.isoformat(),
        "success_station_count": len(success_stations),
        "failed_station_count": len(failed_stations),
        "success_stations": success_stations,
        "failed_stations": failed_stations,
        "realtimeArrivalList": all_arrivals,
    }

    file_path = RAW_DIR / f"subway_batch_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)

    return file_path


if __name__ == "__main__":
    arrivals, success_stations, failed_stations = collect_all_stations()

    file_path = save_raw_data(
        arrivals,
        success_stations,
        failed_stations,
    )

    print()
    print(f"수집 성공 역 수: {len(success_stations)}")
    print(f"수집 실패 역 수: {len(failed_stations)}")
    print(f"전체 도착정보: {len(arrivals)}건")
    print(f"Raw 저장 완료: {file_path}")