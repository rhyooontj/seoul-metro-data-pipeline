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


class StationRequestError(Exception):
    """A safe-to-log request failure, without URLs or API response messages."""

    def __init__(self, reason, retryable=True):
        super().__init__(reason)
        self.retryable = retryable


def validate_response(data):
    if not isinstance(data, dict):
        raise StationRequestError("응답이 JSON 객체가 아닙니다.")

    result = data.get("errorMessage", data.get("RESULT"))
    if not isinstance(result, dict):
        raise StationRequestError("API 상태 정보가 없습니다.")

    code = result.get("code", result.get("CODE"))

    if code not in ("INFO-000", "INFO-200"):
        permanent_codes = {
            "INFO-100",
            "ERROR-300",
            "ERROR-301",
            "ERROR-310",
            "ERROR-331",
            "ERROR-332",
            "ERROR-333",
            "ERROR-334",
            "ERROR-335",
            "ERROR-336",
        }

        raise StationRequestError(
            "API 내부 오류 또는 알 수 없는 상태입니다.",
            retryable=(
                code not in permanent_codes
                if isinstance(code, str)
                else True
            ),
        )

    if "status" in result and result["status"] != 200:
        raise StationRequestError(
            "API 내부 HTTP 상태가 정상이 아닙니다."
        )

    if code == "INFO-200" and "realtimeArrivalList" not in data:
        return []

    arrivals = data.get("realtimeArrivalList")

    if not isinstance(arrivals, list):
        raise StationRequestError(
            "도착정보 목록이 없거나 list 타입이 아닙니다."
        )

    if code == "INFO-200":
        if arrivals:
            raise StationRequestError(
                "데이터 없음 상태와 도착정보가 일치하지 않습니다."
            )
        return []

    return arrivals


def get_station_arrival(station_name: str):
    url = (
        f"http://swopenapi.seoul.go.kr/api/subway/"
        f"{API_KEY}/json/realtimeStationArrival/0/5/{station_name}"
    )

    for attempt in range(1, 4):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            try:
                data = response.json()
            except ValueError:
                raise StationRequestError(
                    "JSON 파싱 실패"
                ) from None

            return validate_response(data)

        except requests.RequestException as error:
            # 실제 오류 유형을 기록하되 API 키가 포함된 URL은 출력하지 않음
            status = getattr(
                getattr(error, "response", None),
                "status_code",
                None,
            )

            error_type = type(error).__name__

            failure_message = (
                f"{error_type} (HTTP {status})"
                if status is not None
                else error_type
            )

            failure = StationRequestError(
                failure_message,
                retryable=status not in (400, 401, 403, 404),
            )

        except StationRequestError as error:
            failure = error

        print(
            f"{station_name}: 요청 실패 "
            f"(시도 {attempt}/3) - {failure}"
        )

        if attempt == 3 or not failure.retryable:
            raise failure from None

        time.sleep(attempt)


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

        except StationRequestError:
            failed_stations.append(station_name)
            print(f"{station_name}: 최종 요청 실패")

        time.sleep(0.2)

    return all_arrivals, success_stations, failed_stations


def save_raw_data(
    all_arrivals,
    success_stations,
    failed_stations,
):
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
        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=2,
        )

    return file_path


def main():
    arrivals, success_stations, failed_stations = (
        collect_all_stations()
    )

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

    if not success_stations or not arrivals:
        print(
            "수집 실패: 성공한 역이 없거나 "
            "전체 도착정보가 0건입니다."
        )
        return 1

    if failed_stations:
        print(
            "일부 역 수집 실패: 정상 수집 데이터로 "
            "후속 처리를 진행합니다."
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())