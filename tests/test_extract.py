import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import Mock, call, mock_open, patch

import requests

from src import extract


def response_with(data):
    response = Mock()
    response.json.return_value = data
    return response


def normal_response(arrivals):
    return response_with({
        "errorMessage": {"code": "INFO-000", "status": 200},
        "realtimeArrivalList": arrivals,
    })


class ExtractTests(unittest.TestCase):
    def setUp(self):
        self.output = io.StringIO()
        self.enterContext(redirect_stdout(self.output))
        self.sleep = self.enterContext(patch.object(extract.time, "sleep"))
        self.get = self.enterContext(patch.object(extract.requests, "get"))
        self.enterContext(patch.object(extract, "API_KEY", "test-only-key"))

    def test_normal_response(self):
        arrivals = [{"statnNm": "강남"}]
        self.get.return_value = normal_response(arrivals)
        self.assertEqual(extract.get_station_arrival("강남"), arrivals)
        self.get.assert_called_once()
        self.get.return_value.raise_for_status.assert_called_once()
        self.sleep.assert_not_called()

    def test_timeout_then_success(self):
        arrivals = [{"statnNm": "강남"}]
        self.get.side_effect = [requests.Timeout("private URL"), normal_response(arrivals)]
        self.assertEqual(extract.get_station_arrival("강남"), arrivals)
        self.assertEqual(self.get.call_count, 2)
        self.sleep.assert_called_once_with(1)

    def test_three_request_failures(self):
        self.get.side_effect = requests.ConnectionError("private URL")
        with self.assertRaises(extract.StationRequestError):
            extract.get_station_arrival("강남")
        self.assertEqual(self.get.call_count, 3)
        self.assertEqual(self.sleep.call_args_list, [call(1), call(2)])

    def test_http_200_internal_error(self):
        self.get.return_value = response_with({
            "errorMessage": {"code": "ERROR-500", "status": 200},
            "realtimeArrivalList": [{"statnNm": "강남"}],
        })
        with self.assertRaises(extract.StationRequestError):
            extract.get_station_arrival("강남")
        self.assertEqual(self.get.call_count, 3)

    def test_authentication_error_is_not_retried(self):
        self.get.return_value = response_with({"errorMessage": {"code": "INFO-100"}})
        with self.assertRaises(extract.StationRequestError):
            extract.get_station_arrival("강남")
        self.get.assert_called_once()
        self.sleep.assert_not_called()

    def test_transient_http_error_is_retried(self):
        response = normal_response([])
        response.status_code = 503
        response.raise_for_status.side_effect = requests.HTTPError(response=response)
        self.get.return_value = response
        with self.assertRaises(extract.StationRequestError):
            extract.get_station_arrival("강남")
        self.assertEqual(self.get.call_count, 3)

    def test_http_authentication_error_is_not_retried(self):
        response = normal_response([])
        response.status_code = 401
        response.raise_for_status.side_effect = requests.HTTPError(response=response)
        self.get.return_value = response
        with self.assertRaises(extract.StationRequestError):
            extract.get_station_arrival("강남")
        self.get.assert_called_once()

    def test_json_parsing_error_is_retried(self):
        response = Mock()
        response.json.side_effect = ValueError("private response body")
        self.get.return_value = response
        with self.assertRaises(extract.StationRequestError):
            extract.get_station_arrival("강남")
        self.assertEqual(self.get.call_count, 3)

    def test_invalid_list_types(self):
        for arrivals in (None, {}, "invalid", 7):
            with self.subTest(arrivals=arrivals):
                self.get.reset_mock()
                self.get.return_value = normal_response(arrivals)
                with self.assertRaises(extract.StationRequestError):
                    extract.get_station_arrival("강남")
                self.assertEqual(self.get.call_count, 3)

    def test_unrecognizable_response_structures(self):
        cases = [None, [], {}, {"realtimeArrivalList": []},
                 {"errorMessage": {"code": "INFO-000"}},
                 {"errorMessage": {"code": "INFO-000", "status": 500},
                  "realtimeArrivalList": []}]
        for data in cases:
            with self.subTest(data=data):
                self.get.reset_mock()
                self.get.return_value = response_with(data)
                with self.assertRaises(extract.StationRequestError):
                    extract.get_station_arrival("강남")
                self.assertEqual(self.get.call_count, 3)

    def test_explicit_no_data_is_success_without_retry(self):
        self.get.return_value = response_with({"errorMessage": {"code": "INFO-200"}})
        self.assertEqual(extract.get_station_arrival("강남"), [])
        self.get.assert_called_once()
        self.sleep.assert_not_called()

    def test_normal_empty_list_is_success_without_retry(self):
        self.get.return_value = normal_response([])
        self.assertEqual(extract.get_station_arrival("강남"), [])
        self.get.assert_called_once()

    def test_result_envelope(self):
        arrivals = [{"statnNm": "강남"}]
        self.get.return_value = response_with({
            "RESULT": {"CODE": "INFO-000"}, "realtimeArrivalList": arrivals,
        })
        self.assertEqual(extract.get_station_arrival("강남"), arrivals)

    def test_no_data_with_nonempty_or_invalid_list_is_failure(self):
        for arrivals in ([{"statnNm": "강남"}], None):
            with self.subTest(arrivals=arrivals):
                self.get.return_value = response_with({
                    "errorMessage": {"code": "INFO-200"},
                    "realtimeArrivalList": arrivals,
                })
                with self.assertRaises(extract.StationRequestError):
                    extract.get_station_arrival("강남")

    def run_main_and_read_saved_json(self):
        writer = mock_open()
        with patch("builtins.open", writer), patch.object(Path, "mkdir"):
            exit_code = extract.main()
        writer.assert_called_once()
        path, mode = writer.call_args.args
        self.assertRegex(path.name, r"^subway_batch_\d{8}_\d{6}\.json$")
        self.assertEqual(path.parent, extract.RAW_DIR)
        self.assertEqual(mode, "w")
        self.assertEqual(writer.call_args.kwargs["encoding"], "utf-8")
        saved = json.loads("".join(c.args[0] for c in writer().write.call_args_list))
        self.assertEqual(set(saved), {
            "collected_at", "success_station_count", "failed_station_count",
            "success_stations", "failed_stations", "realtimeArrivalList",
        })
        return exit_code, saved

    def test_partial_failure_saves_data_and_returns_zero(self):
        arrivals = [{"statnNm": "강남", "sequence": i} for i in range(50)]
        self.get.side_effect = (
            [normal_response(arrivals)] + [normal_response([]) for _ in range(15)]
            + [requests.Timeout("private URL") for _ in range(6)]
        )
        code, saved = self.run_main_and_read_saved_json()
        self.assertEqual(code, 0)
        self.assertEqual(saved["success_station_count"], 16)
        self.assertEqual(saved["failed_station_count"], 2)
        self.assertEqual(saved["success_stations"], extract.STATIONS[:16])
        self.assertEqual(saved["failed_stations"], extract.STATIONS[16:])
        self.assertEqual(saved["realtimeArrivalList"], arrivals)
        self.assertEqual(self.get.call_count, 22)

    def test_all_failures_save_diagnostic_json_and_return_one(self):
        self.get.side_effect = requests.Timeout("private URL")
        code, saved = self.run_main_and_read_saved_json()
        self.assertEqual(code, 1)
        self.assertEqual(saved["success_station_count"], 0)
        self.assertEqual(saved["success_stations"], [])
        self.assertEqual(saved["failed_station_count"], 18)
        self.assertEqual(saved["failed_stations"], extract.STATIONS)
        self.assertEqual(saved["realtimeArrivalList"], [])
        self.assertEqual(self.get.call_count, 54)

    def test_all_normal_but_empty_save_json_and_return_one(self):
        self.get.return_value = normal_response([])
        code, saved = self.run_main_and_read_saved_json()
        self.assertEqual(code, 1)
        self.assertEqual(saved["success_station_count"], 18)
        self.assertEqual(saved["failed_station_count"], 0)
        self.assertEqual(saved["realtimeArrivalList"], [])
        self.assertEqual(self.get.call_count, 18)

    def test_all_explicit_no_data_return_one(self):
        self.get.return_value = response_with({"RESULT": {"CODE": "INFO-200"}})
        code, saved = self.run_main_and_read_saved_json()
        self.assertEqual(code, 1)
        self.assertEqual(saved["success_station_count"], 18)

    def test_logs_do_not_expose_secrets_urls_or_response_messages(self):
        self.get.side_effect = requests.Timeout(
            "http://example.invalid/api/subway/test-only-key/json/ private-body"
        )
        with self.assertRaises(extract.StationRequestError) as caught:
            extract.get_station_arrival("강남")
        text = self.output.getvalue() + str(caught.exception)
        for private in ("test-only-key", "http://", "private-body"):
            self.assertNotIn(private, text)
        self.assertIn("강남", text)
        self.assertIn("시도 3/3", text)

    def test_internal_error_message_is_not_logged(self):
        self.get.return_value = response_with({"errorMessage": {
            "code": "ERROR-500", "message": "test-only-key private-body",
        }})
        with self.assertRaises(extract.StationRequestError):
            extract.get_station_arrival("강남")
        self.assertNotIn("test-only-key", self.output.getvalue())
        self.assertNotIn("private-body", self.output.getvalue())

    def test_unexpected_error_is_not_hidden(self):
        self.get.side_effect = RuntimeError("unexpected bug")
        with self.assertRaises(RuntimeError):
            extract.get_station_arrival("강남")


if __name__ == "__main__":
    unittest.main()
