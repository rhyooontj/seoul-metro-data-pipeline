import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import psycopg

from src import load


ARRIVAL = {
    "subway_id": "1002", "station_id": "1002000222", "station_name": "강남",
    "direction": "상행", "train_line": "성수행", "train_no": "1234",
    "arrival_seconds": 90, "arrival_message": "도착 예정", "current_station": "교대",
    "received_at": "2026-10-07T18:00:00",
}
BATCH_FILE = Path("data/processed/subway_batch_20261007_093000_cleaned.json")


class LoadTests(unittest.TestCase):
    def setUp(self):
        self.output = io.StringIO()
        self.enterContext(redirect_stdout(self.output))
        self.connect = self.enterContext(patch.object(load.psycopg, "connect"))
        self.conn = self.connect.return_value
        self.cur = self.conn.cursor.return_value.__enter__.return_value
        self.cur.fetchone.side_effect = [
            ("public.pipeline_load_history",), (BATCH_FILE.name,),
        ]
        self.rows = [dict(ARRIVAL), dict(ARRIVAL, train_no="5678")]
        self.reader = self.enterContext(patch(
            "builtins.open", mock_open(read_data=json.dumps(self.rows)),
        ))

    def arrival_calls(self):
        return [c for c in self.cur.execute.call_args_list
                if "INSERT INTO public.subway_arrival" in c.args[0]]

    def history_calls(self):
        return [c for c in self.cur.execute.call_args_list
                if "INSERT INTO public.pipeline_load_history" in c.args[0]]

    def test_new_batch_claims_history_and_inserts_then_commits(self):
        self.assertEqual(load.load_data(BATCH_FILE), 2)
        self.conn.commit.assert_called_once()
        self.conn.rollback.assert_not_called()
        self.conn.close.assert_called_once()
        self.assertEqual(len(self.history_calls()), 1)
        self.assertEqual(len(self.arrival_calls()), 2)
        history = self.history_calls()[0]
        self.assertEqual(history.args[1], (BATCH_FILE.name, BATCH_FILE.name, 2))
        self.assertIn("ON CONFLICT (batch_id) DO NOTHING", history.args[0])
        self.assertIn("RETURNING batch_id", history.args[0])
        events = [c[0] for c in self.conn.mock_calls]
        history_index = next(
            i for i, c in enumerate(self.conn.mock_calls)
            if c[0] == "cursor().__enter__().execute"
            and "INSERT INTO public.pipeline_load_history" in c.args[0]
        )
        data_indexes = [
            i for i, c in enumerate(self.conn.mock_calls)
            if c[0] == "cursor().__enter__().execute"
            and "INSERT INTO public.subway_arrival" in c.args[0]
        ]
        self.assertLess(history_index, min(data_indexes))
        self.assertLess(max(data_indexes), events.index("commit"))

    def test_existing_batch_skips_all_arrival_inserts(self):
        self.cur.fetchone.side_effect = [("public.pipeline_load_history",), None]
        self.assertEqual(load.load_data(BATCH_FILE), 0)
        self.assertEqual(self.arrival_calls(), [])
        self.assertEqual(len(self.history_calls()), 1)
        self.conn.commit.assert_called_once()
        self.conn.rollback.assert_not_called()
        self.assertIn("이미 처리된 배치이므로 skip", self.output.getvalue())

    def test_data_insert_error_rolls_back_claim_and_data(self):
        def execute(sql, *params):
            if "INSERT INTO public.subway_arrival" in sql:
                if len(self.arrival_calls()) == 2:
                    raise psycopg.DatabaseError("test failure")
        self.cur.execute.side_effect = execute
        with self.assertRaises(psycopg.DatabaseError):
            load.load_data(BATCH_FILE)
        self.assertEqual(len(self.history_calls()), 1)
        self.assertEqual(len(self.arrival_calls()), 2)
        self.conn.rollback.assert_called_once()
        self.conn.commit.assert_not_called()
        self.conn.close.assert_called_once()

    def test_history_insert_error_rolls_back_without_data(self):
        def execute(sql, *params):
            if "INSERT INTO public.pipeline_load_history" in sql:
                raise psycopg.DatabaseError("test failure")
        self.cur.execute.side_effect = execute
        with self.assertRaises(psycopg.DatabaseError):
            load.load_data(BATCH_FILE)
        self.assertEqual(self.arrival_calls(), [])
        self.conn.rollback.assert_called_once()
        self.conn.commit.assert_not_called()
        self.conn.close.assert_called_once()

    def test_commit_error_rolls_back(self):
        self.conn.commit.side_effect = psycopg.DatabaseError("test failure")
        with self.assertRaises(psycopg.DatabaseError):
            load.load_data(BATCH_FILE)
        self.conn.rollback.assert_called_once()
        self.conn.close.assert_called_once()

    def test_empty_processed_list_fails_before_connect(self):
        self.reader.return_value.read.return_value = "[]"
        with self.assertRaises(ValueError):
            load.load_data(BATCH_FILE)
        self.connect.assert_not_called()

    def test_invalid_processed_structure_fails_before_connect(self):
        for data in (None, {}, {"realtimeArrivalList": []}, [None], [{}]):
            with self.subTest(data=data):
                self.reader.return_value.read.return_value = json.dumps(data)
                with self.assertRaises(ValueError):
                    load.load_data(BATCH_FILE)
                self.connect.assert_not_called()

    def test_same_file_always_has_same_batch_id(self):
        self.assertEqual(load.get_batch_id(BATCH_FILE), BATCH_FILE.name)
        self.assertEqual(load.get_batch_id(BATCH_FILE), load.get_batch_id(str(BATCH_FILE)))
        self.assertEqual(load.get_batch_id(BATCH_FILE),
                         load.get_batch_id(Path("/another/directory") / BATCH_FILE.name))

    def test_different_filenames_have_different_batch_ids(self):
        self.assertNotEqual(load.get_batch_id(BATCH_FILE),
                            load.get_batch_id("subway_batch_20261007_100000_cleaned.json"))

    def test_original_insert_column_order_and_values_are_preserved(self):
        load.load_data(BATCH_FILE)
        expected_columns = (
            "subway_id", "station_id", "station_name", "direction", "train_line",
            "train_no", "arrival_seconds", "arrival_message", "current_station",
            "received_at",
        )
        self.assertEqual(load.ARRIVAL_COLUMNS, expected_columns)
        for row, executed in zip(self.rows, self.arrival_calls()):
            self.assertEqual(executed.args[1], tuple(row[c] for c in expected_columns))
            self.assertEqual(executed.args[0].count("%s"), 10)
            positions = [executed.args[0].index(c) for c in expected_columns]
            self.assertEqual(positions, sorted(positions))

    def test_missing_history_default_is_fail_closed(self):
        self.cur.fetchone.side_effect = [(None,)]
        with self.assertRaises(load.LoadHistoryUnavailable):
            load.load_data(BATCH_FILE)
        self.assertEqual(self.history_calls(), [])
        self.assertEqual(self.arrival_calls(), [])
        self.conn.rollback.assert_called_once()
        self.conn.commit.assert_not_called()

    def test_explicit_legacy_mode_preserves_pre_migration_operations(self):
        self.cur.fetchone.side_effect = [(None,)]
        self.assertEqual(load.load_data(BATCH_FILE, allow_legacy=True), 2)
        self.assertEqual(self.history_calls(), [])
        self.assertEqual(len(self.arrival_calls()), 2)
        self.conn.commit.assert_called_once()
        self.assertIn("멱등성은 비활성", self.output.getvalue())

    def test_history_present_enforces_idempotency_even_in_legacy_mode(self):
        self.cur.fetchone.side_effect = [("public.pipeline_load_history",), None]
        self.assertEqual(load.load_data(BATCH_FILE, allow_legacy=True), 0)
        self.assertEqual(self.arrival_calls(), [])

    def test_main_duplicate_skip_is_success(self):
        self.cur.fetchone.side_effect = [("public.pipeline_load_history",), None]
        with patch.object(load, "get_latest_processed_file", return_value=BATCH_FILE):
            self.assertEqual(load.main(), 0)
        self.assertEqual(self.arrival_calls(), [])

    def test_main_empty_batch_is_failure_without_history(self):
        self.reader.return_value.read.return_value = "[]"
        with patch.object(load, "get_latest_processed_file", return_value=BATCH_FILE):
            self.assertEqual(load.main(), 1)
        self.connect.assert_not_called()

    def test_password_and_connection_details_are_not_in_sql_or_logs(self):
        secret = "test-only-password"
        with patch.object(load, "DB_PASSWORD", secret):
            load.load_data(BATCH_FILE)
        for executed in self.cur.execute.call_args_list:
            self.assertNotIn(secret, executed.args[0])
        self.assertNotIn(secret, self.output.getvalue())
        self.assertEqual(self.connect.call_args.kwargs["password"], secret)

    def test_existing_connection_arguments_are_preserved(self):
        with patch.multiple(load, DB_HOST="test-host", DB_NAME="test-db",
                            DB_USER="test-user", DB_PASSWORD="test-only-password"):
            load.load_data(BATCH_FILE)
        self.connect.assert_called_once_with(
            host="test-host", port=5432, dbname="test-db",
            user="test-user", password="test-only-password",
        )

    def test_main_database_errors_are_not_printed(self):
        self.connect.side_effect = psycopg.OperationalError(
            "postgresql://user:test-only-password@example.invalid/db"
        )
        with patch.object(load, "get_latest_processed_file", return_value=BATCH_FILE):
            self.assertEqual(load.main(), 1)
        for private in ("test-only-password", "postgresql://", "example.invalid"):
            self.assertNotIn(private, self.output.getvalue())

    def test_unexpected_errors_roll_back_and_propagate(self):
        self.cur.execute.side_effect = RuntimeError("unexpected bug")
        with self.assertRaises(RuntimeError):
            load.load_data(BATCH_FILE)
        self.conn.rollback.assert_called_once()
        self.conn.commit.assert_not_called()
        self.conn.close.assert_called_once()


class TransactionSimulationTests(unittest.TestCase):
    """An in-memory transaction model: no PostgreSQL connection or writes."""

    def setUp(self):
        self.output = io.StringIO()
        self.enterContext(redirect_stdout(self.output))
        self.enterContext(patch("builtins.open", mock_open(read_data=json.dumps([
            ARRIVAL, dict(ARRIVAL, train_no="5678"),
        ]))))
        self.history = {}
        self.arrivals = []

    def connection(self, *, fail_data=False):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        pending_history = {}
        pending_arrivals = []
        result = None

        def execute(sql, params=None):
            nonlocal result
            if "to_regclass" in sql:
                result = ("public.pipeline_load_history",)
            elif "INSERT INTO public.pipeline_load_history" in sql:
                batch_id = params[0]
                if batch_id in self.history:
                    result = None
                else:
                    pending_history[batch_id] = params
                    result = (batch_id,)
            elif "INSERT INTO public.subway_arrival" in sql:
                if fail_data and pending_arrivals:
                    raise psycopg.DatabaseError("simulated insert error")
                pending_arrivals.append(params)

        def commit():
            self.history.update(pending_history)
            self.arrivals.extend(pending_arrivals)
            pending_history.clear()
            pending_arrivals.clear()

        def rollback():
            pending_history.clear()
            pending_arrivals.clear()

        cur.execute.side_effect = execute
        cur.fetchone.side_effect = lambda: result
        conn.commit.side_effect = commit
        conn.rollback.side_effect = rollback
        return conn

    def test_same_batch_twice_commits_only_one_set_of_arrivals(self):
        with patch.object(load.psycopg, "connect", side_effect=lambda **kw: self.connection()):
            self.assertEqual(load.load_data(BATCH_FILE), 2)
            self.assertEqual(load.load_data(BATCH_FILE), 0)
        self.assertEqual(len(self.arrivals), 2)
        self.assertEqual(list(self.history), [BATCH_FILE.name])

    def test_failed_batch_has_no_history_and_can_be_retried(self):
        with patch.object(load.psycopg, "connect", return_value=self.connection(fail_data=True)):
            with self.assertRaises(psycopg.DatabaseError):
                load.load_data(BATCH_FILE)
        self.assertEqual(self.history, {})
        self.assertEqual(self.arrivals, [])
        with patch.object(load.psycopg, "connect", return_value=self.connection()):
            self.assertEqual(load.load_data(BATCH_FILE), 2)
        self.assertEqual(len(self.arrivals), 2)
        self.assertEqual(len(self.history), 1)


if __name__ == "__main__":
    unittest.main()
