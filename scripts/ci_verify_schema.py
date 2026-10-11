"""Read-only schema verification for an empty, explicitly designated CI DB."""

import os
import sys

import psycopg


# Matches the reviewed baseline DDL, including ordinal position and nullability.
EXPECTED_COLUMNS = {
    "subway_arrival": [
        ("id", "bigint", None, "NO"),
        ("subway_id", "character varying", 20, "YES"),
        ("station_id", "character varying", 20, "YES"),
        ("station_name", "character varying", 100, "YES"),
        ("direction", "character varying", 20, "YES"),
        ("train_line", "character varying", 100, "YES"),
        ("train_no", "character varying", 20, "YES"),
        ("arrival_seconds", "integer", None, "YES"),
        ("arrival_message", "character varying", 100, "YES"),
        ("current_station", "character varying", 100, "YES"),
        ("received_at", "timestamp without time zone", None, "YES"),
    ],
    "pipeline_load_history": [
        ("batch_id", "text", None, "NO"),
        ("source_file", "text", None, "NO"),
        ("loaded_at", "timestamp with time zone", None, "NO"),
        ("row_count", "bigint", None, "NO"),
    ],
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def verify_schema(conn):
    with conn.cursor() as cur:
        cur.execute("SHOW transaction_read_only")
        require(cur.fetchone() == ("on",), "Schema session must be read-only")
        for table, expected in EXPECTED_COLUMNS.items():
            cur.execute(
                "SELECT table_type FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s",
                (table,),
            )
            require(cur.fetchone() == ("BASE TABLE",), f"Missing table: {table}")
            cur.execute(
                "SELECT column_name, data_type, character_maximum_length, "
                "is_nullable, column_default, is_identity "
                "FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s "
                "ORDER BY ordinal_position",
                (table,),
            )
            columns = cur.fetchall()
            require([row[:4] for row in columns] == expected,
                    f"Column structure mismatch: {table}")
            expected_defaults = {
                "id": "nextval('subway_arrival_id_seq'::regclass)",
                "loaded_at": "CURRENT_TIMESTAMP",
            }
            for row in columns:
                require(row[4] == expected_defaults.get(row[0]),
                        f"Default mismatch: {table}.{row[0]}")
                require(row[5] == "NO", f"Expected BIGSERIAL baseline: {table}")
            cur.execute(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conrelid = to_regclass(%s) AND contype = 'p'",
                (f"public.{table}",),
            )
            key = "id" if table == "subway_arrival" else "batch_id"
            require(cur.fetchall() == [(f"PRIMARY KEY ({key})",)],
                    f"Primary key mismatch: {table}")

        cur.execute("SELECT pg_get_serial_sequence('public.subway_arrival', 'id')")
        require(cur.fetchone() == ("public.subway_arrival_id_seq",),
                "ID sequence ownership mismatch")
        cur.execute(
            "SELECT data_type, start_value, min_value, max_value, increment_by, "
            "cycle, cache_size FROM pg_sequences "
            "WHERE schemaname = 'public' AND sequencename = 'subway_arrival_id_seq'"
        )
        require(cur.fetchone() == ("bigint", 1, 1, 9223372036854775807, 1, False, 1),
                "ID sequence structure mismatch")
        cur.execute(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = 'public.pipeline_load_history'::regclass "
            "AND contype = 'c'"
        )
        require(cur.fetchall() == [("CHECK ((row_count > 0))",)],
                "History positive row_count constraint mismatch")
        cur.execute("SELECT count(*) FROM public.pipeline_load_history")
        require(cur.fetchone() == (0,), "Initial history must be empty")
        cur.execute("SELECT count(*) FROM public.subway_arrival")
        require(cur.fetchone() == (0,), "Initial arrival table must be empty")


def main():
    try:
        require(os.environ.get("CI_SCHEMA_VERIFY") == "1",
                "Set CI_SCHEMA_VERIFY=1 only for a disposable CI DB")
        host = os.environ["POSTGRES_HOST"]
        require(host in ("127.0.0.1", "localhost") or host.startswith("metro-ci-"),
                "Only local or explicitly named CI hosts are allowed")
        require(os.environ["POSTGRES_DB"] == "metro_db", "Unexpected CI database")
        with psycopg.connect(
            host=host,
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            connect_timeout=10,
            options="-c default_transaction_read_only=on",
        ) as conn:
            verify_schema(conn)
    except (psycopg.Error, KeyError, ValueError):
        # Driver exceptions can contain credentials or connection details.
        print("CI schema verification FAILED: connection, configuration or schema mismatch.",
              file=sys.stderr)
        return 1
    print("CI schema verification PASS: columns, defaults, PKs, sequence, empty tables.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
