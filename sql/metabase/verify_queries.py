"""Execute chart SELECTs in a read-only snapshot and compare to raw aggregates.

Run from repository root: .venv/Scripts/python.exe sql/metabase/verify_queries.py
Writes validation evidence and CSV results only beside these SQL files.
"""
import csv
import hashlib
import json
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
RELATIONS = (
    "public.subway_arrival", "analytics.mart_arrival_quality_daily",
    "analytics.mart_station_arrival_summary", "analytics.mart_station_daily_summary",
    "analytics.mart_station_hourly_summary",
)
AGGREGATES = """
    count(*) as arrival_record_count,
    count(*) filter (where arrival_seconds=0) as zero_arrival_count,
    count(*) filter (where arrival_seconds>0) as positive_arrival_count,
    count(*) filter (where arrival_seconds is null) as null_arrival_count,
    count(*) filter (where arrival_seconds<0) as negative_arrival_count,
    count(*) filter (where arrival_seconds=0)::numeric / nullif(count(*),0)
        as zero_arrival_rate,
    sum(arrival_seconds) as arrival_seconds_sum,
    coalesce(sum(arrival_seconds) filter (where arrival_seconds>0),0)
        as positive_arrival_seconds_sum,
    avg(arrival_seconds) as avg_arrival_seconds,
    avg(arrival_seconds) filter (where arrival_seconds>0) as avg_positive_arrival_seconds
"""


def snapshot(conn):
    return {
        name: conn.execute(f"SELECT count(*) as rows, md5(string_agg(row_to_json(t)::text, '' ORDER BY row_to_json(t)::text)) as content_checksum FROM {name} t").fetchone()
        for name in RELATIONS
    }


def model_hashes():
    return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for folder in ("models", "tests") for p in (ROOT / "metro_dbt" / folder).rglob("*") if p.is_file()}


def main():
    load_dotenv(ROOT / ".env")
    report = {"dbt_files_before": model_hashes(), "queries": {}}
    with psycopg.connect(host=os.getenv("POSTGRES_HOST", "localhost"), dbname=os.getenv("POSTGRES_DB"), user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"), options="-c default_transaction_read_only=on", row_factory=dict_row) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        report["transaction"] = conn.execute("SELECT current_setting('transaction_read_only') as read_only, current_setting('transaction_isolation') as isolation, now() as checked_at").fetchone()
        report["before"] = snapshot(conn)
        report["columns"] = conn.execute("SELECT column_name, data_type, udt_name, character_maximum_length, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_schema='analytics' AND table_name='mart_arrival_quality_daily' ORDER BY ordinal_position").fetchall()
        report["raw_checksum"] = conn.execute("SELECT count(*) as rows, md5(string_agg(md5(row_to_json(t)::text), '' ORDER BY id)) as checksum FROM public.subway_arrival t").fetchone()
        reconciliation = (ROOT / "metro_dbt/tests/mart_arrival_quality_daily_reconciliation.sql").read_text(encoding="utf-8").replace("{{ source('raw', 'subway_arrival') }}", "public.subway_arrival").replace("{{ ref('mart_arrival_quality_daily') }}", "analytics.mart_arrival_quality_daily")
        differences = conn.execute(reconciliation).fetchall()
        assert not differences, "Mart differs from raw"
        report["mart_raw_differences"] = len(differences)
        report["scopes"] = []
        for label, condition in (("all", "true"), ("since_20260930", "received_at >= timestamp '2026-09-30'")):
            raw = conn.execute(f"SELECT {AGGREGATES} FROM public.subway_arrival WHERE {condition}").fetchone()
            report["scopes"].append(dict(scope=label, **raw))
        assert report["scopes"][0]["arrival_record_count"] == 31987
        assert report["scopes"][1]["arrival_record_count"] == 31982
        specs = (
            ("01_line_zero_arrival_rate.sql", "subway_id", "subway_id"),
            ("02_station_arrival_metrics.sql", "station_name", "station_name"),
            ("03_daily_zero_arrival_trend.sql", "received_date", "received_at::date"),
        )
        for filename, key, expression in specs:
            actual = conn.execute((HERE / filename).read_text(encoding="utf-8")).fetchall()
            expected = conn.execute(f"SELECT {expression} as {key}, {AGGREGATES} FROM public.subway_arrival WHERE received_at >= timestamp '2026-09-30' GROUP BY {expression}").fetchall()
            indexed = {r[key]: r for r in expected}
            assert len(actual) == len(indexed) and {r[key] for r in actual} == set(indexed)
            for row in actual:
                for col in row.keys() & indexed[row[key]].keys():
                    assert row[col] == indexed[row[key]][col], (filename, row[key], col)
                assert row["arrival_record_count"] == sum(row[c] for c in ("zero_arrival_count", "positive_arrival_count", "null_arrival_count", "negative_arrival_count"))
                assert 0 <= row["zero_arrival_rate"] <= 1
            assert sum(r["arrival_record_count"] for r in actual) == 31982
            assert sum(r["zero_arrival_count"] for r in actual) == 10524
            assert sum(r["positive_arrival_count"] for r in actual) == 21458
            with (HERE / filename.replace(".sql", ".csv")).open("w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(actual[0]))
                writer.writeheader()
                writer.writerows(actual)
            report["queries"][filename] = {"rows": len(actual), "raw_aggregate_differences": 0, "total_records": sum(r["arrival_record_count"] for r in actual), "zero_records": sum(r["zero_arrival_count"] for r in actual), "positive_records": sum(r["positive_arrival_count"] for r in actual), "results": actual}
        report["unmapped_lines"] = [r["subway_id"] for r in report["queries"]["01_line_zero_arrival_rate.sql"]["results"] if not r["line_mapping_verified"]]
        report["zero_only_groups"] = conn.execute("SELECT count(*) as groups, sum(arrival_record_count) as observations, count(*) filter (where avg_positive_arrival_seconds is not null or positive_arrival_seconds_sum != 0) as invalid_groups FROM analytics.mart_arrival_quality_daily WHERE positive_arrival_count=0").fetchone()
        assert report["zero_only_groups"]["invalid_groups"] == 0
        # Roll up the same zero-only subset: NULL must survive sum/count division.
        check = conn.execute("SELECT sum(positive_arrival_seconds_sum)::numeric / nullif(sum(positive_arrival_count),0) as avg_positive FROM analytics.mart_arrival_quality_daily WHERE positive_arrival_count=0").fetchone()
        assert check["avg_positive"] is None
        report["zero_only_rollup_avg"] = check["avg_positive"]
        station_sql = (HERE / "02_station_arrival_metrics.sql").read_text(encoding="utf-8")
        zero_only_sql = station_sql.replace(
            "from analytics.mart_arrival_quality_daily",
            "from (select * from analytics.mart_arrival_quality_daily where positive_arrival_count=0) as zero_only_input",
        )
        zero_only_results = conn.execute(zero_only_sql).fetchall()
        assert zero_only_results and all(
            r["avg_positive_arrival_seconds"] is None
            and r["positive_arrival_seconds_sum"] == 0
            and r["arrival_record_count"] > 0
            for r in zero_only_results
        )
        report["station_query_zero_only_input"] = {"result_rows": len(zero_only_results), "all_positive_averages_null": True}
        report["after_same_snapshot"] = snapshot(conn)
        assert report["before"] == report["after_same_snapshot"]
    # Fresh connection additionally checks committed state after all SELECTs.
    with psycopg.connect(host=os.getenv("POSTGRES_HOST", "localhost"), dbname=os.getenv("POSTGRES_DB"), user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"), options="-c default_transaction_read_only=on", row_factory=dict_row) as conn:
        report["after_fresh_connection"] = snapshot(conn)
    report["dbt_files_after"] = model_hashes()
    assert report["before"] == report["after_fresh_connection"]
    assert report["dbt_files_before"] == report["dbt_files_after"]
    report["preserved"] = True
    (HERE / "validation_results.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"transaction": report["transaction"], "columns": report["columns"], "scopes": report["scopes"], "queries": {k: {c: v for c,v in r.items() if c != "results"} for k,r in report["queries"].items()}, "zero_only_groups": report["zero_only_groups"], "unmapped_lines": report["unmapped_lines"], "raw_checksum": report["raw_checksum"], "preserved": report["preserved"]}, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
