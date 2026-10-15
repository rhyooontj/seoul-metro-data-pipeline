"""Read-only before/after evidence for the quality Mart deployment."""
import argparse
import hashlib
import json
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs/zero_arrival_analysis"
EXISTING = ("mart_station_arrival_summary", "mart_station_daily_summary", "mart_station_hourly_summary")


def snapshot():
    load_dotenv(ROOT / ".env")
    with psycopg.connect(host=os.getenv("POSTGRES_HOST", "localhost"), dbname=os.getenv("POSTGRES_DB"), user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"), options="-c default_transaction_read_only=on", row_factory=dict_row) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        result = {"raw": conn.execute("SELECT count(*) AS n, md5(string_agg(md5(row_to_json(t)::text), '' ORDER BY id)) AS checksum FROM public.subway_arrival t").fetchone(), "existing_relations": {}}
        for name in EXISTING:
            result["existing_relations"][name] = {
                "rows": conn.execute(f"SELECT count(*) AS n, md5(string_agg(row_to_json(t)::text, '' ORDER BY row_to_json(t)::text)) AS checksum FROM analytics.{name} t").fetchone(),
                "columns": conn.execute("SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_schema='analytics' AND table_name=%s ORDER BY ordinal_position", (name,)).fetchall(),
            }
        if conn.execute("SELECT to_regclass('analytics.mart_arrival_quality_daily') AS relation").fetchone()["relation"]:
            result["quality_scopes"] = conn.execute("""
                SELECT 'all' AS scope, count(*) AS groups,
                       sum(arrival_record_count) AS total,
                       sum(zero_arrival_count) AS zero,
                       sum(positive_arrival_count) AS positive,
                       sum(null_arrival_count) AS null_count,
                       sum(negative_arrival_count) AS negative,
                       sum(positive_arrival_seconds_sum) AS positive_sum,
                       sum(arrival_seconds_sum)::numeric / nullif(sum(arrival_record_count-null_arrival_count),0) AS avg_all,
                       sum(positive_arrival_seconds_sum)::numeric / nullif(sum(positive_arrival_count),0) AS avg_positive
                FROM analytics.mart_arrival_quality_daily
                UNION ALL
                SELECT 'since_20260930', count(*), sum(arrival_record_count),
                       sum(zero_arrival_count), sum(positive_arrival_count),
                       sum(null_arrival_count), sum(negative_arrival_count),
                       sum(positive_arrival_seconds_sum),
                       sum(arrival_seconds_sum)::numeric / nullif(sum(arrival_record_count-null_arrival_count),0),
                       sum(positive_arrival_seconds_sum)::numeric / nullif(sum(positive_arrival_count),0)
                FROM analytics.mart_arrival_quality_daily WHERE received_date >= date '2026-09-30'
            """).fetchall()
            result["zero_only"] = conn.execute("""
                SELECT count(*) AS groups, sum(arrival_record_count) AS total,
                       count(*) FILTER (WHERE avg_positive_arrival_seconds IS NOT NULL OR positive_arrival_seconds_sum != 0) AS invalid_groups
                FROM analytics.mart_arrival_quality_daily WHERE positive_arrival_count=0
            """).fetchone()
    excluded = {"arrival_quality_schema.yml", "mart_arrival_quality_daily.sql"}
    result["existing_dbt_files"] = {
        str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
        for folder in ("models", "tests") for p in (ROOT / "metro_dbt" / folder).rglob("*")
        if p.is_file() and p.name not in excluded and not p.name.startswith("mart_arrival_quality_daily_")
    }
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("before", "after"))
    args = parser.parse_args()
    EVIDENCE.mkdir(exist_ok=True)
    result = json.loads(json.dumps(snapshot(), ensure_ascii=False, default=str))
    if args.phase == "after":
        before = json.loads((EVIDENCE / "quality_mart_before.json").read_text(encoding="utf-8"))
        result["preservation_checks"] = {k: before[k] == result[k] for k in ("raw", "existing_relations", "existing_dbt_files")}
        assert all(result["preservation_checks"].values()), result["preservation_checks"]
        scopes = {r["scope"]: r for r in result["quality_scopes"]}
        assert int(scopes["all"]["total"]) == 31987
        assert int(scopes["since_20260930"]["total"]) == 31982
        assert result["zero_only"]["groups"] > 0 and result["zero_only"]["invalid_groups"] == 0
    (EVIDENCE / f"quality_mart_{args.phase}.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k != "existing_dbt_files"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
