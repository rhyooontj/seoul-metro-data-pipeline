"""Read-only diagnosis; writes evidence only under docs/zero_arrival_analysis.

Run from repository root: .venv/Scripts/python.exe docs/diagnose_zero_arrivals.py
No extraction, loading, dbt execution or dashboard updates are performed.
"""
import csv
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs/zero_arrival_analysis"
COLS = ("subway_id", "station_id", "station_name", "direction", "train_line", "train_no", "arrival_seconds", "arrival_message", "current_station", "received_at")
RAW_COLS = ("subwayId", "statnId", "statnNm", "updnLine", "trainLineNm", "btrainNo", "barvlDt", "arvlMsg2", "arvlMsg3", "recptnDt")


def key(row):
    return tuple(row[c] for c in COLS)


def raw_key(row):
    values = [row.get(c) for c in RAW_COLS]
    values[6] = int(values[6])
    values[9] = datetime.strptime(values[9], "%Y-%m-%d %H:%M:%S")
    return tuple(values)


def message_category(msg):
    if re.search(r"\[?\d+\]?\s*번째 전역", msg):
        return "N번째 전역"
    if "분" in msg or "초" in msg:
        return "시간 안내"
    for phrase in ("전역 출발", "전역 진입", "전역 도착", "출발", "진입", "도착"):
        if phrase in msg:
            return phrase
    return "기타"


def stats(rows):
    vals = [r["arrival_seconds"] for r in rows]
    positives = [v for v in vals if v is not None and v > 0]
    zeros = vals.count(0)
    avg_all = sum(v for v in vals if v is not None) / sum(v is not None for v in vals)
    avg_positive = sum(positives) / len(positives) if positives else None
    return dict(total=len(vals), zero=zeros, positive=len(positives), null=vals.count(None), negative=sum(v is not None and v < 0 for v in vals), zero_pct=round(100*zeros/len(vals), 4), sum_positive=sum(positives), avg_all=round(avg_all, 4), avg_positive=round(avg_positive, 4) if avg_positive is not None else None, delta_seconds=round(avg_positive-avg_all, 4) if avg_positive is not None else None)


def grouped(rows, columns):
    groups = defaultdict(list)
    for row in rows:
        groups[tuple(row[c] for c in columns)].append(row)
    return [dict(zip(columns, k), **stats(v)) for k, v in sorted(groups.items(), key=lambda x: tuple(str(i) for i in x[0]))]


def write_csv(name, rows):
    with (OUT / (name + ".csv")).open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else [])
        writer.writeheader()
        writer.writerows(rows)


def write_tables(outputs):
    sections = []
    names = {"by_station": "역별 0초 비율 및 평균 민감도", "by_status": "열차 상태별 0초 현황", "by_message_category": "도착 메시지 유형별 0초 현황", "by_line": "호선별 0초 현황", "by_day": "API 생성일별 표본과 0초 비율"}
    code_names = {"0": "진입", "1": "도착", "2": "출발", "3": "전역 출발", "4": "전역 진입", "5": "전역 도착", "99": "운행 중"}
    line_names = {"1001": "1호선", "1002": "2호선", "1003": "3호선", "1004": "4호선", "1005": "5호선", "1006": "6호선", "1007": "7호선", "1008": "8호선", "1009": "9호선", "1063": "경의중앙선", "1065": "공항철도", "1075": "수인분당선", "1077": "신분당선"}
    for name, title in names.items():
        sections.extend(["### " + title, "", "| 구분 | 전체 | 0초 | 0초 비율 | 포함 평균 | 양수 평균 | 차이 |", "|---|---:|---:|---:|---:|---:|---:|"])
        for row in outputs[name]:
            label = str(next(iter(row.values())))
            if name == "by_status":
                label += " " + code_names.get(label, "미확인")
            if name == "by_line":
                label += " " + line_names.get(label, "미확인")
            cells = [label, f"{row['total']:,}", f"{row['zero']:,}", f"{row['zero_pct']:.2f}%"]
            cells += [f"{row[c]:.2f}" if row[c] is not None else "NULL(표본 없음)" for c in ("avg_all", "avg_positive", "delta_seconds")]
            sections.append("| " + " | ".join(cells) + " |")
        sections.append("")
    (OUT / "tables.md").write_text("\n".join(sections) + "\n", encoding="utf-8")


def main():
    load_dotenv(ROOT / ".env")
    OUT.mkdir(exist_ok=True)
    conn = psycopg.connect(host=os.getenv("POSTGRES_HOST", "localhost"), dbname=os.getenv("POSTGRES_DB"), user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"), options="-c default_transaction_read_only=on", row_factory=dict_row)
    with conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        before = conn.execute("SELECT count(*) AS n, md5(string_agg(md5(row_to_json(t)::text), '' ORDER BY id)) AS checksum FROM public.subway_arrival t").fetchone()
        db_rows = conn.execute("SELECT * FROM public.subway_arrival ORDER BY id").fetchall()
        tables = conn.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_name LIKE 'mart_%' OR table_name LIKE '%card%' OR table_name LIKE '%dashboard%'").fetchall()
        after = conn.execute("SELECT count(*) AS n, md5(string_agg(md5(row_to_json(t)::text), '' ORDER BY id)) AS checksum FROM public.subway_arrival t").fetchone()
    matched = defaultdict(list)
    raw_counts = Counter()
    raw_rows = []
    parse_errors = []
    for path in sorted((ROOT / "data/raw").glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        for raw in data.get("realtimeArrivalList", []):
            try:
                k = raw_key(raw)
            except (ValueError, TypeError, KeyError) as exc:
                parse_errors.append(dict(file=path.name, error=type(exc).__name__))
                continue
            record = dict(zip(COLS, k), arvl_cd=str(raw.get("arvlCd")), train_type=str(raw.get("btrainSttus")), file=path.name, collected_at=data.get("collected_at"))
            matched[k].append(record)
            raw_counts[k] += 1
            raw_rows.append(record)
    analysis = [r.copy() for r in db_rows if r["received_at"] >= datetime(2026, 9, 30)]
    unmatched = []
    ambiguous = []
    for row in analysis:
        matches = matched[key(row)]
        codes = {r["arvl_cd"] for r in matches}
        types = {r["train_type"] for r in matches}
        row["arvl_cd"] = next(iter(codes)) if len(codes) == 1 else "UNMATCHED" if not codes else "AMBIGUOUS"
        row["train_type"] = next(iter(types)) if len(types) == 1 else "UNMATCHED" if not types else "AMBIGUOUS"
        row["message_category"] = message_category(row["arrival_message"])
        row["received_date"] = row["received_at"].date().isoformat()
        row["received_hour"] = row["received_at"].hour
        if not matches:
            unmatched.append(row["id"])
        if len(codes) > 1:
            ambiguous.append(row["id"])
    outputs = {
        "by_station": grouped(analysis, ("station_name",)),
        "by_station_line_direction": grouped(analysis, ("station_name", "subway_id", "direction")),
        "by_status": grouped(analysis, ("arvl_cd",)),
        "by_message_category": grouped(analysis, ("message_category",)),
        "by_message_exact": grouped(analysis, ("arrival_message",)),
        "by_status_message": grouped(analysis, ("arvl_cd", "message_category")),
        "by_line": grouped(analysis, ("subway_id",)),
        "by_train_type": grouped(analysis, ("train_type",)),
        "by_day": grouped(analysis, ("received_date",)),
        "by_hour": grouped(analysis, ("received_hour",)),
    }
    for name, rows in outputs.items():
        write_csv(name, rows)
    write_tables(outputs)
    db_counts = Counter(key(r) for r in db_rows)
    mismatch = [dict(key=str(k), db_count=n, raw_count=raw_counts[k]) for k, n in db_counts.items() if raw_counts[k] != n]
    examples = []
    seen = set()
    for row in analysis:
        if row["arrival_seconds"] == 0 and row["message_category"] not in seen:
            seen.add(row["message_category"])
            examples.append(dict(db_id=row["id"], **matched[key(row)][0]))
    report = dict(checked_at=datetime.now().isoformat(), filter="received_at >= TIMESTAMP '2026-09-30 00:00:00' (KST interpretation)", before=before, after=after, unchanged=before == after, full=stats(db_rows), analysis=stats(analysis), raw_files=len(list((ROOT / "data/raw").glob("*.json"))), raw_rows=len(raw_rows), raw_parse_errors=parse_errors, db_raw_multiplicity_mismatches=mismatch, raw_extra_rows=sum(max(0,n-db_counts[k]) for k,n in raw_counts.items()), unmatched_ids=unmatched, ambiguous_status_ids=ambiguous, zero_examples=examples, tables=tables, **outputs)
    (OUT / "summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("before", "after", "unchanged", "full", "analysis", "raw_files", "raw_rows", "raw_extra_rows", "unmatched_ids", "ambiguous_status_ids", "by_status", "by_message_category", "by_line", "by_station")}, ensure_ascii=False, indent=2, default=str))
    print("DB/raw multiplicity mismatches:", len(mismatch), "parse errors:", len(parse_errors))


if __name__ == "__main__":
    main()
