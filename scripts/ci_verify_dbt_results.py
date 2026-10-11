"""Validate all enabled dbt tests, including the intentional duplicate warning."""

import argparse
from collections import Counter
import json
from pathlib import Path
import sys


WARNING_TEST = "warn_source_exact_duplicates"


def verify_results(results, manifest):
    expected = {
        key: node for key, node in manifest["nodes"].items()
        if node["resource_type"] == "test" and node.get("config", {}).get("enabled", True)
    }
    warning_ids = [key for key, node in expected.items() if node["name"] == WARNING_TEST]
    if len(warning_ids) != 1:
        raise ValueError("Expected exactly one duplicate warning test")
    executed = results["results"]
    ids = [item["unique_id"] for item in executed]
    if len(ids) != len(set(ids)) or set(ids) != set(expected):
        raise ValueError("Results must include every enabled test exactly once")
    warning_id = warning_ids[0]
    for item in executed:
        required = "warn" if item["unique_id"] == warning_id else "pass"
        if item["status"] != required:
            raise ValueError("Unexpected test status")
        if required == "warn" and not (
            isinstance(item.get("failures"), int) and item["failures"] > 0
        ):
            raise ValueError("Duplicate fixture must produce failing groups as WARN")
    return Counter(item["status"] for item in executed)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path,
                        default=Path("metro_dbt/target/run_results.json"))
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args(argv)
    manifest_path = args.manifest or args.results.with_name("manifest.json")
    try:
        counts = verify_results(
            json.loads(args.results.read_text(encoding="utf-8")),
            json.loads(manifest_path.read_text(encoding="utf-8")),
        )
    except (OSError, ValueError, KeyError, TypeError):
        print("CI dbt result verification FAILED: missing artifacts, tests or unexpected status.",
              file=sys.stderr)
        return 1
    print(f"CI dbt result verification PASS: PASS={counts['pass']} WARN={counts['warn']} "
          "FAIL=0 ERROR=0 SKIP=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
