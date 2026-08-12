#!/usr/bin/env python3
"""Verify query-result equivalence across NGRAM build modes."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys


MODES = ("regular", "auto", "force_direct")


def metric_files(mode_root: pathlib.Path) -> list[pathlib.Path]:
    bundle_root = mode_root / "bundle"
    if not bundle_root.is_dir():
        bundle_root = mode_root
    files = sorted(bundle_root.glob("cases/*/metrics.json"))
    if not files:
        raise ValueError(f"no metrics.json files below {mode_root}")
    return files


def load_mode(mode_root: pathlib.Path) -> tuple[dict, dict]:
    correctness = {}
    performance = {}
    for path in metric_files(mode_root):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for test in payload.get("tests", []):
            key = (
                test.get("suite_name", ""),
                test.get("dataset", ""),
                test.get("expression", ""),
                test.get("actual_expression", ""),
            )
            if key in correctness:
                raise ValueError(f"duplicate query identity in {mode_root}: {key}")
            correctness[key] = {
                "matched_rows": int(test["matched_rows"]),
                "total_rows": int(test["total_rows"]),
            }
            performance[key] = {
                "index_build_ms": test.get("index_build_ms"),
                "index_size_bytes": test.get("index_size_bytes"),
                "index_config": test.get("index_config", ""),
            }
    if not correctness:
        raise ValueError(f"no tests in metrics below {mode_root}")
    return correctness, performance


def validate_result_root(result_root: pathlib.Path) -> dict:
    result_root = pathlib.Path(result_root)
    by_mode = {}
    performance = {}
    for mode in MODES:
        by_mode[mode], performance[mode] = load_mode(result_root / mode)

    baseline = by_mode["regular"]
    for mode in MODES[1:]:
        if set(by_mode[mode]) != set(baseline):
            missing = sorted(set(baseline) - set(by_mode[mode]))
            extra = sorted(set(by_mode[mode]) - set(baseline))
            raise ValueError(
                f"query identities differ for {mode}: "
                f"missing={missing}, extra={extra}"
            )
        for key, expected in baseline.items():
            actual = by_mode[mode][key]
            for field in ("matched_rows", "total_rows"):
                if actual[field] != expected[field]:
                    raise ValueError(
                        f"{field} differs for {mode} at {key}: "
                        f"expected {expected[field]}, got {actual[field]}"
                    )

    row_counts = {value["total_rows"] for value in baseline.values()}
    if len(row_counts) != 1:
        raise ValueError(f"inconsistent total_rows in regular results: {sorted(row_counts)}")
    canonical_rows = [
        {"identity": list(key), **baseline[key]} for key in sorted(baseline)
    ]
    canonical = json.dumps(canonical_rows, sort_keys=True, separators=(",", ":"))
    performance_rows = {
        mode: [
            {"identity": list(key), **values}
            for key, values in sorted(mode_performance.items())
        ]
        for mode, mode_performance in performance.items()
    }
    summary = {
        "row_count": row_counts.pop(),
        "query_count": len(baseline),
        "correctness_sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "performance": performance_rows,
    }
    (result_root / "validation.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("result_root", type=pathlib.Path)
    args = parser.parse_args()
    summary = validate_result_root(args.result_root.resolve())
    print(
        f"validated {summary['query_count']} queries over {summary['row_count']} rows; "
        f"sha256={summary['correctness_sha256']}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
