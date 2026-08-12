#!/usr/bin/env python3
"""Run scalar-benchmark once per experimental NGRAM build mode.

This B-branch helper creates a temporary scalar-benchmark case from a compact
cardinality spec. Generated dictionaries, temporary cases, Milvus storage, and
result bundles live below --results-dir and are never written into the source
tree.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time


MODES = ("regular", "auto", "force_direct")
RESULT_DIR_RE = re.compile(r"^\s*Output directory:\s*(.+?)\s*$", re.MULTILINE)


def load_spec(path: pathlib.Path) -> dict:
    spec = json.loads(path.read_text(encoding="utf-8"))
    required = ("rows", "cardinality", "seed", "min_gram", "max_gram")
    missing = [name for name in required if name not in spec]
    if missing:
        raise ValueError(f"missing spec keys: {', '.join(missing)}")
    rows = int(spec["rows"])
    cardinality = int(spec["cardinality"])
    if rows <= 0 or cardinality <= 0 or cardinality > rows:
        raise ValueError("rows/cardinality must satisfy rows >= cardinality > 0")
    return spec


def selected_modes(raw_modes: list[str]) -> tuple[str, ...]:
    modes = tuple(raw_modes or MODES)
    unknown = sorted(set(modes) - set(MODES))
    if unknown:
        raise ValueError(f"unsupported modes: {', '.join(unknown)}")
    if len(set(modes)) != len(modes):
        raise ValueError("modes must not be repeated")
    return modes


def write_dictionary(path: pathlib.Path, cardinality: int, seed: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    width = max(6, len(str(cardinality - 1)))
    with path.open("w", encoding="utf-8", newline="\n") as output:
        for value_id in range(cardinality):
            # Stable fixed-width values keep cardinality exact and make suffix
            # query selectivity predictable without storing generated data.
            output.write(f"seed{seed:08x}-value-{value_id:0{width}d}-xyz\n")


def write_dataset(
    path: pathlib.Path, spec: dict, dictionary_path: pathlib.Path
) -> None:
    content = f"""dataset:
  name: ngram_cardinality_{spec['cardinality']}
  rows: {spec['rows']}
  seed: {spec['seed']}
  dictionaries:
    ngram_values:
      items_file: {json.dumps(str(dictionary_path))}
  fields:
    - name: value
      logical_type: string
      nullable: false
      generator:
        kind: categorical
        values:
          dictionary: ngram_values
          sequential: true
"""
    path.write_text(content, encoding="utf-8")


def write_case(
    path: pathlib.Path,
    dataset_path: pathlib.Path,
    spec: dict,
    mode: str,
) -> None:
    content = f"""version: 1
test_params:
  warmup_iterations: {int(spec.get('warmup_iterations', 1))}
  test_iterations: {int(spec.get('test_iterations', 3))}
  collect_memory_stats: false
  enable_flame_graph: false
preset_datasets:
  - name: card_{spec['cardinality']}
    ref: {json.dumps(str(dataset_path))}
preset_index_configs:
  - name: ngram_{mode}
    field_configs:
      value:
        type: NGRAM
        params:
          min_gram: {json.dumps(str(spec['min_gram']))}
          max_gram: {json.dumps(str(spec['max_gram']))}
          ngram_build_mode: {json.dumps(mode)}
cases:
  - name: ngram_cardinality_{spec['cardinality']}
    suites:
      - name: default
        datasets: [card_{spec['cardinality']}]
        index_configs: [ngram_{mode}]
        expr_templates:
          - name: suffix_all
            expr_template: value like "%xyz"
          - name: contains_value
            expr_template: value like "%value%"
          - name: suffix_missing
            expr_template: value like "%missing"
"""
    path.write_text(content, encoding="utf-8")


def run_mode(
    binary: pathlib.Path,
    scalar_benchmark_root: pathlib.Path,
    milvus_dir: pathlib.Path,
    result_root: pathlib.Path,
    spec: dict,
    mode: str,
) -> None:
    mode_root = result_root / mode
    work_root = mode_root / "work"
    if mode_root.exists():
        shutil.rmtree(mode_root)
    work_root.mkdir(parents=True)

    dictionary_path = work_root / "data" / "values.txt"
    dataset_path = work_root / "ngram_dataset.yaml"
    case_path = work_root / f"ngram_{mode}.yaml"
    write_dictionary(dictionary_path, int(spec["cardinality"]), int(spec["seed"]))
    write_dataset(dataset_path, spec, dictionary_path)
    write_case(case_path, dataset_path, spec, mode)

    artifact_root = work_root / "artifacts"
    env = os.environ.copy()
    env["SCALAR_BENCH_CASES_DIR"] = str(
        scalar_benchmark_root / "benchmark" / "bench_cases"
    )
    env["MILVUS_DIR"] = str(milvus_dir)
    env["SCALAR_BENCH_ARTIFACT_ROOT"] = str(artifact_root)
    env.setdefault("GLOG_minloglevel", "2")
    env.setdefault("MY_LOG_LEVEL", "error")

    completed = subprocess.run(
        [
            str(binary),
            "--config",
            str(case_path),
            "--no-upload",
            "--milvus-repo",
            str(milvus_dir),
        ],
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    (mode_root / "benchmark.log").write_text(completed.stdout, encoding="utf-8")
    if completed.returncode != 0:
        raise RuntimeError(
            f"{mode} benchmark failed with exit {completed.returncode}; "
            f"see {mode_root / 'benchmark.log'}"
        )

    match = RESULT_DIR_RE.search(completed.stdout)
    if not match:
        raise RuntimeError(f"{mode} benchmark did not report its output directory")
    bundle_dir = pathlib.Path(match.group(1)).resolve()
    expected_root = artifact_root.resolve()
    if expected_root not in bundle_dir.parents:
        raise RuntimeError(
            f"{mode} benchmark wrote outside the requested artifact root: "
            f"{bundle_dir}"
        )
    shutil.move(str(bundle_dir), str(mode_root / "bundle"))
    shutil.rmtree(work_root)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    parser.add_argument("--spec", type=pathlib.Path, required=True)
    parser.add_argument(
        "--scalar-benchmark-root",
        type=pathlib.Path,
        default=pathlib.Path(
            os.environ.get("SCALAR_BENCH_ROOT", "~/scalar-benchmark")
        ).expanduser(),
    )
    parser.add_argument(
        "--milvus-dir",
        type=pathlib.Path,
        default=pathlib.Path(__file__).resolve().parents[4],
    )
    parser.add_argument(
        "--results-dir",
        type=pathlib.Path,
        default=pathlib.Path("/tmp/ngram-direct-benchmark"),
    )
    parser.add_argument("--mode", action="append", dest="modes", choices=MODES)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    spec = load_spec(args.spec.resolve())
    modes = selected_modes(args.modes)
    run_root = args.results_dir.resolve() / f"cardinality-{spec['cardinality']}-{int(time.time())}"

    if args.dry_run:
        for mode in modes:
            print(f"{mode}: {args.binary} --config <generated-{mode}.yaml>")
        return 0

    binary = args.binary.resolve()
    scalar_root = args.scalar_benchmark_root.resolve()
    milvus_dir = args.milvus_dir.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError(f"benchmark binary is not executable: {binary}")
    if not (scalar_root / "benchmark" / "bench_cases").is_dir():
        raise ValueError(f"invalid scalar-benchmark root: {scalar_root}")
    if not (milvus_dir / "internal" / "core").is_dir():
        raise ValueError(f"invalid Milvus root: {milvus_dir}")

    run_root.mkdir(parents=True)
    (run_root / "spec.json").write_text(
        json.dumps(spec, indent=2) + "\n", encoding="utf-8"
    )
    for mode in modes:
        print(f"Running {mode} ...", flush=True)
        run_mode(binary, scalar_root, milvus_dir, run_root, spec, mode)

    validator = pathlib.Path(__file__).with_name("validate_results.py")
    subprocess.run([sys.executable, str(validator), str(run_root)], check=True)
    print(f"Results: {run_root}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
