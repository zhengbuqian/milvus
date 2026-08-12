#!/usr/bin/env python3

import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parent
VALIDATOR_PATH = ROOT / "validate_results.py"
RUNNER_PATH = ROOT / "run_benchmark.py"


def load_validator():
    spec = importlib.util.spec_from_file_location("validate_results", VALIDATOR_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def write_metrics(bundle_dir: pathlib.Path, mode: str, matched_rows: int) -> None:
    case_dir = bundle_dir / mode / "cases" / "case_0"
    case_dir.mkdir(parents=True)
    payload = {
        "tests": [
            {
                "suite_name": "default",
                "dataset": "card_700k",
                "index_config": f"ngram_{mode}",
                "expression": "suffix_long",
                "actual_expression": 'value like "%xyz"',
                "matched_rows": matched_rows,
                "total_rows": 700000,
                "index_build_ms": 123.0,
                "index_size_bytes": 456,
            }
        ]
    }
    (case_dir / "metrics.json").write_text(json.dumps(payload), encoding="utf-8")


class ExperimentAssetTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.validator = load_validator()
        spec = importlib.util.spec_from_file_location("run_benchmark", RUNNER_PATH)
        cls.runner = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(cls.runner)

    def test_cardinality_specs_cover_700k_and_1m(self):
        for cardinality in (700000, 1000000):
            path = ROOT / "configs" / f"ngram_cardinality_{cardinality}.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["rows"], cardinality)
            self.assertEqual(payload["cardinality"], cardinality)
            self.assertEqual(payload["seed"], 42)
            self.assertEqual(payload["min_gram"], 3)
            self.assertEqual(payload["max_gram"], 4)

    def test_runner_dry_run_lists_all_modes_without_creating_results(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            result_root = pathlib.Path(temp_dir) / "results"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(RUNNER_PATH),
                    "--binary",
                    "/does/not/need/to/exist",
                    "--spec",
                    str(ROOT / "configs" / "ngram_cardinality_700000.json"),
                    "--results-dir",
                    str(result_root),
                    "--dry-run",
                ],
                check=True,
                text=True,
                capture_output=True,
            )
            self.assertIn("regular", completed.stdout)
            self.assertIn("auto", completed.stdout)
            self.assertIn("force_direct", completed.stdout)
            self.assertFalse(result_root.exists())

    def test_generated_inputs_have_exact_cardinality_and_all_modes(self):
        spec = {
            "rows": 5,
            "cardinality": 5,
            "seed": 42,
            "min_gram": 3,
            "max_gram": 4,
            "warmup_iterations": 1,
            "test_iterations": 1,
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = pathlib.Path(temp_dir)
            dictionary = temp_root / "values.txt"
            dataset = temp_root / "dataset.yaml"
            self.runner.write_dictionary(dictionary, 5, 42)
            self.runner.write_dataset(dataset, spec, dictionary)
            values = dictionary.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(values), 5)
            self.assertEqual(len(set(values)), 5)
            self.assertTrue(all(value.endswith("-xyz") for value in values))
            self.assertIn("sequential: true", dataset.read_text(encoding="utf-8"))
            for mode in self.runner.MODES:
                case = temp_root / f"{mode}.yaml"
                self.runner.write_case(case, dataset, spec, mode)
                case_text = case.read_text(encoding="utf-8")
                self.assertIn(f'ngram_build_mode: "{mode}"', case_text)

    def test_scalar_benchmark_patch_applies_to_current_checkout(self):
        checkout = pathlib.Path.home() / "scalar-benchmark"
        if not (checkout / ".git").exists():
            self.skipTest("~/scalar-benchmark checkout is unavailable")
        completed = subprocess.run(
            [
                "git",
                "-C",
                str(checkout),
                "apply",
                "--check",
                str(ROOT / "scalar-benchmark-artifact-root.patch"),
            ],
            text=True,
            capture_output=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_validator_accepts_identical_results_across_modes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            result_root = pathlib.Path(temp_dir)
            for mode in self.validator.MODES:
                write_metrics(result_root, mode, matched_rows=7)
            summary = self.validator.validate_result_root(result_root)
            self.assertEqual(summary["row_count"], 700000)
            self.assertEqual(summary["query_count"], 1)

    def test_validator_rejects_mismatched_results(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            result_root = pathlib.Path(temp_dir)
            write_metrics(result_root, "regular", matched_rows=7)
            write_metrics(result_root, "auto", matched_rows=8)
            write_metrics(result_root, "force_direct", matched_rows=7)
            with self.assertRaisesRegex(ValueError, "matched_rows"):
                self.validator.validate_result_root(result_root)


if __name__ == "__main__":
    unittest.main()
