# NGRAM direct-writer experiment assets

This directory is intentionally B-only. It compares the experimental
`regular`, `auto`, and `force_direct` NGRAM build modes at cardinality 700,000
and 1,000,000 without checking generated data or benchmark results into Git.

The runner requires a scalar-benchmark binary built against this Milvus B
worktree. It generates exact-cardinality fixed-width strings, creates one
temporary benchmark case per mode, and validates that every query reports the
same `matched_rows` and `total_rows` before retaining performance numbers.

```bash
python3 internal/core/benchmark/ngram_direct/run_benchmark.py \
  --binary ~/scalar-benchmark/build_benchmark/scalar_filter_bench \
  --scalar-benchmark-root ~/scalar-benchmark \
  --milvus-dir "$PWD" \
  --spec internal/core/benchmark/ngram_direct/configs/ngram_cardinality_700000.json
```

Use the `ngram_cardinality_1000000.json` spec for the one-million-cardinality
run. The default output root is `/tmp/ngram-direct-benchmark`. Pass
`--results-dir` to place generated dictionaries and result bundles elsewhere.
Do not place that directory under the Git worktree.

To inspect commands without generating files or running the benchmark:

```bash
python3 internal/core/benchmark/ngram_direct/run_benchmark.py \
  --binary /path/to/scalar_filter_bench \
  --spec internal/core/benchmark/ngram_direct/configs/ngram_cardinality_700000.json \
  --dry-run
```

Validation can also be rerun independently:

```bash
python3 internal/core/benchmark/ngram_direct/validate_results.py \
  /tmp/ngram-direct-benchmark/cardinality-700000-<timestamp>
```

`SCALAR_BENCH_ARTIFACT_ROOT` is consumed by the small scalar-benchmark patch
in this experiment directory. If the external scalar-benchmark checkout has
not yet applied that patch, apply it before running:

```bash
git -C ~/scalar-benchmark apply \
  "$PWD/internal/core/benchmark/ngram_direct/scalar-benchmark-artifact-root.patch"
```
