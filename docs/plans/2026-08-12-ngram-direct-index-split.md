# NGRAM Direct Index Branch Split Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Produce stacked A/B branches in zilliztech/tantivy and Milvus, with all sealed Tantivy index builds defaulting to direct single-segment writing in A and experimental memory gating/replay isolated in B.

**Architecture:** Tantivy A provides a stable regular/direct backend abstraction and a production `SingleSegmentIndexWriter`; Milvus A selects direct only for finite sealed builds while retaining regular/background-merge writers for growing Text Match. Tantivy B adds memory estimation, and Milvus B adds auto admission, runtime replay, and benchmark assets.

**Tech Stack:** Rust/Tantivy, C FFI, C++ Milvus segcore/indexbuilder, Go index parameter validation, CMake/Cargo, GTest, scalar-benchmark.

---

### Task 1: Build Tantivy A on current zilliztech main

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/indexer/single_segment_index_writer.rs`

**Step 1: Write failing contract tests**

Add focused tests for batched explicit doc IDs, strictly increasing IDs across batches, final one-segment metadata, panic/error poisoning, maximum-document checks, and preservation of the original `IndexSettings` after finalize.

**Step 2: Verify RED**

Run the focused single-segment writer tests against `origin/main`; confirm they fail because the batch/direct APIs and settings behavior are absent.

**Step 3: Implement the production direct writer**

Port the final non-memory-estimator portions of commits `49bcfd9..16abc01`: direct `SegmentWriter` submission, batched user doc IDs, validation, failure state, finalize lifecycle, futures executor dependency, and restoration of original settings. Do not include finalize-memory estimates or postings byte-size helpers.

**Step 4: Verify GREEN**

Run focused tests and `cargo fmt --check`; then run the relevant Tantivy crate tests.

**Step 5: Commit**

Commit production and required tests with `git commit -s`; no Co-Author trailer.

### Task 2: Build Tantivy B on A

**Files:**
- Modify: `src/indexer/single_segment_index_writer.rs`
- Modify: `src/postings/mod.rs`
- Modify: `src/postings/postings_writer.rs`
- Add only if needed: benchmark/experiment files already present in the prior experimental workspace

**Step 1: Restore estimator tests and verify RED**

Add tests for finalize base-memory estimation and supporting term/postings byte calculations; confirm A lacks the API.

**Step 2: Restore B-only implementation**

Port the `fa26e31` estimator and only the settings-commit test hunks that are specifically about estimation. Keep B APIs unused by Tantivy A.

**Step 3: Verify and commit**

Run focused estimator tests and formatting, then commit with `-s`.

### Task 3: Build Milvus A against Tantivy A

**Files:**
- Modify: `internal/core/thirdparty/tantivy/tantivy-binding/Cargo.toml`
- Regenerate: `internal/core/thirdparty/tantivy/tantivy-binding/Cargo.lock`
- Modify: binding writer modules under `internal/core/thirdparty/tantivy/tantivy-binding/src/`
- Modify: `internal/core/thirdparty/tantivy/tantivy-binding/include/tantivy-binding.h`
- Modify: `internal/core/thirdparty/tantivy/tantivy-wrapper.h`
- Modify: `internal/core/src/index/NgramInvertedIndex.cpp`
- Modify: `internal/core/src/index/InvertedIndexTantivy.cpp/.h`
- Modify: `internal/core/src/index/TextMatchIndex.cpp/.h`
- Modify: JSON key-stat writer construction where sealed-only
- Test: corresponding existing index/binding GTests and Rust tests

**Step 1: Add failing sealed-backend tests**

For NGRAM, scalar inverted, sealed Text Match, and JSON key stats, assert the direct writer is selected, final metadata has exactly one segment, and query/null/doc-ID behavior is unchanged. Add a growing Text Match assertion that regular/background-merge behavior is retained.

**Step 2: Verify RED**

Run focused Rust and C++ tests and confirm regular sealed writers still flush/merge or lack the direct constructor.

**Step 3: Add a clear writer backend API**

Introduce an explicit regular/direct backend parameter through the C ABI and C++ wrapper instead of overloading unrelated compatibility flags. Route finite sealed constructors to direct; keep loading/readers and growing writers unchanged.

**Step 4: Port NGRAM pipeline optimizations**

Port batch FFI, batch channel submission, batch-size tuning, lightweight documents, and direct NGRAM finalization. Keep cross-batch doc-ID validation. Exclude all soft-limit, auto, replay, build-stat, raw-benchmark, and stress code.

**Step 5: Pin Tantivy A and regenerate lockfile**

Update the git revision to Tantivy A and regenerate Cargo.lock mechanically.

**Step 6: Verify GREEN**

Run focused Rust binding tests, Milvus core index GTests, formatting/lint checks, and inspect segment/merge markers.

**Step 7: Commit**

Create signed production commits, with the developer Signed-off-by last and no AI Co-Author.

### Task 4: Build Milvus B on A

**Files:**
- Restore B-only changes in `IndexInfo.h`, `Meta.h`, `NgramInvertedIndex.*`, indexbuilder/config validation, binding status APIs, and B-only tests
- Add: benchmark configs, high-cardinality data generator, run/verification scripts in an experiment directory
- Add: the split design/plan documents
- Update: Tantivy revision to Tantivy B and regenerate Cargo.lock

**Step 1: Add failing gate/replay tests**

Cover preflight direct/regular selection, runtime replay from row zero, finish-time replay, invalid UTF-8 staying a typed error, and stats markers.

**Step 2: Restore auto/gate/replay**

Port the experimental 2 GiB soft limit, conservative estimate, batch/finalize checks, full replay, and diagnostic modes without altering A defaults.

**Step 3: Add benchmark/data assets**

Check in reproducible cardinality-700k/1m generation/config scripts and result validators; keep generated datasets and run artifacts out of git.

**Step 4: Verify and commit**

Run focused tests plus the small benchmark smoke test, then commit all B-only work with `-s`.

### Task 5: Final review, push, and handoff

**Step 1: Static review**

Read `~/.claude/CODE_REVIEW_GUIDE.md`, compare A against current upstream and B against A, skip generated/test implementation details as directed, and fix all current-PR issues.

**Step 2: Fresh verification**

Run the complete focused verification matrix on all four tips and record exact commands/results.

**Step 3: Push branches**

Push Tantivy A/B to zilliztech and Milvus A/B to the user's Milvus fork. Do not create PRs or post GitHub comments.

**Step 4: Handoff**

Report branch URLs, SHAs, stack bases, test evidence, and suggested PR reading order. Tell the user to have Alfred perform PR creation if desired.
