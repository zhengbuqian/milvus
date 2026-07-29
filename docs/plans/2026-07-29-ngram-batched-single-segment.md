# NGRAM Batched Single-Segment Build Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build sealed NGRAM indexes with batched FFI, batched document submission, a lightweight document representation, and an optimized Tantivy single-segment writer, then quantify each cumulative improvement.

**Architecture:** Extend `zilliztech/tantivy` so `SingleSegmentIndexWriter` accepts explicit document-ID batches and sends one channel message per batch. In Milvus, add an NGRAM-only batch C ABI and writer variant, first retaining the default document for attribution, then replacing it with a shared-buffer `NgramDocument`, and finally switching to the enhanced single-segment writer. Keep every optimization as a signed cumulative commit and benchmark every stage against the same current-master baseline.

**Tech Stack:** Rust, Tantivy, C ABI/cbindgen, C++17, GoogleTest, Cargo tests, CMake/Ninja, sccache, scalar-benchmark, Linux perf.

---

## Global constraints

- All shell commands start with `rtk`.
- Milvus branch: `codex/ngram-batched-single-segment`.
- Tantivy branch: `codex/ngram-single-segment-batch`.
- Every commit uses `git commit -s`; never add an AI Co-Author.
- Do not edit or reset existing untracked files in `/home/zilliz/scalar-benchmark`.
- Do not create a GitHub PR or issue, and do not push either repository without explicit user approval. Local development and all stage benchmarks use the local Tantivy worktree.
- Every Rust source change requires an explicit Tantivy target clean before the C++ benchmark build because `internal/core/thirdparty/tantivy/CMakeLists.txt` does not track Rust source dependencies.
- Production code follows TDD: write the test, run it and observe the expected failure, then implement.

## Task 1: Prepare the Tantivy development worktree

**Files:**
- Worktree: `/tmp/kilo/codex-ngram-tantivy`
- Base commit: `96f3335ab5f061926c5b44cf246e81243e1dedc5`

**Step 1: Clone the real repository with the Cargo mirror as an object reference**

Run:

```bash
rtk git clone --reference-if-able /home/zilliz/.cargo/git/db/tantivy-8e3af7eccc01f54a https://github.com/zilliztech/tantivy.git /tmp/kilo/codex-ngram-tantivy
```

Expected: clone succeeds under the configured `/tmp/kilo` sccache base.

**Step 2: Create the local branch at the exact pinned revision**

Run:

```bash
rtk git -C /tmp/kilo/codex-ngram-tantivy switch --detach 96f3335ab5f061926c5b44cf246e81243e1dedc5
rtk git -C /tmp/kilo/codex-ngram-tantivy switch -c codex/ngram-single-segment-batch
```

Expected: clean branch at `96f3335`.

**Step 3: Run the existing single-segment tests**

Run:

```bash
rtk env RUSTC_WRAPPER=sccache cargo +1.89 test --manifest-path /tmp/kilo/codex-ngram-tantivy/Cargo.toml single_segment_index_writer -- --nocapture
```

Expected: baseline tests pass.

## Task 2: Add explicit document-ID batches to Tantivy SingleSegmentIndexWriter

**Files:**
- Modify: `/tmp/kilo/codex-ngram-tantivy/src/indexer/single_segment_index_writer.rs`
- Test: `/tmp/kilo/codex-ngram-tantivy/src/indexer/single_segment_index_writer.rs`

**Step 1: Write failing tests for sparse IDs and batch finalization**

Add tests that build a schema with `enable_user_specified_doc_id()`, submit documents with IDs `[0, 2, 5]`, finalize, reopen, and assert postings return exactly those IDs and metadata has one segment.

The wished-for API is:

```rust
writer.add_documents_with_doc_ids(vec![(0, doc0), (2, doc2), (5, doc5)])?;
```

Also add one test asserting duplicate or descending IDs fail with `InvalidArgument`.

**Step 2: Run the tests and verify RED**

Run:

```bash
rtk env RUSTC_WRAPPER=sccache cargo +1.89 test --manifest-path /tmp/kilo/codex-ngram-tantivy/Cargo.toml single_segment_index_writer -- --nocapture
```

Expected: compile failure because `add_documents_with_doc_ids` does not exist.

**Step 3: Change the channel message to a document batch**

Implement an internal batch type:

```rust
type AddBatch<D> = Vec<AddOperation<D>>;
```

Change the writer channel from `Sender<D>` to `Sender<AddBatch<D>>`. The worker receives one batch and loops through its operations, awaiting `segment_writer.add_document(operation)` for each item.

**Step 4: Add strict ID validation and the public batch API**

Store `last_doc_id_plus_one` in the writer. Implement:

```rust
pub fn add_documents_with_doc_ids<I>(&mut self, documents: I) -> crate::Result<()>
where
    I: IntoIterator<Item = (u32, D)>,
    I::IntoIter: ExactSizeIterator;
```

Requirements:

- empty batch succeeds;
- IDs are strictly increasing but may be sparse;
- every `AddOperation` carries `doc_id: Some(id)`;
- opstamps remain monotonic within the writer;
- `add_document(document)` remains a one-element batch with `doc_id: None` for non-user-ID schemas.

**Step 5: Run the tests and verify GREEN**

Run the command from Step 2.

Expected: all targeted tests pass.

**Step 6: Commit the Tantivy stage**

Run:

```bash
rtk git -C /tmp/kilo/codex-ngram-tantivy add src/indexer/single_segment_index_writer.rs
rtk git -C /tmp/kilo/codex-ngram-tantivy commit -s -m 'enhance: batch single segment document writes'
```

## Task 3: Propagate worker errors and expose memory usage

**Files:**
- Modify: `/tmp/kilo/codex-ngram-tantivy/src/indexer/single_segment_index_writer.rs`
- Test: `/tmp/kilo/codex-ngram-tantivy/src/indexer/single_segment_index_writer.rs`

**Step 1: Write failing tests**

Add tests for:

- send after a worker failure returns an error;
- finalize propagates the worker error rather than replacing it with a generic message;
- `mem_usage()` becomes non-zero after a batch is processed.

Use a small polling helper with a bounded deadline for the asynchronous memory observation; do not use an arbitrary sleep as the correctness condition.

**Step 2: Verify RED**

Run the targeted Cargo test command.

Expected: the new error/memory assertions fail.

**Step 3: Implement shared writer state**

Add shared state for:

- latest segment-writer memory usage;
- the first worker error;
- worker-alive/closed status.

Update memory usage after each processed batch. Preserve the concrete Tantivy error where possible.

**Step 4: Verify GREEN and run the full Tantivy indexer tests**

Run:

```bash
rtk env RUSTC_WRAPPER=sccache cargo +1.89 test --manifest-path /tmp/kilo/codex-ngram-tantivy/Cargo.toml indexer -- --nocapture
```

Expected: all indexer tests pass.

**Step 5: Commit**

```bash
rtk git -C /tmp/kilo/codex-ngram-tantivy add src/indexer/single_segment_index_writer.rs
rtk git -C /tmp/kilo/codex-ngram-tantivy commit -s -m 'fix: propagate single segment worker state'
```

## Task 4: Capture the current Milvus NGRAM baseline (B0)

**Files:**
- Config: `/home/zilliz/.codex/visualizations/2026/07/29/019fabe3-ea6f-7b80-b68a-29c83d352a4c/ngram_card100_buildonly.yaml`
- Create artifacts under: `/home/zilliz/.codex/visualizations/2026/07/29/ngram-build-stages/b0-baseline/`

**Step 1: Build current Milvus HEAD in an isolated build directory**

Use source `/home/zilliz/.codex/worktrees/01c5/milvus` and build directory `/tmp/kilo/codex-ngram-opt-build` with `RUSTC_WRAPPER=sccache` and compiler launchers set to sccache. Build release-with-debug-symbols `libmilvus_core.so` and the Tantivy target.

**Step 2: Record provenance**

Record:

- Milvus commit;
- scalar-benchmark commit and ELF build ID;
- `libmilvus_core.so` build ID;
- `libtantivy_binding.a` SHA-256;
- `ldd` output;
- exact YAML hash.

**Step 3: Run baseline measurements**

Run one discarded warmup and eleven measured executions pinned with `taskset -c 0-5`. Capture `Index built in`, segment count, serialized size, and `/usr/bin/time -v` max RSS.

**Step 4: Capture one representative perf profile**

Use `cpu-clock:u`, 999 Hz, DWARF 16 KiB, exact benchmark build markers, and verify zero lost samples and 100% folded coverage.

## Task 5: Stage S1 — NGRAM batch FFI only

**Files:**
- Modify: `internal/core/thirdparty/tantivy/tantivy-binding/src/index_writer_c.rs`
- Modify/generated: `internal/core/thirdparty/tantivy/tantivy-binding/include/tantivy-binding.h`
- Modify: `internal/core/thirdparty/tantivy/tantivy-wrapper.h`
- Modify: `internal/core/src/index/NgramInvertedIndex.cpp`
- Modify: `internal/core/src/index/NgramInvertedIndex.h`
- Test: `internal/core/thirdparty/tantivy/tantivy-binding/src/index_writer.rs`
- Test: `internal/core/src/index/NgramInvertedIndexTest.cpp`

**Step 1: Write failing Rust tests for the batch C/Rust boundary helper**

Add an internal safe helper behind the FFI function and test:

- multiple strings with non-zero start IDs;
- null/invalid entries;
- embedded NUL;
- UTF-8;
- invalid UTF-8 returns an error containing the row ID;
- negative/overflowing IDs are rejected before conversion.

The stage-S1 helper deliberately calls the existing per-row `writer.add()` internally.

**Step 2: Verify RED**

Run:

```bash
rtk env RUSTC_WRAPPER=sccache cargo +1.89 test --manifest-path internal/core/thirdparty/tantivy/tantivy-binding/Cargo.toml ngram_batch -- --nocapture
```

Expected: tests fail because the helper does not exist.

**Step 3: Implement the batch ABI**

Add a C ABI accepting pointer, length, document-ID, and validity arrays. Return one `RustResult` for the whole batch. Do not retain input pointers.

**Step 4: Add the C++ batch wrapper**

Add an NGRAM-only method that constructs pointer/length/ID/validity arrays and calls the new FFI once. This creates one `RustResultWrapper` per batch, not per row.

**Step 5: Collect String/VarChar and JSON rows once**

Refactor `NgramInvertedIndex` so the same pass:

- collects row views and explicit IDs;
- records validity/missing values;
- computes `avg_row_size`.

For JSON, preserve the existing null-offset behavior and explicit row IDs.

**Step 6: Verify GREEN**

Run the Rust test from Step 2 and the NGRAM C++ tests:

```bash
rtk /tmp/kilo/codex-ngram-opt-build/unittest/all_tests --gtest_filter='*Ngram*'
```

Expected: both pass.

**Step 7: Commit S1**

```bash
rtk git add internal/core/thirdparty/tantivy internal/core/src/index/NgramInvertedIndex.cpp internal/core/src/index/NgramInvertedIndex.h internal/core/src/index/NgramInvertedIndexTest.cpp
rtk git commit -s -m 'enhance: batch ngram string ffi'
```

**Step 8: Build and benchmark S1**

Explicitly clean the Tantivy target, rebuild, run the same eleven-iteration protocol, and capture one profile under `s1-batch-ffi/`.

## Task 6: Stage S2 — Batch channel with default TantivyDocument

**Files:**
- Modify: `internal/core/thirdparty/tantivy/tantivy-binding/src/index_writer_v7/index_writer.rs`
- Modify: `internal/core/thirdparty/tantivy/tantivy-binding/src/index_writer.rs`
- Modify: `internal/core/thirdparty/tantivy/tantivy-binding/src/index_writer_c.rs`
- Test: `internal/core/thirdparty/tantivy/tantivy-binding/src/index_writer.rs`

**Step 1: Write a failing batch-postings equivalence test**

Build one index through the S1 per-row Rust loop and another through a wished-for bounded-batch method using `Vec<TantivyDocument>` and `add_documents_with_doc_id`. Compare every NGRAM term posting list and document count.

**Step 2: Verify RED**

Run the targeted Rust test and confirm the bounded-batch method is absent.

**Step 3: Implement bounded document batches**

Use a configurable internal batch size beginning at 512. Construct default `TantivyDocument`s, submit each batch once, and preserve explicit contiguous row IDs within each batch. For sparse JSON IDs, end the current batch at a gap and start the next batch at the next ID.

**Step 4: Verify GREEN and edge batch sizes**

Test sizes `0`, `1`, `511`, `512`, and `513`, plus multiple batches and a document-ID gap.

**Step 5: Commit S2**

```bash
rtk git add internal/core/thirdparty/tantivy
rtk git commit -s -m 'enhance: batch ngram document submission'
```

**Step 6: Benchmark S2**

Measure candidate batch sizes 256, 512, and 1024 using the same build and pinning. Select the best wall/RSS trade-off, record the choice, then capture the canonical eleven-run and perf artifacts under `s2-batch-channel/`.

## Task 7: Stage S3 — Lightweight shared-buffer NGRAM document

**Files:**
- Create: `internal/core/thirdparty/tantivy/tantivy-binding/src/index_ngram_document.rs`
- Modify: `internal/core/thirdparty/tantivy/tantivy-binding/src/lib.rs`
- Modify: `internal/core/thirdparty/tantivy/tantivy-binding/src/index_ngram_writer.rs`
- Modify: `internal/core/thirdparty/tantivy/tantivy-binding/src/index_writer.rs`
- Test: `internal/core/thirdparty/tantivy/tantivy-binding/src/index_ngram_document.rs`

**Step 1: Write failing custom-Document tests**

Define the intended behavior:

- valid text yields exactly one `(Field, Str)` value;
- valid empty string yields one empty value;
- null/missing yields no values;
- cloned documents share the same backing `Arc`;
- dropping the input pointer source before indexing is safe.

**Step 2: Verify RED**

Run the targeted Cargo test and confirm the new module/type is absent.

**Step 3: Implement `NgramBatchData` and `NgramDocument`**

Use one flat byte buffer and compact range metadata per batch. Implement Tantivy `Value` and `Document` without `TantivyDocument::default()`.

**Step 4: Add an NGRAM-specific writer variant**

Add:

```rust
IndexWriterWrapper::NgramV7(NgramIndexWriterWrapperImpl)
```

The variant owns `IndexWriter<NgramDocument>` for S3 and implements only the operations NGRAM requires: create reader, add batch, commit/finish, and logging.

**Step 5: Verify postings equivalence**

Compare the S2 default-document index and S3 lightweight-document index term by term, including null, empty, UTF-8, and sparse ID cases.

**Step 6: Commit S3**

```bash
rtk git add internal/core/thirdparty/tantivy
rtk git commit -s -m 'enhance: use lightweight ngram documents'
```

**Step 7: Benchmark S3**

Run the canonical measurement/profile protocol under `s3-lightweight-document/`. Verify `CompactDoc::with_capacity` disappears from the NGRAM frontend profile.

## Task 8: Stage S4 — Pin and use the optimized Tantivy SingleSegmentIndexWriter

**Files:**
- Modify: `internal/core/thirdparty/tantivy/tantivy-binding/Cargo.toml`
- Modify: `internal/core/thirdparty/tantivy/tantivy-binding/Cargo.lock`
- Modify: `internal/core/thirdparty/tantivy/tantivy-binding/src/index_ngram_writer.rs`
- Modify: `internal/core/thirdparty/tantivy/tantivy-binding/src/index_writer.rs`
- Test: `internal/core/thirdparty/tantivy/tantivy-binding/src/index_ngram_writer.rs`

**Step 1: Use the local Tantivy commit without pushing**

Point the development build at the local Tantivy git worktree or a temporary local path override. Keep that override out of the portable Milvus commit. Record the immutable local Tantivy commit SHA. After all measurements are complete, ask the user separately whether the Tantivy branch should be pushed and the Milvus dependency pinned to a remotely fetchable revision.

**Step 2: Verify the local dependency**

Use a local `file://` git URL or Cargo path override in an uncommitted development edit and run the Rust binding tests. Revert the local URL before any portable Milvus commit.

**Step 3: Write a failing single-segment NGRAM test**

Use enough data to exceed the normal 15 MiB flush threshold. Assert:

- explicit/sparse row IDs are preserved;
- final searchable segment count is exactly one;
- there is no merge log/event;
- postings equal the S3 regular-writer result.

**Step 4: Verify RED against the old Tantivy revision**

Expected: the required batch single-segment API is unavailable.

**Step 5: Pin the new Tantivy revision and switch the NGRAM writer**

Update the V7 `tantivy` git revision and regenerate `Cargo.lock`. Use the optimized `SingleSegmentIndexWriter<NgramDocument>` in the NGRAM-specific writer.

**Step 6: Implement the direct-build memory gate**

Track batch-owned bytes and Tantivy writer memory. If the configured direct-build limit would be exceeded, select the explicit S3 regular-writer fallback before submitting data. Log the selected mode and memory estimate.

**Step 7: Verify GREEN**

Run:

```bash
rtk env RUSTC_WRAPPER=sccache cargo +1.89 test --manifest-path internal/core/thirdparty/tantivy/tantivy-binding/Cargo.toml ngram -- --nocapture
rtk /tmp/kilo/codex-ngram-opt-build/unittest/all_tests --gtest_filter='*Ngram*'
```

Expected: all pass and direct mode produces one segment.

**Step 8: Commit S4**

```bash
rtk git add internal/core/thirdparty/tantivy
rtk git commit -s -m 'enhance: build ngram indexes as one segment'
```

**Step 9: Benchmark S4**

Run the canonical protocol under `s4-single-segment/`. Verify the merge-only hotspot is absent and report peak RSS.

## Task 9: Query and storage correctness gate

**Files:**
- Modify: `internal/core/src/index/NgramInvertedIndexTest.cpp`
- Config: `/home/zilliz/.codex/visualizations/2026/07/29/019fabe3-ea6f-7b80-b68a-29c83d352a4c/ngram_card100.yaml`

**Step 1: Add missing C++ regression coverage**

Add tests for multiple `FieldData` chunks, trailing null, JSON missing/error, and reopen-after-upload. Verify optimized and brute-force results.

**Step 2: Run the full NGRAM C++ suite**

Run the NGRAM gtest filter with the required runtime library path.

**Step 3: Run scalar LIKE validation**

Run the full query config with `no_index` and NGRAM. Compare matched row counts for suffix long/medium/short/single-character and record p50/p99 stability.

**Step 4: Run a larger memory-stress build**

Use the larger-cardinality NGRAM configuration. Record whether direct mode or fallback is selected, max RSS, build time, and final segment count.

**Step 5: Commit any test-only additions**

```bash
rtk git add internal/core/src/index/NgramInvertedIndexTest.cpp
rtk git commit -s -m 'test: cover batched ngram index builds'
```

## Task 10: Final analysis and documentation

**Files:**
- Create: `/home/zilliz/.codex/visualizations/2026/07/29/ngram-build-stages/stage_results.tsv`
- Create: `/home/zilliz/.codex/visualizations/2026/07/29/ngram-build-stages/report.md`
- Update if needed: `docs/plans/2026-07-29-ngram-batched-single-segment-design.md`

**Step 1: Produce the cumulative table**

Include B0 and S1-S4 median, p25/p75, MAD, adjacent delta, total delta, max RSS, index size, and segment count.

**Step 2: Compare profiles**

Confirm expected hotspot removal at each stage:

- S1: per-row FFI/result allocation;
- S2: per-row channel/block-on;
- S3: `CompactDoc::with_capacity`;
- S4: merger thread, heap merge, and merge memcpy.

**Step 3: State the production recommendation conservatively**

Enable only stages that pass correctness, query stability, and memory gates. Do not claim direct single-segment safety for workloads not exercised.

**Step 4: Commit repository documentation updates**

Use `git commit -s`; keep the developer Signed-off-by last.

## Task 11: Review and final verification

**Step 1: Read the mandatory code-review guide**

Before any code review, read `/home/zilliz/.claude/CODE_REVIEW_GUIDE.md` completely and follow it.

**Step 2: Review both repository diffs**

Review Tantivy first, then Milvus, including error paths, memory growth, doc-ID invariants, and generated header consistency.

**Step 3: Run final fresh verification**

At minimum:

- Tantivy targeted and indexer tests;
- Rust binding NGRAM tests;
- Milvus NGRAM C++ tests;
- scalar LIKE correctness run;
- exact stage benchmark checksums and profile coverage;
- `git diff --check` and clean status in both repositories.

**Step 4: Report exact commit SHAs and artifacts**

Do not create a PR. If a PR is later requested, hand that GitHub-side action to Alfred.
