# NGRAM Batched Single-Segment Build Design

Date: 2026-07-29

## Goal

Reduce sealed NGRAM index build time by removing four independent costs while preserving term-to-row postings and query behavior:

1. one C++ to Rust FFI call and one heap-backed `RustResultWrapper` per row;
2. one Tantivy channel send per row;
3. one default `TantivyDocument` allocation per row;
4. the finish-time multi-segment merge introduced by `4955fcd`.

The work is NGRAM-only. Generic scalar, text, JSON-key-stats, and growing-index writers are out of scope.

## Baseline

The current path is:

```text
C++ FieldData
  -> one tantivy_index_add_string FFI call per row
  -> one RustResultWrapper allocation per call
  -> one TantivyDocument::default() per row
  -> one IndexWriter::add_document_with_doc_id call per row
  -> one async-channel message per row
  -> Tantivy tokenizer and postings writer
  -> auto-flushed segments
  -> synchronous finish-time merge-all
```

The `like_suffix/card_100` workload expands one million input strings into about 11.9 million NGRAM postings. With the current 15 MiB writer arena it produces two segments, and the 2-to-1 merge costs about 3.18 seconds.

## Cross-Repository Architecture

### `zilliztech/tantivy`

Extend the pinned V7 `SingleSegmentIndexWriter` instead of copying private `SegmentWriter` or `save_metas` logic into Milvus.

New behavior:

- channel messages carry a batch of `AddOperation<D>` rather than one `D`;
- accept batches with explicit, strictly increasing document IDs;
- preserve sparse document IDs;
- process every operation in a batch against the same `SegmentWriter`;
- propagate worker, channel, document-ID, and finalize errors;
- continue to finalize exactly one searchable segment;
- expose memory usage so Milvus can enforce an explicit peak-memory policy.

The existing single-document API remains as a thin one-element-batch wrapper inside the fork. Milvus will consume a new Tantivy git commit through `Cargo.toml`/`Cargo.lock`.

### Milvus Tantivy binding

Add an NGRAM-specific V7 writer variant rather than making the generic V5/V7 writer hierarchy generic over a new document type.

```text
IndexWriterWrapper::NgramV7(NgramIndexWriterWrapperImpl)
```

The NGRAM writer owns either the current regular `IndexWriter` or the optimized `SingleSegmentIndexWriter` during staged benchmarking. Production-final mode uses the optimized single-segment writer.

### Lightweight NGRAM document

Replace per-row `TantivyDocument` construction with an NGRAM-only `Document` implementation.

```text
NgramBatchData
  flat UTF-8 byte buffer
  per-value byte ranges
  per-document value ranges

NgramDocument
  Arc<NgramBatchData>
  document index
```

The document iterator returns zero or one string field for the current NGRAM use cases. A null or missing value returns no field; a valid empty string returns one zero-length string. The shared `Arc` makes the data `Send + Sync + 'static` while allocating string storage once per batch instead of once per row.

Tantivy may still perform small per-document allocations inside `SegmentWriter`; this design specifically removes the default `CompactDoc` allocations visible in the current profile.

## C ABI

Add a stable NGRAM-only batch entry point that receives original strings, never precomputed grams:

```text
tantivy_ngram_add_strings_batch(
    writer,
    string_ptrs,
    string_lengths,
    document_ids,
    validity,
    count)
```

Properties:

- pointer plus length preserves embedded NUL bytes;
- invalid rows may use a null pointer with length zero;
- Rust copies valid data into a Rust-owned batch buffer before returning;
- invalid UTF-8 reports the logical document ID;
- document IDs must be non-negative, fit `u32`, and be strictly increasing;
- one FFI call produces one `RustResult`, removing per-row `RustResultWrapper` allocation.

NGRAM tokenization remains entirely inside Tantivy. No gram crosses the FFI boundary.

## C++ data preparation

`NgramInvertedIndex` collects one logical entry per row across all `FieldData` chunks before calling the batch FFI.

- String/VarChar: preserve row order and validity.
- JSON: preserve explicit row IDs for values, null, non-existent paths, and cast errors; skipped values remain sparse IDs rather than silently renumbering rows.
- `avg_row_size` is calculated during the same collection pass, removing the current extra scan.

The vectors hold views only until the FFI call returns. Rust never retains C++ pointers.

## Staged Performance Attribution

Each stage is cumulative and retained as a signed local commit for reproducibility.

### B0: Baseline

Current master: per-row FFI, `TantivyDocument`, per-row channel send, regular writer, finish-time merge-all.

### S1: Batch FFI

Use one NGRAM batch FFI call, but Rust deliberately loops through the current per-row document/send path.

Expected removals:

- per-row FFI boundary;
- per-row C++ `RustResultWrapper` allocation.

### S2: Batch channel

Build a `Vec<TantivyDocument>` and call the batch writer API once per bounded chunk.

Expected removal:

- per-row channel send, lock, block-on, and wakeup.

The default document allocation remains, isolating channel batching.

### S3: Lightweight document

Replace `TantivyDocument` with `NgramDocument` and shared batch storage.

Expected removal:

- per-row `CompactDoc::with_capacity` and field-vector allocations.

### S4: Optimized single segment

Switch the NGRAM writer to the enhanced Tantivy `SingleSegmentIndexWriter` batch API.

Expected removal:

- auto-flushed intermediate segments;
- finish-time postings merge and rewrite.

## Memory Policy

A direct single segment holds the segment state until finalize, so the 15 MiB argument is not a hard memory cap. Production code must not imply otherwise.

The implementation will:

- expose writer memory usage from Tantivy;
- record estimated batch-owned bytes and writer memory in logs;
- reject a build before unsafe growth when a configurable NGRAM direct-build limit is exceeded;
- keep the existing regular-writer/merge path available as an explicit fallback, not a silent behavior change;
- benchmark both `card_100` and a larger stress dataset before enabling the direct path by default.

The final default decision will be based on measured wall time and peak RSS. If the stress gate is unsafe, the direct writer remains guarded while S1-S3 can still ship independently.

## Error Handling

- Rust APIs return `Result`; no `expect` or panic is added.
- Channel close, worker panic, worker error, invalid UTF-8, invalid document order, and finalize errors propagate through the existing `RustResult` ABI.
- Milvus adds context without changing the underlying error category.
- The writer is consumed exactly once at finish, preserving the current dangling-pointer protection in the C++ wrapper.

## Correctness Tests

### Tantivy fork

- single and batch adds produce identical postings;
- explicit sparse IDs survive finalize;
- duplicate, descending, and overflowing IDs fail;
- empty batch and one-element batch work;
- channel close and worker failure propagate;
- final metadata contains exactly one segment.

### Rust binding

- batch FFI matches the old per-row path;
- embedded NUL and multi-byte UTF-8 work;
- invalid UTF-8 identifies the logical row;
- C++ input lifetime is not retained;
- batch sizes around the selected boundary work;
- lightweight and default documents produce identical terms/postings.

### Milvus C++

- String/VarChar, nullable rows, consecutive and trailing nulls;
- multiple `FieldData` chunks;
- JSON value, null, missing path, and cast error;
- LIKE suffix long/medium/short/single-character results equal brute force;
- upload, reload, and query after reopening from disk;
- `Count`, maximum document ID, null offsets, and final segment count.

## Benchmark Method

For B0 and S1-S4:

- use the same scalar-benchmark ELF and `ngram_card100_buildonly.yaml`;
- use release builds with debug symbols and the same runtime libraries;
- explicitly clean and rebuild the Tantivy target at every stage because the current CMake custom command lacks Rust-source dependencies;
- run one discarded warmup and at least eleven measured iterations in an interleaved order;
- report median, p25/p75, MAD, adjacent-stage delta, total delta, index size, segment count, and max RSS;
- capture one representative `perf cpu-clock:u` profile per stage;
- profile runs do not contribute to latency statistics;
- run the full LIKE query validation and a larger memory-stress dataset.

The historical batch-add regression in issue `#41375` makes query-latency stability a required gate, not an optional follow-up.

## Deliverables

- one Tantivy fork branch with batch/document-ID `SingleSegmentIndexWriter` support;
- one Milvus branch with S1-S4 as cumulative signed commits;
- unit and integration tests;
- exact benchmark table and representative profiles/flamegraphs;
- an explicit recommendation on which stages are safe to enable by default.
