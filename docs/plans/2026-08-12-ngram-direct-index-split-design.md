# NGRAM Direct Index Branch Split Design

## Goal

Split the existing Milvus/Tantivy NGRAM optimization work into a stack of production-ready A branches and experiment-only B branches, with `master/main -> A -> B` in each repository.

## Confirmed scope

Branch A contains the production direct-writer implementation and required correctness tests. In Milvus, every sealed Tantivy-backed scalar/text index build uses the direct single-segment interface by default; growing Text Match writers keep their existing regular writer behavior. NGRAM, ordinary scalar inverted, sealed Text Match, and sealed JSON key-stat/index paths are included where their writer APIs support the direct backend.

Branch B is based on A and contains the experimental memory admission gate, the `regular/auto/force_direct` selection modes, runtime direct-to-regular full replay, memory estimation helpers, high-cardinality/stress benchmark hooks, data-generation/configuration scripts, and profiling artifacts. B is not intended for the production PR.

## Repository topology

```text
Milvus:  origin/master -> codex/ngram-direct-a -> codex/ngram-direct-b
Tantivy: origin/main   -> codex/ngram-direct-a -> codex/ngram-direct-b
```

The Tantivy A tip must be pinned by Milvus A. Milvus B updates the pin to the Tantivy B tip. Each branch gets its own regenerated Cargo.lock.

## Tantivy A

Port the stable `SingleSegmentIndexWriter` implementation, batched document API, strict increasing user-doc-ID validation, worker/error-state handling, executor dependency, and settings preservation. The A API must expose a clear sealed direct-writer constructor while retaining the regular constructor for growing/background-merge users. Required tests cover one-segment finalization, document-ID contracts, error propagation, and settings preservation. The finalize-memory estimator and its supporting postings size helpers remain in B.

## Milvus A

Extend the Tantivy C/C++ binding and `TantivyIndexWrapper` so sealed constructors for NGRAM, ordinary scalar inverted, sealed Text Match, and sealed JSON key-stat/index writers select the direct backend by default. Keep growing Text Match on the existing regular writer with background merge. Preserve null/doc-ID semantics and reader compatibility. Required tests assert one final segment, zero merge for sealed direct builds, correct query/posting results, null offsets, and unchanged growing behavior.

## Tantivy B

Add finalize-memory accounting and posting-size helpers needed by the experimental gate, plus their unit tests and any benchmark-only hooks. No B-only API should be required by A.

## Milvus B

Add configurable `ngram_build_mode`, soft-limit preflight estimation, runtime checks, direct-to-regular full replay, structured build statistics, stress/high-cardinality benchmark hooks, raw-data UT entry points, benchmark configs/data generators, and validation scripts. B may pin Tantivy B, but A must remain independently buildable against Tantivy A.

## Verification

For Tantivy A/B: run focused Rust unit tests for the single-segment writer and memory estimator, then the relevant workspace checks. For Milvus A/B: run focused index/binding tests, verify sealed NGRAM/inverted/Text Match/JSON outputs contain one segment and no merge markers, and run the existing scalar benchmark only on B. Perform a static diff review against the corresponding upstream base before pushing; do not create PRs in this task.
