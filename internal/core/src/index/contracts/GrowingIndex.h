// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include "index/contracts/ScalarPredicateReader.h"
#include "index/contracts/TextMatchReader.h"

// The Appender face: incremental indexes on growing segments.
//
// See core_refactor/01-scalar-index.md §7 and §7.1.
//
// HOLDER AND LIFETIME (§3 principle 1): held by segcore's growing insert path
// (segcore's `GrowingIndexSet` holds one appender per indexed field and the
// insert path calls `Append` directly); an INTERFACE ON THE INDEX OBJECT;
// LONG-LIVED, CONCURRENT WITH READS.
//
// This is not a future hypothesis. `TextMatchIndex`'s growing constructor
// (commit interval + background merge + `AddTextsGrowing` + `Commit`/`Reload` +
// a reader snapshot, `TextMatchIndex.h:31-37`) is already a growing incremental
// index, and segcore's `ScalarFieldIndexing<T>` (`FieldIndexing.h:141`) is a
// second, parallel mechanism. This face is that pattern extracted into one
// contract.
//
// THREE KEY SEMANTICS:
//
//  1. SNAPSHOT PLUS WATERMARK; NO REAL-TIME PROMISE. tantivy's commit/reload is
//     natively this model. Today text match's commit lag is IMPLICIT; the
//     contract makes it an explicit watermark.
//
//  2. BRIDGING THE UNCOVERED TAIL IS NOT IN THIS CONTRACT. What to do about
//     rows in `[CommittedRows(), insert_barrier)` — not yet in the index — is an
//     execution policy of segcore/exec; the index only reports the watermark.
//     Same principle as §5.5's "the decision belongs to the consumer".
//     The policy is PER FAMILY (text match may lag and is not back-filled;
//     everything else falls back to a column scan), the policy table lives in
//     segcore keyed by family, and THE INDEX SIDE CARRIES NO BIT EXPRESSING IT —
//     otherwise `ReaderCaps` would start carrying product semantics.
//     (Open: which side `NgramReader` and `JsonFlatIndex` fall on, §12.6.)
//
//  3. BUILD-IN-PLACE IS NOT GROWING. Both "build an index inside segcore", but
//     they are different faces: build-in-place is a BUILDER (one-shot,
//     exclusive, ends in an Artifact, input is one fully loaded column — today
//     `generate_interim_index` (`ChunkedSegmentSealedImpl.h:1385`) and
//     `CreateTextIndex`/`CreateTextIndexWithSchema` (`:252,2023`)); growing is an
//     APPENDER (long-lived, concurrent with writes, produces only watermarked
//     snapshots). Conflating them is precisely why `TextMatchIndex` has a
//     growing writer and a sealed build-in-place path inside one class (§2.2).
//     Build-in-place goes through `IndexBuilder<T>::Add`, fed chunk by chunk by
//     the caller — it is NOT another constructor branch of a growing class.
//
// THE ASYMMETRY VERSUS SEALED (§4.3): an appender is not fetched per write; it
// is held for the lifetime of the growing segment. `ReaderSnapshot()` returns A
// DIFFERENT OBJECT: an immutable snapshot created at commit/reload time and
// shared by all concurrent queries through `shared_ptr`. CREATED ONCE PER
// COMMIT, NOT ONCE PER QUERY. So neither side has a per-query proxy: sealed pins
// one long-lived object, growing shares one commit-era snapshot.
//
// ACTIVATED DEFECT — READ BEFORE IMPLEMENTING (§13.1). Landing
// `GrowingScalarIndex` IS the trigger condition for the `size_per_chunk_`
// overrun documented at `Expr.h:2240-2252`: the cached index bitmap is
// segment-global (a scalar index is always a single chunk) while
// `size_per_chunk_` is the raw data's chunk granularity, and today nothing
// reaches that branch on a growing segment because `HasIndex()` is only true
// there for vector/geometry. The same PR that lands growing scalar indexes must
// fix that boundary first, or route growing scalar indexes around that branch.
// See also §13.3: `ByteSize()` returns a cached value that is NOT refreshed
// while a growing segment keeps inserting, yet growing indexes are charged to
// cachinglayer all the same.

namespace milvus::index {

// Declared, not included, ON PURPOSE. `VectorSearchReader` lives in
// VectorFaces.h, which legitimately includes knowhere headers (§12.1(c) rules
// that knowhere types may appear in the vector contract). §10 rule 6 requires
// SCALAR-family contracts to include zero knowhere, and `GrowingScalarIndex<T>`
// lives in this header — so the vector appender's return type is forward
// declared here, and only vector implementations include VectorFaces.h.
class VectorSearchReader;

// The scalar appender.
template <typename T>
class GrowingScalarIndex {
 public:
    virtual ~GrowingScalarIndex() = default;

    // Held exclusively by the insert path; safe concurrently with reads.
    virtual void
    Append(int64_t reserved_offset,
           size_t n,
           const T* values,
           const bool* valid) = 0;

    // The read side takes a snapshot. The snapshot is immutable and covers
    // [0, snapshot->Count()). AN EMPTY RETURN MEANS THERE IS NO READABLE
    // SNAPSHOT YET (the build threshold has not been reached).
    virtual std::shared_ptr<const ScalarPredicateReader<T>>
    ReaderSnapshot() const = 0;

    // The watermark: which row the snapshot covers up to. MONOTONICALLY
    // NON-DECREASING.
    virtual int64_t
    CommittedRows() const = 0;
};

// Same shape, different face on the snapshot.
class GrowingTextIndex {
 public:
    virtual ~GrowingTextIndex() = default;

    virtual void
    Append(int64_t reserved_offset,
           size_t n,
           const std::string_view* values,
           const bool* valid) = 0;

    virtual std::shared_ptr<const TextMatchReader>
    ReaderSnapshot() const = 0;

    virtual int64_t
    CommittedRows() const = 0;
};

// THE VECTOR APPENDER SITS BESIDE THE SCALAR ONE — see §7.1 and §11.3.
//
// The Appender face is NOT scalar-only. Both families implement append today:
// `VectorFieldIndexing::AppendSegmentIndexDense`/`Sparse` (`FieldIndexing.h:
// 281,287`) and `ScalarFieldIndexing::AppendSegmentIndex` (`:171,177`). What is
// wrong today is the SHARED ROOT: `FieldIndexing` (`:51`) is the UNION of both
// families' interfaces — 3 of its 5 pure virtuals are vector-only, 2 are
// scalar-only, and each subclass throws away the other half (`:152,161,183` and
// `:294,303`). That is the same Liskov violation as `IndexBase`, on the growing
// side.
//
// The rule this pins down: THE FOUR FACES ARE UNIFIED ACROSS BOTH FAMILIES;
// THE INTERFACES INSIDE EACH FACE ARE SPLIT BY FAMILY. Faces are cut by CALLER
// (who uses it), families by DATA AND ALGORITHM (what structure) — the two are
// orthogonal. `FieldIndexing`'s error was reading "the two families share a
// face" as "the two families share a set of methods".
//
// The shared SEMANTICS are exactly three, and they hold for both families:
//   - immutable snapshot, shared concurrently (tantivy commit/reload | knowhere
//     interim index generation swap)
//   - a watermark (`CommittedRows()` | today implicit in
//     `sync_data_with_index()`)
//   - no snapshot before the threshold (scalar's threshold is simply always 0,
//     `ScalarFieldIndexing::get_build_threshold()` returns 0 at `:197`; vector
//     reads it from config at `:321`) — absorbed by "an empty snapshot means
//     none yet", so the difference is not a divergence.
//
// WHY dense/sparse STOP BEING TWO METHODS: not because of the signature, but
// because each of those methods is a Builder AND an Appender at once. The
// `!built_` branch (`FieldIndexing.cpp:376-405` / `:518-551`) gathers
// [0, build_threshold) out of the whole `ConcurrentVector` for a
// `BuildWithDataset` — that is COLD-START FULL BUILD, §6.1.1's form B+, and it
// belongs to the Builder face. The `built_` branch (`:578-626`) uses only this
// batch for `AddWithDataset` — that is the Appender. The `const VectorBase*`
// (the whole column) in the signature exists ONLY for the first branch; the
// second branch uses it solely to get this batch's validity
// (`PrepareNullableAppendInfo` at `:84` calls only
// `bulk_is_valid_range(reserved_offset, size, ...)`), which is equivalent to a
// `const bool* valid`. So the migration order is: move the cold-start branch to
// the Builder face first (segcore gathers once from the column when it crosses
// the threshold, feeds `IndexBuilder`, and hands the artifact to the appender as
// its starting point); `Append` then degenerates to the SINGLE method below,
// `VectorBase*` leaves the signature, and dense vs sparse differ only in `dim`.
class GrowingVectorIndex {
 public:
    virtual ~GrowingVectorIndex() = default;

    // `dim` is the only difference between dense and sparse: dense is always the
    // schema dim, sparse is this batch's dim.
    virtual void
    Append(int64_t reserved_offset,
           size_t n,
           const void* data,
           int64_t dim,
           const bool* valid) = 0;

    virtual std::shared_ptr<const VectorSearchReader>
    ReaderSnapshot() const = 0;

    // Empty snapshot while the threshold has not been reached.
    virtual int64_t
    CommittedRows() const = 0;
};

// RETIRING `IndexBase` MUST INCLUDE THE GROWING SIDE (§7.1, §11.2 rule 3).
// `FieldIndexing::get_chunk_indexing` / `get_segment_indexing`
// (`FieldIndexing.h:128,131`) also return `PinWrapper<index::IndexBase*>` — the
// growing side uses `IndexBase` as its type-erased handle too. Those two exits
// are part of the W1 exit checklist; cleaning only the sealed side leaves a
// reference behind. The handle becomes `IndexReaderBase`.

}  // namespace milvus::index
