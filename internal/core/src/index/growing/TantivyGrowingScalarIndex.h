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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/Types.h"
#include "index/contracts/GrowingIndex.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/ScalarPredicateReader.h"
#include "index/growing/GrowingAppenderBase.h"
#include "index/growing/GrowingCommitPolicy.h"
#include "index/scalar/inverted/InvertedIndexReader.h"
#include "tantivy-wrapper.h"

// ============================================================================
// !!!!!!  READ THIS BEFORE LANDING ANY GROWING SCALAR INDEX  !!!!!!
//
//   THIS CLASS IS THE TRIGGER CONDITION FOR AN EXISTING OUT-OF-BOUNDS BUG.
//   core_refactor/01-scalar-index.md §13.1; issue #51237 is the same shape.
//
// `exec/expression/Expr.h:2239-2253` already names it, in the code:
//
//     "Slice by active_count_ for the same reason as
//      ProcessIndexChunksForValid(): the cached bitmap is segment-global (a
//      scalar index always has exactly one chunk), while size_per_chunk_ is the
//      raw-data chunk granularity (segcore.chunkRows) and is unrelated to it. On
//      a sealed segment the two agree -- size_per_chunk() == get_row_count() --
//      which is why bounding by size_per_chunk_ has not broken yet; today only
//      sealed segments reach here, because on growing segments HasIndex() is
//      true only for vector/geometry fields and real geometry predicates take
//      the dedicated branch in GISFunctionFilterExpr::EvalForIndexSegment().
//      THE MOMENT A SCALAR FIELD GAINS AN INTERIM INDEX ON GROWING, or geometry
//      is routed through ProcessIndexChunks, size_per_chunk_ would over-run the
//      bitmap exactly as in issue #51237."
//
//   A GROWING SCALAR INDEX *IS* "a scalar field gaining an interim index on
//   growing". The two facts that keep the branch safe today both stop holding:
//   the index bitmap covers `CommittedRows()` (a watermark that lags inserts,
//   §7 semantics 1) while `size_per_chunk_` counts raw rows, and on a growing
//   segment those are unrelated in both directions.
//
//   §13.1's requirement, verbatim: "the same PR that lands growing scalar
//   indexes must first fix that boundary, or route growing scalar indexes down a
//   path that does not go through that branch." Fixing it is an `exec/` change
//   and is NOT in this change's scope; this comment exists so that it cannot be
//   landed unknowingly. The blast radius is a read past the end of a
//   `TargetBitmap`, i.e. wrong results or a crash, not a compile error.
//
//   Related and on the same trigger: §13.3 — `ByteSize()` returns a CACHED value
//   that is never refreshed while a growing segment keeps inserting
//   (`index/Index.h:130-135` says so and declares the method "designed for
//   sealed segments only"), yet growing indexes are charged to cachinglayer all
//   the same. Making growing indexes first-class widens that mis-accounting.
// ============================================================================

// The tantivy-backed growing scalar appender.
//
// See core_refactor/01-scalar-index.md §7 (the three semantics), §7.1 (faces are
// unified across families, interfaces inside a face are split by family), §12.6
// (which families may lag).
//
// WHY TANTIVY IS THE ENGINE HERE. §7's model — append, commit on a cadence,
// reload, hand readers an immutable generation — is tantivy's native model, and
// it is the only engine in the tree that already implements it
// (`TextMatchIndex`'s growing constructor, `index/TextMatchIndex.cpp:36-57`).
// The other scalar engines cannot do it as they stand: marisa builds a trie in
// one shot and then walks it again to fill `str_ids_`
// (`StringIndexMarisa.cpp:173-205`), `ScalarIndexSort` sorts a complete array,
// bitmap needs the full value domain. That is §6.1.1 form B, and form B has no
// incremental story — which is exactly why `ScalarFieldIndexing<T>`'s marisa and
// sort objects (`segcore/FieldIndexing.cpp:659-662`) are constructed and then
// never fed.
//
// COMPOSED, NOT INHERITED (§3 principle 2): this holds a
// `milvus::tantivy::TantivyIndexWrapper`. It does NOT derive from
// `InvertedIndexTantivy<T>` — that implementation-inheritance shortcut is the
// one §2.2 and §10 rule 3 outlaw, and it is how `TextMatchIndex` ended up with
// `In`/`Range` it must never answer.

namespace milvus::index {

// THE SNAPSHOT IS THE SEALED READER, REUSED — `InvertedIndexReader<T>`
// (index/scalar/inverted/InvertedIndexReader.h), which already implements
// `IndexReaderBase` + `ScalarPredicateReader<T>` + `PatternMatchReader` +
// `NullReader` over a `shared_ptr<TantivyIndexWrapper>`.
//
// That reuse is the design's own claim made concrete: THE READ IMPLEMENTATION IS
// SHARED BETWEEN SEALED AND GROWING; ONLY THE WRITE FACE DIFFERS (§3 principle
// 1 — faces are cut by CALLER, not by segment state; §7.1 — "the four faces are
// unified across families, the interfaces inside a face are split by family").
// A commit produces a new `InvertedIndexReader<T>` over the reloaded tantivy
// generation and every concurrent query shares it through `shared_ptr` (§4.3).
// The vector side does the same thing with `VectorMemReader<T>`
// (index/growing/KnowhereGrowingVectorIndex.h).
//
// !! THREE THINGS TO CHECK BEFORE RELYING ON THE REUSE.
//
//  1. `Count()` MUST BE THE WATERMARK, NOT THE ENGINE'S LIVE COUNT.
//     `InvertedIndexReader<T>`'s constructor is
//     `(engine, null_offsets, is_nested_index)` — no explicit count — so its
//     `Count()` can only come from the engine, which KEEPS GROWING behind the
//     snapshot. §5 fixes every bitmap's size at `Count()`, and §7 fixes a
//     growing snapshot's coverage at `CommittedRows()`; if `Count()` runs ahead,
//     consumers get bitmaps sized to rows the snapshot never indexed. That is
//     the same class of defect as §13.1 below. Its sibling `TextIndexReader`
//     already takes `count` explicitly (`index/scalar/text/TextIndexReader.h`),
//     so the fix is to make the inverted reader match. Cross-family; flagged in
//     the report rather than patched here.
//  2. FACE-COVERAGE GAP (see GrowingAppenderBase.h): the contract's
//     `ReaderSnapshot()` returns `shared_ptr<const ScalarPredicateReader<T>>` —
//     ONE face — while the reused snapshot really provides four (root +
//     predicate + pattern match + null). Through the contract's return type the
//     other three are unreachable, so a growing VARCHAR field could not answer
//     `LIKE` or `IS NULL` from its index even though the engine can.
//     `ReaderSnapshotErased()` is the proposed fix.
//  3. THE BITSET CALLBACK MUST BE THE GROWING ONE. It is bound on the ENGINE at
//     `create_reader(SetBitsetFn)` time, not on the reader, so the appender —
//     which owns the wrapper — is the one that must install `SetBitsetGrowing`.
//     On a growing segment tantivy can return doc ids beyond the bitset the
//     query allocated, which the sealed setter's contract says cannot happen
//     (`index/Utils.h`, `SetBitsetSealed`).

template <typename T>
class TantivyGrowingScalarIndex final : public GrowingScalarIndex<T>,
                                        public GrowingAppenderBase {
 public:
    // `commit_interval_in_ms` is segcore's cadence knob, injected — NOT read
    // from a global config here (§8: configuration arrives as a construction
    // argument; `FMIndex.h:30` including `segcore/SegcoreConfig.h` is the
    // anti-pattern being fixed).
    TantivyGrowingScalarIndex(DataType value_type,
                              std::string family,
                              const char* unique_id,
                              int64_t commit_interval_in_ms);

    ~TantivyGrowingScalarIndex() override = default;

    // --- GrowingScalarIndex<T> (§7) -----------------------------------------

    // Held exclusively by segcore's insert path; safe concurrently with reads.
    // `reserved_offset` is the row id of `values[0]`, matching
    // `TantivyIndexWrapper::add_data(ptr, n, offset_begin)` — tantivy indexes by
    // explicit row id, which is why appending concurrently with reads needs no
    // ordering with the column write.
    void
    Append(int64_t reserved_offset,
           size_t n,
           const T* values,
           const bool* valid) override;

    // Null before the first commit. §7: "an empty return means there is no
    // readable snapshot yet"; for tantivy the threshold is effectively zero
    // (matching `ScalarFieldIndexing::get_build_threshold()` returning 0,
    // `segcore/FieldIndexing.h:196-199`), so this is null only until the first
    // commit lands.
    std::shared_ptr<const ScalarPredicateReader<T>>
    ReaderSnapshot() const override;

    int64_t
    CommittedRows() const override;

    // --- GrowingAppenderBase (family-local root, see that header) -----------

    DataType
    ValueType() const override;

    std::string
    Family() const override;

    IndexReaderBasePtr
    ReaderSnapshotErased() const override;

 private:
    // Commit + reload, then publish a new snapshot. Called from `Append` when
    // the policy says so, and by segcore before a query that wants freshness.
    //
    // NOTE WHAT IS *NOT* HERE: no `Serialize`, no `Upload`, no flush path.
    // Growing persistence goes through segcore's flush, which re-builds the
    // index from the flushed column through the BUILDER face (§3 principle 1,
    // §7 point 3). An appender never produces an artifact.
    void
    CommitAndPublish();

    const DataType value_type_;
    const std::string family_;

    // COMPOSED ENGINE (§3 principle 2).
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> wrapper_;

    GrowingCommitPolicy commit_policy_;

    // Published snapshot. Readers take a `shared_ptr` copy; the appender
    // replaces the pointer under `mtx_` at each commit. Both sides therefore see
    // an immutable object and no reader is ever blocked by an append.
    mutable std::mutex mtx_;
    std::shared_ptr<const InvertedIndexReader<T>> snapshot_;

    // Null rows, mirroring `TextMatchIndex`'s `null_offset_`
    // (`index/TextMatchIndex.cpp:279-291`): tantivy still gets a document for a
    // null row so that row ids stay dense, and the null set is tracked here for
    // `NullReader`.
    std::vector<size_t> null_offsets_;
};

}  // namespace milvus::index
