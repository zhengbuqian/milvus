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

#include <map>
#include <memory>
#include <string>
#include <variant>

#include "common/Types.h"
#include "index/contracts/GrowingIndex.h"
#include "index/contracts/ReaderCaps.h"
#include "index/contracts/ScalarPredicateReader.h"
#include "index/contracts/TextMatchReader.h"
#include "segcore/indexing/FieldIndexCapability.h"

// The growing segment's set of index APPENDERS.
//
// See core_refactor/01-scalar-index.md §7 (growing face), §7.1 (the vector
// appender and its `IndexBase` leak), §4.3 (the growing asymmetry) and §11.2
// item 3 (`IndexBase` retires within W1, growing exits included).
//
// -------------------------------------------------------------------------
// WHAT THIS REPLACES: `segcore::FieldIndexing` and its two subclasses
// (segcore/FieldIndexing.h:51,141,270). §2.2 records the diagnosis — the shared
// root is the UNION of two families' interfaces: of its five pure virtuals,
// three are vector-only and two are scalar-only, and each subclass throws away
// the other half (`ScalarFieldIndexing::AppendSegmentIndexDense` / `Sparse` /
// `GetDataFromIndex` throw at `:152,161,183`; `VectorFieldIndexing`'s two
// `AppendSegmentIndex` overloads throw at `:294,303`). That is the same Liskov
// violation as `IndexBase`, just on the growing side, and it proves the split
// the design draws: THE APPENDER FACE IS SHARED BY BOTH FAMILIES, THE APPEND
// SIGNATURE IS NOT.
//
// -------------------------------------------------------------------------
// THE ASYMMETRY WITH THE SEALED SIDE (§4.3, last part). An appender is not
// "fetched per write": this set holds it for the segment's whole life (one per
// indexed field) and the insert path calls `Append` directly. The READ side
// gets a DIFFERENT OBJECT — `ReaderSnapshot()` returns the immutable snapshot
// produced at commit/reload, shared by every concurrent query through one
// `shared_ptr`, CREATED ONCE PER COMMIT AND NOT PER QUERY. So neither side has
// a per-query proxy: sealed pins a long-lived object, growing shares a
// commit-era snapshot.
//
// -------------------------------------------------------------------------
// `IndexBase` EXIT LIST — THE GROWING HALF (§7.1 end, §11.2 item 3).
// `FieldIndexing::get_chunk_indexing` / `get_segment_indexing`
// (segcore/FieldIndexing.h:128,131) also return `PinWrapper<index::IndexBase*>`
// — growing uses `IndexBase` as its type-erased handle too. Both exits are on
// the retirement list; missing them leaves one reference alive after the sealed
// side is clean. Their consumers:
//   - `SegmentGrowingImpl.h:252` (`get_chunk_indexing(chunk_id)`)
//   - `SegmentGrowingImpl.h:562` (`get_segment_indexing()`)
//   - `query/SearchOnGrowing.cpp:139` (`get_segment_indexing()`)
// After W1 the read side of a growing index is `ReaderSnapshot()`, which is a
// typed `shared_ptr<const Face>` and needs no handle type at all.
//
// -------------------------------------------------------------------------
// §13.1 — A BUG THIS SET DIRECTLY ACTIVATES. THIS MUST BE FIXED IN THE SAME PR
// THAT LANDS GROWING SCALAR INDEXES.
// `exec/expression/Expr.h:2240-2252` already spells the trigger out: the cached
// index bitmap is segment-global (a scalar index is always a single chunk)
// while `size_per_chunk_` is the RAW DATA chunk granularity
// (`segcore.chunkRows`) — unrelated quantities that happen to be equal on
// sealed segments, which is the only reason this has not exploded. The comment
// ends: "The moment a scalar field gains an interim index on growing ...
// `size_per_chunk_` would over-run the bitmap exactly as in issue #51237."
// `GrowingScalarIndex<T>` IS that moment. Either fix the bound first, or route
// growing scalar indexes past that branch.
//
// §13.3 — a second, weaker one: `index/Index.h:130-135` says `ByteSize()`
// returns a CACHED value that is not refreshed while a growing segment keeps
// inserting, and declares itself "designed for sealed segments only" — yet
// growing indexes are charged to cachinglayer all the same. Making growing
// indexes a first-class contract widens the mis-accounted surface.

namespace milvus::segcore {

class GrowingIndexSet {
 public:
    // One indexed field's appender. The three alternatives are the three
    // growing contracts; they are PARALLEL, not a hierarchy (§7.1: "the four
    // faces are unified across the two families; the interfaces INSIDE a face
    // are split by family").
    //
    // The scalar arm is a variant over value types because
    // `index::GrowingScalarIndex<T>` is a template — the type erasure needed on
    // the management plane, exactly as the sealed inventory does it (§3
    // principle 4).
    struct Appender {
        DataType value_type{DataType::NONE};
        index::ReaderCaps caps;  // pure data, same role as in the inventory

        std::shared_ptr<void> scalar;  // index::GrowingScalarIndex<T>
        std::shared_ptr<index::GrowingTextIndex> text;
        std::shared_ptr<index::GrowingVectorIndex> vector;
    };

    // ---- Construction -------------------------------------------------------
    // TODO: move existing logic here — `IndexingRecord::Initialize`
    // (segcore/FieldIndexing.h:395-441) decides which fields get a growing
    // index (vector with interim index enabled + geometry today) and calls
    // `segcore::CreateIndex` (`FieldIndexing.h:566`). The successor creates the
    // family's `GrowingScalarIndex<T>` / `GrowingTextIndex` /
    // `GrowingVectorIndex` implementation instead.
    void
    Initialize(const Schema& schema, const IndexMetaPtr& index_meta);

    bool
    Has(FieldId field_id) const;

    // ---- Insert path (Appender face) ----------------------------------------
    // The insert path holds this exclusively and is concurrent with reads.
    //
    // TODO: move existing logic here — `IndexingRecord::AppendingIndex`
    // (segcore/FieldIndexing.h:443-456, two overloads: `DataArray*` and
    // `FieldDataPtr`), called from `SegmentGrowingImpl.cpp:798,1041`. The
    // `const VectorBase*` parameter DOES NOT SURVIVE: §7.1 shows it exists only
    // for the `!built_` cold-start branch (`FieldIndexing.cpp:376-405` sparse /
    // `:518-551` dense), which gathers `[0, build_threshold)` out of the whole
    // `ConcurrentVector` and does a full `BuildWithDataset`. That branch is
    // BUILDER work (§6.1.1 form B+), not appender work: segcore gathers once
    // when it crosses the threshold, feeds `index::IndexBuilder<T>`, and hands
    // the artifact to the appender as its starting point. The `built_` branch,
    // the real appender, only ever reads the new batch's validity
    // (`PrepareNullableAppendInfo` -> `bulk_is_valid_range(reserved_offset,
    // size, ...)`, `FieldIndexing.cpp:84`), which is exactly a `const bool*`.
    // With the cold start moved out, dense and sparse stop being two methods —
    // they differ only in a `dim` argument (§11.3).
    template <typename T>
    void
    Append(FieldId field_id,
           int64_t reserved_offset,
           size_t n,
           const T* values,
           const bool* valid);

    void
    AppendText(FieldId field_id,
               int64_t reserved_offset,
               size_t n,
               const std::string_view* values,
               const bool* valid);

    void
    AppendVector(FieldId field_id,
                 int64_t reserved_offset,
                 size_t n,
                 const void* data,
                 int64_t dim,
                 const bool* valid);

    // ---- Read path (snapshot, NOT a pin) ------------------------------------
    // Null means "no readable snapshot yet" — for scalar families the build
    // threshold is 0 so that is only the empty case, for vector families it is
    // the below-threshold case. §7's contract absorbs the difference: "an empty
    // return expresses it", so the threshold is not a contract-level concept
    // (`ScalarFieldIndexing::get_build_threshold()` returns 0 at
    // `FieldIndexing.h:197`, `VectorFieldIndexing::get_build_threshold()` reads
    // config at `:321`).
    template <typename T>
    std::shared_ptr<const index::ScalarPredicateReader<T>>
    ReaderSnapshot(FieldId field_id) const;

    std::shared_ptr<const index::TextMatchReader>
    TextReaderSnapshot(FieldId field_id) const;

    // ---- Watermark ----------------------------------------------------------
    // How far the snapshot covers, monotonically non-decreasing. §7 item 1:
    // the commit lag of today's text match index is IMPLICIT; the contract
    // turns it into an explicit watermark.
    //
    // §7 item 2: WHAT TO DO ABOUT `[CommittedRows(), insert_barrier)` IS NOT
    // PART OF THE INDEX CONTRACT — it is segcore/exec execution policy, keyed
    // by index family, and the table lives HERE (see `LagPolicy` below). The
    // index side carries no bit expressing it, otherwise `ReaderCaps` would
    // start carrying product semantics.
    int64_t
    CommittedRows(FieldId field_id) const;

    // Family-keyed policy for the rows the snapshot does not cover yet.
    // §7 item 2: "text match may lag (no top-up); every other family falls back
    // to a column scan to top up."
    //
    // OPEN (§12.6): `NgramReader` and `JsonFlatIndex` are unassigned. Both are
    // tantivy-based and mechanically identical to text match, but they serve
    // ordinary predicates (`LIKE`, JSON path comparisons) where the user's
    // expectation about "just-written data is immediately queryable" differs
    // from full-text search. That is a PRODUCT question, not a technical one.
    // Until it is answered they default to `TopUpByColumnScan` — conservative,
    // possibly wasteful. Leaving it open costs no redesign, only two missing
    // rows in this table.
    enum class LagPolicy {
        AllowLag,           // text match
        TopUpByColumnScan,  // everything else, and the default
    };

    LagPolicy
    LagPolicyOf(FieldId field_id) const;

    // ---- Capability plane ---------------------------------------------------
    // Same shape as the sealed inventory's, so exec's path selection does not
    // care which kind of segment it is on. Growing needs no "must not pin"
    // caveat (there is no cache slot), but it reports caps the same way so
    // `DetermineExecPath` stays one function.
    FieldIndexCapability
    Capability(FieldId field_id) const;

 private:
    std::map<FieldId, Appender> appenders_;
};

}  // namespace milvus::segcore
