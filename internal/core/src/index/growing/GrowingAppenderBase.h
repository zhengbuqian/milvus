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

#include <cstdint>
#include <string>

#include "common/Types.h"
#include "index/contracts/IndexReader.h"

// !! CONTRACT GAP — THE APPENDER FACE HAS NO TYPE-ERASED ROOT.
//
// See core_refactor/01-scalar-index.md §7, §7.1, §4.2 (the read side's root) and
// §3 principle 4 ("templates on the hot path, type erasure only on the
// MANAGEMENT face").
//
// THE PROBLEM. segcore's `GrowingIndexSet` holds ONE APPENDER PER INDEXED FIELD,
// and those fields have different value types, so the table is heterogeneous.
// `contracts/GrowingIndex.h` offers only `GrowingScalarIndex<T>` (templated),
// `GrowingTextIndex` and `GrowingVectorIndex` — three unrelated class templates
// and classes with no common base. A heterogeneous container of them degenerates
// to `shared_ptr<void>` plus a per-type `static_pointer_cast` at every use, i.e.
// an UNCHECKED cast keyed by knowledge the container does not carry. The read
// side solved exactly this with `IndexReaderBase` (§4.2); the write side has no
// counterpart. Today's `FieldIndexing` "solves" it by being the union of both
// families' interfaces — the Liskov violation §2.2 and §7.1 catalogue — so
// erasure by God-base is not available either.
//
// THE DECISION TAKEN HERE: ADD A ROOT, symmetric to `IndexReaderBase`, carrying
// only what is genuinely T-free. Rationale, in order of weight:
//
//  1. `CommittedRows()` IS ALREADY T-FREE and is declared identically on all
//     three contract classes — the watermark is the appender's whole public
//     read-side story (§7 semantics 1), and every consumer of it (segcore
//     deciding what to do with `[CommittedRows(), insert_barrier)`) is
//     type-agnostic (§7 semantics 2).
//  2. §12.6's LAG POLICY TABLE IS KEYED BY FAMILY and lives in segcore
//     ("text match may lag; everything else falls back to a column scan"). To
//     look a row up, segcore must ask an appender its family WITHOUT knowing its
//     value type. `Family()` is that, and it is the same string the sealed side
//     already uses as its registry key (`IndexLoader::Family()`, §6.2).
//  3. `ReaderSnapshotErased()` MAKES THE READ PATH SYMMETRIC WITH SEALED. On the
//     sealed side segcore holds `CacheSlot<IndexReaderBase>` and the consumer
//     sibling-casts to the face it wants, ONCE PER EXPRESSION NODE (§4.3). With
//     an erased snapshot accessor the growing path is identical — get the root,
//     cast once to the face — and segcore never has to downcast the APPENDER at
//     all. It also removes a second gap: the typed `ReaderSnapshot()` in the
//     contract returns exactly ONE face (`ScalarPredicateReader<T>`), so a
//     growing inverted index could never expose `PatternMatchReader` or
//     `NullReader` on the same snapshot, and a growing RTree could not exist at
//     all (see GrowingSpatialIndexFace.h). One erased root, N faces, same rule
//     as sealed.
//
// WHY NOT `shared_ptr<void>`: it is not cheaper, it is only later. Every use
// site needs a `static_pointer_cast` whose correctness rests on an out-of-band
// tag, an unchecked cast is exactly what §9's exit criterion ("`dynamic_cast` to
// a concrete index goes to zero") is trying to eliminate on the read side, and
// the two things segcore actually wants from the table — the watermark and the
// family — are T-free, so erasing to `void` throws away information the design
// already declares.
//
// WHAT IT COLLAPSES ON THE CONSUMER SIDE. `segcore/indexing/GrowingIndexSet.h`
// currently models one field's appender as
//
//     struct Appender {
//         DataType value_type;
//         index::ReaderCaps caps;
//         std::shared_ptr<void> scalar;              // GrowingScalarIndex<T>
//         std::shared_ptr<index::GrowingTextIndex> text;
//         std::shared_ptr<index::GrowingVectorIndex> vector;
//     };
//
// — three parallel pointers of which exactly one is set, one of them untyped,
// plus a `value_type` field carried alongside precisely because the pointer
// cannot answer it. With this root the whole struct is one
// `std::shared_ptr<GrowingAppenderBase>`: `value_type` becomes `ValueType()`,
// the family arm becomes a CHECKED downcast performed only by the insert path
// (which knows `T` from the field schema anyway), and the read path stops
// downcasting the appender at all because `ReaderSnapshotErased()` already hands
// back the read root. `caps` stays as it is — it is pure data by design (§4.1).
//
// THIS FILE IS FAMILY-LOCAL BY NECESSITY: `index/contracts/` is owned elsewhere
// and is not edited here. If the contract owner accepts the shape, this belongs
// in `contracts/GrowingIndex.h` next to the three typed appenders, and the typed
// classes should derive from it — the same relationship `IndexReaderBase` has
// with the query faces, except that here the root SHOULD be a base (the typed
// appenders are classes, not mixins, and there is exactly one per object).

namespace milvus::index {

class GrowingAppenderBase {
 public:
    virtual ~GrowingAppenderBase() = default;

    // The value type that the typed appender erased. Lets a heterogeneous
    // holder route to the right `GrowingScalarIndex<T>` with a CHECKED cast.
    virtual DataType
    ValueType() const = 0;

    // Registry / policy key, same vocabulary as `IndexLoader::Family()`
    // ("inverted", "text", "rtree", "HNSW", ...). Consumed by §12.6's lag-policy
    // table in segcore.
    virtual std::string
    Family() const = 0;

    // §7 semantics 1. Monotonically non-decreasing.
    virtual int64_t
    CommittedRows() const = 0;

    // The current immutable snapshot as the type-erased read root, or null when
    // there is none yet (threshold not reached — §7's "an empty snapshot means
    // no readable snapshot yet"). The consumer sibling-casts to the face it
    // needs, exactly as it does after a pin on the sealed side (§4.3).
    virtual IndexReaderBasePtr
    ReaderSnapshotErased() const = 0;
};

// RETIREMENT LIST — the growing side's exits to `IndexBase` (§7.1, §11.2 rule 3).
// §7.1 names the first two; the third came out of the segcore consumer wiring and
// belongs on the same list:
//
//   1. `FieldIndexing::get_chunk_indexing(chunk_id)`  -> PinWrapper<IndexBase*>
//   2. `FieldIndexing::get_segment_indexing()`        -> PinWrapper<IndexBase*>
//   3. `FieldIndexing::has_raw_data()`                -> calls
//      `index_->HasRawData()` on an `IndexBase`, consumed at
//      `SegmentGrowingImpl.cpp:409,764,1032,1997,2078` (via
//      `IndexingRecord::HasRawData`, which gates it on `SyncDataWithIndex`).
//
// All three land in the same place after W1: (1) and (2) become
// `ReaderSnapshotErased()` above, and (3) becomes a question asked OF THE
// SNAPSHOT, not of the appender — `VectorValueReader::HasRawData()` for the
// vector family, and on the scalar side `ReaderCaps::value_lookup` /
// `cheap_value_lookup`, which is where "can I get the original value back" lives
// by design (§5.5, §4.1). Note the consequence for the caller: today
// `has_raw_data()` is answerable while no snapshot exists; afterwards a null
// snapshot means "no index-backed raw data", which is the same answer the
// `SyncDataWithIndex` gate already produces.

}  // namespace milvus::index
