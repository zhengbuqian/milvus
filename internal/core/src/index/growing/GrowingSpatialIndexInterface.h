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

#include "index/contracts/SpatialReader.h"

// !! CONTRACT GAP — THE ONE GROWING SCALAR INDEX THAT EXISTS TODAY FITS NONE OF
// THE THREE APPENDER CLASSES IN `index/contracts/GrowingIndex.h`.
//
// See core_refactor/01-scalar-index.md §7, §7.1 and §5.6.
//
// THE FACTS.
//   - `contracts/GrowingIndex.h` declares exactly three appenders:
//     `GrowingScalarIndex<T>` (snapshot interface hardwired to
//     `ScalarPredicateReader<T>`), `GrowingTextIndex` (snapshot interface
//     `TextMatchReader`) and `GrowingVectorIndex` (snapshot interface
//     `VectorSearchReader`).
//   - The only scalar family that ACTUALLY appends on a growing segment today is
//     GEOMETRY: `ScalarFieldIndexing<std::string>` builds an
//     `index::RTreeIndex<std::string>` (`segcore/FieldIndexing.cpp:650`) and
//     `process_geometry_data` calls `rtree_index->AddGeometry(wkb, offset)` per
//     row (`:797`). It is also the only one `sync_data_with_index()` ever
//     returns true for (`segcore/FieldIndexing.h:201-218`) — every other scalar
//     type reaches `AppendSegmentIndex` and hits
//     `ThrowInfo(Unsupported, "... not implemented for non-geometry scalar
//     fields")` (`FieldIndexing.cpp:706-711,745-750`), even though
//     `recreate_index` did construct a marisa / sort index for it (`:659-662`).
//     Those objects are never fed and never read: DEAD CODE, and the honest
//     reading of "growing scalar indexes exist today" is "exactly one does, and
//     it is spatial".
//   - RTree's query interface is `SpatialReader` + `NullReader` (§5.6), NOT
//     `ScalarPredicateReader<T>` — §5.6 is explicit that RTree must not
//     implement the predicate interface, because `T = std::string` would drag
//     WKB point predicates back in. So `GrowingScalarIndex<std::string>` cannot
//     be its appender: its `ReaderSnapshot()` return type is the one interface
//     RTree is forbidden to have.
//
// WHY THIS IS A CONTRACT SHAPE PROBLEM, NOT A MISSING CLASS. Adding
// `GrowingSpatialIndex` (below) fixes geometry and leaves the next family
// stranded: a growing INVERTED index would want `ScalarPredicateReader<T>` AND
// `PatternMatchReader` AND `NullReader` on one snapshot, and the contract's
// return type can carry only one interface. The structural fix is for
// `ReaderSnapshot()` to return `IndexReaderBasePtr` (the type-erased base
// class) and let the consumer sibling-cast, exactly as
// `JsonIndexReader::Resolve` already does (§5.7) — one snapshot, N interfaces,
// same rule as the sealed side. Declared family-locally here rather than by
// editing contracts/; see the report.

namespace milvus::index {

// Same three semantics as its siblings (§7): append is exclusive to the insert
// path and concurrent-safe with reads; the snapshot is immutable and covers
// [0, CommittedRows()); an empty snapshot means "nothing readable yet".
class GrowingSpatialIndex {
 public:
    virtual ~GrowingSpatialIndex() = default;

    // WKB payloads, matching `RTreeIndex::AddGeometry(const std::string& wkb,
    // int64_t row_offset)`. `std::string_view` for the same reason §5.1 gives
    // for the predicate interface: every input here is read-only and points at
    // caller memory.
    virtual void
    Append(int64_t reserved_offset,
           size_t n,
           const std::string_view* wkb_values,
           const bool* valid) = 0;

    virtual std::shared_ptr<const SpatialReader>
    ReaderSnapshot() const = 0;

    virtual int64_t
    CommittedRows() const = 0;
};

}  // namespace milvus::index
