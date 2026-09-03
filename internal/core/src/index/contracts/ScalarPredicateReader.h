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

#include "common/Types.h"

// Point / range predicates. Pure mixin, does NOT derive from `IndexReaderBase`
// (§4).
//
// See core_refactor/01-scalar-index.md §5.1.

namespace milvus::index {

// NATIVE ENUM, deliberately not `milvus::OpType`.
//
// §5.1's snippet writes `OpType`, but `milvus::OpType` is `proto::plan::OpType`
// (`common/Types.h:106`). README §5 rule 2 and this document's own §5.6 forbid a
// pb type on a contract signature and prescribe exactly this remedy: "define a
// native enum in the contract layer and map at the boundary". §5.6 applies it to
// the GIS operator; the same reasoning applies verbatim here, so the comparison
// operators get a native enum too. proto -> native mapping happens in plan/exec.
//
// The value set is the comparison subset of `proto::plan::OpType`; the LIKE
// family lives in `PatternMatchReader::PatternOp`, and In / NotIn / Range are
// methods here rather than enum values.
enum class CompareOp {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterEqual,
    LessThan,
    LessEqual,
};

// For string families T is `std::string_view` (§5.1): every input on this
// interface and on `PatternMatchReader` is read-only and points at caller
// memory, so view-ing has no lifetime problem — the tantivy FFI is ptr+len
// anyway, and so are marisa's `predictive_search` and FMIndex's pattern.
//
// THIS APPLIES TO THE INPUT SIDE ONLY. The value interface
// (`ScalarValueReader<T>::Lookup`) must NOT follow — see the reason there.
template <typename T>
class ScalarPredicateReader {
 public:
    virtual ~ScalarPredicateReader() = default;

    // Output is always a `TargetBitmap`; 1 = hit; size == Count() (§5).
    // No sparse-offset or callback variant exists, by decision: selectivity is a
    // RUNTIME property of the query, not a static property of the family, so the
    // shape cannot be chosen per family — and of the two, the bitmap is the one
    // that does not explode when it degenerates (Count()/8 regardless of
    // selectivity, versus 8 bytes/row for sparse offsets: a 64x blow-up on dense
    // results, 720MB at 90M hits).
    virtual TargetBitmap
    In(size_t n, const T* values) const = 0;

    virtual TargetBitmap
    NotIn(size_t n, const T* values) const = 0;

    virtual TargetBitmap
    Range(const T& value, CompareOp op) const = 0;

    virtual TargetBitmap
    Range(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const = 0;
};

// Removed relative to today's `ScalarIndex<T>` (§5.1):
//   - `Query(DatasetPtr)`, the knowhere-shaped catch-all entry point.
//   - `Build` / `Size` / `GetIndexType`: not query-interface members.
//   - `IsNull` / `IsNotNull`: moved to the cross-family `NullReader`.
//   - `InApplyFilter`: ZERO production call sites (the only reference is
//     `JsonFlatIndexTest.cpp:799`), and RTree's implementation is a throwing
//     shell.
//   - `InApplyCallback`: one consumer,
//     `PhyUnaryRangeFilterExpr::ExecArrayEqualForIndex` (`UnaryExpr.cpp:804,807`).
//     It exists nominally to avoid materializing a full bitmap, yet both
//     implementations do `TargetBitmap bitset(Count()); terms_query(...);
//     apply_hits_with_callback(...)` — they materialize the full bitmap anyway
//     (`InvertedIndexTantivy.cpp:428` still carries "todo: could push-down the
//     callback to tantivy query"). exec's 1% early exit is achievable with a
//     plain `In()` plus a bitmap intersection, and faster than today's
//     `unordered_set` intersection.

}  // namespace milvus::index
