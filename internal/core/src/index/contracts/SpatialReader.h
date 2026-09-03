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

#include "common/Geometry.h"
#include "common/Types.h"

// Spatial relation predicates. Pure mixin, does NOT derive from
// `IndexReaderBase` (§4).
//
// See core_refactor/01-scalar-index.md §5.6.
//
// GEOMETRY IS A SCALAR FAMILY, NOT A SEPARATE COMPONENT. `RTreeIndex` already
// sits under `ScalarIndex<T>`; the only difference from the other scalar
// families is the OPERATOR (spatial relations instead of point/range
// comparisons). Lifecycle, build, persistence and pin are identical. Splitting
// a component out by operator would be the wrong interface split — "one narrow
// contract per family" is this document's method, and one more spatial family
// is not an exception (§1).
//
// RTree's interfaces are exactly `SpatialReader` + `NullReader`. Everything it
// was forced to implement — In / NotIn / Range x2 / InApplyFilter /
// InApplyCallback / Reverse_Lookup / Query(DatasetPtr) — is removed: all seven
// are `ThrowInfo(NotImplemented)` shells with zero implementations and zero
// production callers (`RTreeIndex.cpp:462,515,525,534,542`,
// `RTreeIndex.h:187`). `IsNull`/`IsNotNull` are NOT removed — they are real
// (`RTreeIndex.cpp:469,491`, derived from `null_offset_`) and `geo_field IS
// NULL` reaches the index through `PhyNullExpr`. They belong to the
// unconditional `NullReader` interface (§5).

namespace milvus::index {

// NATIVE ENUM — this one is called out explicitly by §5.6. Today
// `RTreeIndex::QueryCandidates` takes
// `proto::plan::GISFunctionFilterExpr_GISOp` directly (`RTreeIndex.h:184`),
// which violates "pb only in adapters" (README §5 rule 2). The proto -> native
// mapping happens on the plan/exec side.
// The value set is the subset of `proto::plan::GISFunctionFilterExpr_GISOp`
// that actually reaches the index today:
//   - `STIsValid` is EXCLUDED: `GISFunctionFilterExpr.cpp:201-202,232` returns
//     early with "STIsValid operation cannot use index", so it never reaches a
//     spatial reader.
//   - `DWithin` IS included, and needs NO `distance` parameter on this
//     interface: exec turns the distance into a bounding-box geometry BEFORE
//     the index call
//     (`create_bounding_box_for_dwithin`, `GISFunctionFilterExpr.cpp:448-455`,
//     with the comment "Distance is not used for bounding box intersection
//     query") and the exact distance test happens in the refine step. Keeping the
//     conversion on exec's side is the same coarse/refine split §5.6 endorses.
//   - `Invalid` is excluded: a native contract enum has no reason to carry
//     proto's zero-value placeholder.
enum class SpatialOp {
    Equals,
    Touches,
    Overlaps,
    Crosses,
    Contains,
    Intersects,
    Within,
    DWithin,
};

class SpatialReader {
 public:
    virtual ~SpatialReader() = default;

    // Candidate generation: MBR coarse filter. The result is a SUPERSET
    // (`ReaderCaps::spatial = true`, `ReaderCaps::exact = false`); the exact
    // spatial relation is decided by exec after fetching the original values of
    // the candidate rows.
    //
    // Output is a `TargetBitmap`, not today's `std::vector<int64_t>` of
    // candidate offsets: that vector is an RTree-era spelling, and §5 fixes ONE
    // output shape for every interface. The consumer iterates the set bits; the
    // refine logic is unchanged.
    //
    // OBSERVED FACT, worth knowing before implementing: today
    // `RTreeIndexWrapper::query_candidates` (`RTreeIndexWrapper.cpp:253-281`)
    // IGNORES `op` entirely — it is used only in a log line — and always runs
    // `intersects(bounding_box(query_geom))`. The parameter stays on the
    // contract because exec must be able to say which relation it wants (a
    // future spatial index can prune on it, and the caps/refine contract is
    // stated per operator), but do not assume the current implementation
    // discriminates on it.
    virtual TargetBitmap
    Candidates(SpatialOp op, const Geometry& query_geom) const = 0;
};

}  // namespace milvus::index
