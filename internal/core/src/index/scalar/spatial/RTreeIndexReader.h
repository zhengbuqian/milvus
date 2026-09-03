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
#include <memory>
#include <vector>

#include "common/Geometry.h"
#include "common/Types.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/NullReader.h"
#include "index/contracts/SpatialReader.h"
#include "index/scalar/spatial/RTreeEngine.h"

// The READER of the spatial family.
//
// See 01-scalar-index.md §5.6 (`SpatialReader`) and §8's `RTreeIndex` row.
// Geometry is a SCALAR FAMILY, not a separate component: it differs from the
// others only in its OPERATORS (spatial relations rather than point/range
// comparisons); lifecycle, build, persistence and pin are identical (§1).
//
// INTERFACES: `SpatialReader` + `NullReader`, and nothing else.
//   - The seven point-predicate methods RTree was forced to implement are
//     deleted: `In` (RTreeIndex.cpp:424), `NotIn` (:478), `Range` x2 (:486,
//     :494), `InApplyFilter` (:459), `InApplyCallback` (:469), `Reverse_Lookup`
//     (RTreeIndex.h:149) — every one a `ThrowInfo(NotImplemented)` shell.
//   - `IsNull`/`IsNotNull` are KEPT (RTreeIndex.cpp:431-457). §5.6 is explicit:
//     they are real implementations and `geo_field IS NULL` reaches the index
//     through `PhyNullExpr`. Deleting them along with the shells would break a
//     live path.
//
// DE-TEMPLATED. `RTreeIndex<T>` is a template instantiated for `std::string`
// ONLY (RTreeIndex.cpp:760), and several members hard-assume WKB strings
// (RTreeIndex.cpp:523,555,589). The template parameter carried no information.

namespace milvus::index {

class RTreeIndexReader final : public IndexReaderBase,
                               public SpatialReader,
                               public NullReader {
 public:
    RTreeIndexReader(std::unique_ptr<RTreeQueryEngine> engine,
                     std::vector<size_t> null_offsets,
                     int64_t total_num_rows);

    ~RTreeIndexReader() override;

    // ---- IndexReaderBase (§4.2) ----------------------------------------

    ReaderCaps
    Caps() const override;

    Domain
    CoordDomain() const override;

    int64_t
    Count() const override;

    DataType
    ValueType() const override;

    int64_t
    MemoryUsage() const override;

    ResourceUsage
    CellByteSize() const override;

    // ---- SpatialReader (§5.6) ------------------------------------------

    // OUTPUT SHAPE CHANGED, deliberately: today's `QueryCandidates`
    // (RTreeIndex.h:183-186) fills a `std::vector<int64_t>` of candidate
    // offsets. §5 fixes ONE output shape for every interface, and sparse
    // offsets are 8 bytes/row against the bitmap's 1 bit/row — a 64x blow-up
    // whenever the candidate set is dense, which for an MBR coarse filter is
    // common. The refine loop on the exec side iterates set bits instead of
    // vector entries; nothing else changes.
    //
    // INPUT TYPE CHANGED: native `SpatialOp`, not
    // `proto::plan::GISFunctionFilterExpr_GISOp` (README §5 rule 2). The
    // proto->native mapping happens in exec, where the plan is already being
    // read. `STIsValid` never appears here (exec forces the raw-data path for
    // it, GISFunctionFilterExpr.cpp:200-204) and `DWithin`'s distance is turned
    // into a bounding box before the call (:448-459), which is why this
    // interface needs no distance parameter.
    TargetBitmap
    Candidates(SpatialOp op, const Geometry& query_geom) const override;

    // ---- NullReader (§5) -----------------------------------------------

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    std::unique_ptr<RTreeQueryEngine> engine_;

    // Sorted ascending. THE SORTEDNESS IS AN UNSTATED INVARIANT TODAY that
    // `IsNull`'s `std::lower_bound` (RTreeIndex.cpp:437-438) silently depends
    // on: every writer happens to append in order, and nothing asserts it. A
    // reader built once by a builder or a loader can and should assert it at
    // construction.
    //
    // Also gone with immutability: the `folly::SharedMutexWritePriority`
    // (RTreeIndex.h:217) that guarded this vector. It existed because the same
    // object was written by the growing path while being read by queries.
    std::vector<size_t> null_offsets_;

    int64_t total_num_rows_{0};
};

}  // namespace milvus::index
