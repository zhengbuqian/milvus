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

#include "index/scalar/spatial/RTreeIndexReader.h"

#include <utility>

namespace milvus::index {

RTreeIndexReader::RTreeIndexReader(std::unique_ptr<RTreeQueryEngine> engine,
                                   std::vector<size_t> null_offsets,
                                   int64_t total_num_rows)
    : engine_(std::move(engine)),
      null_offsets_(std::move(null_offsets)),
      total_num_rows_(total_num_rows) {
    // TODO: assert null_offsets_ is sorted ascending (see the member comment).
}

RTreeIndexReader::~RTreeIndexReader() = default;

ReaderCaps
RTreeIndexReader::Caps() const {
    // `exact = false`: the MBR filter returns a SUPERSET and exec refines
    // (`PhyGISRefineConjunctExpr`). This is the candidate-family contract that
    // §5.6 says ngram should copy — spatial got it right first.
    return ReaderCaps{.spatial = true, .exact = false};
}

Domain
RTreeIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
RTreeIndexReader::Count() const {
    return total_num_rows_;
}

DataType
RTreeIndexReader::ValueType() const {
    return DataType::GEOMETRY;
}

int64_t
RTreeIndexReader::MemoryUsage() const {
    // TODO: move existing logic here (see RTreeIndex.h:160-174 ComputeByteSize:
    // null-offset capacity + engine ByteSize).
    //
    // DO NOT COPY THE BASE CALL. RTreeIndex.h:162 calls
    // `ScalarIndex<T>::ComputeByteSize()` and then reads `cached_byte_size_` at
    // :163 as if it accumulated — but `ScalarIndex` does not define the method,
    // so it resolves to `IndexBase::ComputeByteSize` (Index.h:145-148), which
    // ZEROES the field. The "accumulated" total is therefore always 0 + own
    // parts. Harmless today, but it is dead weight that reads as a bug.
}

ResourceUsage
RTreeIndexReader::CellByteSize() const {
    // NOTE: RTreeIndex never called `SetCellSize` at all, so this family
    // reports whatever the translator estimated from the file size — a third
    // variant on top of the two §12.3 already names.
}

TargetBitmap
RTreeIndexReader::Candidates(SpatialOp op, const Geometry& query_geom) const {
    // TODO: move existing logic here (see RTreeIndex.cpp:505-518 QueryCandidates
    // plus RTreeIndexWrapper.cpp:252-281), converting the candidate offsets into
    // a bitmap of size Count().
    //
    // Two defects to fix rather than transcribe:
    //   1. `const Geometry query_geometry` was taken BY VALUE
    //      (RTreeIndex.h:185), copying a GEOS-owning object on every call.
    //      The contract takes `const Geometry&`.
    //   2. The GEOS context handle is leaked on any throw between
    //      `GEOS_init_r` (RTreeIndex.cpp:513) and `GEOS_finish_r` (:517) —
    //      no RAII guard. Use a scope guard when moving.
    //
    // ALSO ABSORBED HERE: `RTreeIndex::Query(const DatasetPtr&)`
    // (RTreeIndex.cpp:520-543). Note for the record that §5.6 calls that method
    // an empty shell with no production callers; IT IS NOT — it is fully
    // implemented and it is TODAY'S ONLY production entry point, called from
    // `exec/expression/GISFunctionFilterExpr.cpp:466`. What disappears is the
    // knowhere-style `DatasetPtr` packing (the `OPERATOR_TYPE` / `MATCH_VALUE`
    // keys of index/Meta.h:22,28 unpacked at RTreeIndex.cpp:526-531), which
    // becomes two typed parameters. The BEHAVIOUR moves here; only the
    // untyped envelope is deleted.
}

TargetBitmap
RTreeIndexReader::IsNull() const {
    // TODO: move existing logic here (see RTreeIndex.cpp:431-443).
    // The `lower_bound(count)` clamp existed because the GROWING path could
    // append offsets past `Count()`. A sealed reader is immutable and the
    // builder fixes `Count()` at Seal(), so the clamp becomes an assertion.
}

TargetBitmap
RTreeIndexReader::IsNotNull() const {
    // TODO: move existing logic here (see RTreeIndex.cpp:445-457).
}

}  // namespace milvus::index
