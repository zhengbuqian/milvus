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

#include "index/growing/RTreeGrowingSpatialIndex.h"

// SKELETON. Line references are to the tree before refactor phase 1 (master
// e255009e01).

namespace milvus::index {

RTreeGrowingSnapshot::RTreeGrowingSnapshot(int64_t covered_rows)
    : covered_rows_(covered_rows) {
}

milvus::ResourceUsage
RTreeGrowingSnapshot::CellByteSize() const {
    // §12.3 / §13.3.
    return {};
}

ReaderCaps
RTreeGrowingSnapshot::Caps() const {
    // spatial = true, exact = FALSE (candidate family, §5.6).
    return {};
}

Domain
RTreeGrowingSnapshot::CoordDomain() const {
    return Domain::Row;
}

int64_t
RTreeGrowingSnapshot::Count() const {
    return covered_rows_;
}

DataType
RTreeGrowingSnapshot::ValueType() const {
    return DataType::GEOMETRY;
}

int64_t
RTreeGrowingSnapshot::MemoryUsage() const {
    // TODO: move existing logic here (see RTreeIndex.h:170's `ByteSize`).
    return 0;
}

TargetBitmap
RTreeGrowingSnapshot::Candidates(SpatialOp op,
                                 const Geometry& query_geom) const {
    // TODO: move existing logic here (see RTreeIndex.cpp's `QueryCandidates`
    // and RTreeIndexWrapper's query entry), with the two contract changes noted
    // in the header (native `SpatialOp`, `TargetBitmap` output).
    return {};
}

// ---------------------------------------------------------------------------

RTreeGrowingSpatialIndex::RTreeGrowingSpatialIndex() {
    // TODO: create an `RTreeBuildEngine` directly. NOT the `RTreeIndex` family
    // class and NOT a `FileManagerContext` — see the header: the context
    // assembled at `FieldIndexing.cpp:614-660` (chunk manager, arrow filesystem,
    // an `IndexMeta` with `build_id = 0`, `segment_id = 0`,
    // `key = "rtree_index"`) exists only to satisfy `RTreeIndex`'s constructor,
    // and an appender writes nothing to storage.
    //
    // `RTreeBuildEngine` takes an `index_path` because `Finish()` writes there.
    // A growing appender has no such path until the snapshot question above is
    // answered — that is the same open question, seen from the constructor.
}

void
RTreeGrowingSpatialIndex::Append(int64_t reserved_offset,
                                 size_t n,
                                 const std::string_view* wkb_values,
                                 const bool* valid) {
    // TODO: move existing logic here (see FieldIndexing.cpp:753-818
    // `process_geometry_data`): per row,
    // `engine_->AddGeometry(wkb, len, reserved_offset + i)`.
    //
    // KEEP THE SILENT SKIP AND KEEP IT DOCUMENTED: a row whose WKB fails to
    // parse is not added but the offset still advances
    // (`RTreeEngine.h`'s note, from `RTreeIndexWrapper.cpp:134-151`). Offset
    // alignment depends on it, and on the growing side that alignment is also
    // what makes the watermark meaningful.
    //
    // WHAT MUST CHANGE RATHER THAN MOVE:
    //  - the two `AppendSegmentIndex` overloads that decode a `DataArray` vs a
    //    `FieldDataPtr` (`FieldIndexing.cpp:674-751`) collapse into this one
    //    method — decoding the write payload is segcore's, and the appender's
    //    input currency is a plain array (§6.1's "the builder's input currency
    //    is a raw array", which holds for the appender too);
    //  - `built_` / `sync_with_index_` / `index_cur_` become the commit policy's
    //    watermark;
    //  - the per-row try/catch that rethrows as `UnexpectedError`
    //    (`:797-803`) must not swallow a partial append: with a watermark, a
    //    failed row means the watermark simply does not advance.
}

std::shared_ptr<const SpatialReader>
RTreeGrowingSpatialIndex::ReaderSnapshot() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return snapshot_;
}

int64_t
RTreeGrowingSpatialIndex::CommittedRows() const {
    // Today's nearest equivalent is `index_cur_`, which nobody reads
    // (`FieldIndexing.cpp:809`).
    return 0;
}

DataType
RTreeGrowingSpatialIndex::ValueType() const {
    return DataType::GEOMETRY;
}

std::string
RTreeGrowingSpatialIndex::Family() const {
    // `families::kRTree` (index/Families.h).
    return "rtree";
}

IndexReaderBasePtr
RTreeGrowingSpatialIndex::ReaderSnapshotErased() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return std::const_pointer_cast<IndexReaderBase>(
        std::static_pointer_cast<const IndexReaderBase>(snapshot_));
}

}  // namespace milvus::index
