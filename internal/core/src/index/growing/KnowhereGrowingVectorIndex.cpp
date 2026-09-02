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

#include "index/growing/KnowhereGrowingVectorIndex.h"

// SKELETON. `index/` line references are to the pre-W1 tree (master
// e255009e01); `segcore/FieldIndexing.*` references are to the current tree.

namespace milvus::index {

template <typename T>
KnowhereGrowingVectorIndex<T>::KnowhereGrowingVectorIndex(
    DataType value_type,
    IndexType index_type,
    MetricType metric_type,
    IndexVersion version,
    int64_t dim,
    int64_t build_threshold,
    knowhere::Json build_params)
    : value_type_(value_type),
      build_threshold_(build_threshold),
      build_params_(std::move(build_params)),
      engine_(DataType::NONE,
              std::move(index_type),
              std::move(metric_type),
              version,
              /*use_knowhere_build_pool=*/true) {
    engine_.SetDim(dim);
    // TODO: the DataView (`knowhere::ViewDataOp`) engine variant is the one the
    // interim path actually wants — see `KnowhereEngine`'s second constructor
    // and `VectorFieldIndexing::recreate_index` (`FieldIndexing.cpp:179-239`),
    // which builds the view op over the `ConcurrentVector`'s chunks. That view
    // is the one place the appender still touches a segcore type; after §7 it
    // must arrive as an injected callable, not as a `VectorBase*`.
}

template <typename T>
void
KnowhereGrowingVectorIndex<T>::Append(int64_t reserved_offset,
                                      size_t n,
                                      const void* data,
                                      int64_t dim,
                                      const bool* valid) {
    // TODO: move existing logic here — ONLY the `built_` branch of today's two
    // methods (see FieldIndexing.cpp:340-414 sparse, :492-567 dense):
    //   knowhere::GenDataSet(n, dim, data) [+ SetIsSparse for sparse]
    //   -> engine_.Raw().Add(dataset, build_params_)
    //   -> valid_.Append(valid, n)   (today `index_->UpdateValidData`,
    //                                 FieldIndexing.cpp:333,409,486,562)
    //   -> publish
    //
    // DO NOT bring the `!built_` branch: that is the cold-start full build and
    // it belongs to `IndexBuilder<T>`, arriving here through `AdoptBuiltIndex`
    // (§7.1). Bringing it would recreate the exact defect the split exists to
    // remove.
    //
    // ALSO DO NOT bring the catch-and-`recreate_index` recovery
    // (`FieldIndexing.cpp:363-372,516-525`): "recreating the index" on an
    // `AddWithDataset` failure silently drops every row indexed so far and
    // leaves the watermark lying. Under §7 a failed append simply does not
    // advance the watermark, and segcore's fallback (§7 semantics 2) covers the
    // tail.
}

template <typename T>
void
KnowhereGrowingVectorIndex<T>::AdoptBuiltIndex(KnowhereEngine engine,
                                               VectorValidData valid,
                                               int64_t covered_rows) {
    // The Builder -> Appender seam (§7 point 3). segcore crosses the threshold,
    // gathers `[0, build_threshold)` once from the column, feeds
    // `IndexBuilder<T>`, and hands the sealed artifact's engine here.
    engine_ = std::move(engine);
    valid_ = std::move(valid);
    Publish(covered_rows);
}

template <typename T>
void
KnowhereGrowingVectorIndex<T>::Publish(int64_t covered_rows) {
    // TODO: build a `VectorMemReader<T>` over the current engine + validity and
    // swap it in under `mtx_`, then advance `committed_rows_`.
    //
    // !! See the header: today there is no generation to snapshot, so this
    // "snapshot" aliases the live knowhere node. Publishing it as an immutable
    // reader is a PROMISE THE ENGINE DOES NOT CURRENTLY MAKE. Resolve before
    // implementing.
}

template <typename T>
std::shared_ptr<const VectorSearchReader>
KnowhereGrowingVectorIndex<T>::ReaderSnapshot() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return snapshot_;
}

template <typename T>
int64_t
KnowhereGrowingVectorIndex<T>::CommittedRows() const {
    std::lock_guard<std::mutex> lock(mtx_);
    // Today's nearest equivalent is the pair (`index_cur_`,
    // `sync_with_index_`) — a count nobody reads plus a boolean
    // (`FieldIndexing.cpp:336,352,413,489,566`). §7 replaces both with this.
    return committed_rows_;
}

template <typename T>
DataType
KnowhereGrowingVectorIndex<T>::ValueType() const {
    return value_type_;
}

template <typename T>
std::string
KnowhereGrowingVectorIndex<T>::Family() const {
    // The knowhere index type of the interim index — segcore's §12.6 lag-policy
    // table is keyed by this string, and the vector row of that table is
    // "waiting for the threshold is normal", which is why growing vector search
    // already falls back to brute force below it.
    return engine_.KnowhereIndexType();
}

template <typename T>
IndexReaderBasePtr
KnowhereGrowingVectorIndex<T>::ReaderSnapshotErased() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return std::const_pointer_cast<IndexReaderBase>(
        std::static_pointer_cast<const IndexReaderBase>(snapshot_));
}

// The growing interim index is built for the dense float families and sparse;
// binary is skipped today (`IndexingRecord::Initialize` continues on
// VECTOR_BINARY, `segcore/FieldIndexing.h:391-393`) and so is VECTOR_ARRAY.
template class KnowhereGrowingVectorIndex<float>;
template class KnowhereGrowingVectorIndex<float16>;
template class KnowhereGrowingVectorIndex<bfloat16>;
template class KnowhereGrowingVectorIndex<sparse_u32_f32>;

}  // namespace milvus::index
