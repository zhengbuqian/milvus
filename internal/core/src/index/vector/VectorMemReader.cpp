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

#include "index/vector/VectorMemReader.h"

// SKELETON — the bodies are the re-homing map (§11.3: move verbatim, do not
// redesign). Line references are to the pre-W1 tree (master e255009e01).

namespace milvus::index {

template <typename T>
milvus::ResourceUsage
VectorMemReader<T>::CellByteSize() const {
    // TODO: this is NOT a move of `IndexBase::CellByteSize`. §12.3 says the
    // yardstick must be defined first and every family made to fill it the same
    // way. Leaving the hole visible on purpose.
    return {};
}

template <typename T>
ReaderCaps
VectorMemReader<T>::Caps() const {
    // Consistency-check object only — the query-time source is the inventory's
    // cached caps, derived at load time by `VectorMemLoader::DeriveCaps`
    // (§4.1/§4.3). This must return exactly what that returned.
    //
    // NOTE: `ReaderCaps` (contracts/ReaderCaps.h) has no vector-shaped bits at
    // all — every field is a scalar predicate/pattern/ngram/spatial/json notion.
    // A vector reader can only answer "all false, exact = true", which says
    // nothing useful about it. See the report.
    return {};
}

template <typename T>
Domain
VectorMemReader<T>::CoordDomain() const {
    // Row, except for embedding-list indexes — see the header note.
    return Domain::Row;
}

template <typename T>
int64_t
VectorMemReader<T>::Count() const {
    // TODO: move existing logic here (see VectorMemIndex.h:85-95): zero when the
    // nullable mapping is enabled and has no valid rows, zero for an empty
    // embedding-list index, otherwise `engine_.RawCount()`.
    return 0;
}

template <typename T>
DataType
VectorMemReader<T>::ValueType() const {
    // The vector element type this reader was built for (VECTOR_FLOAT,
    // VECTOR_FLOAT16, ..., VECTOR_SPARSE_U32_F32). Today this is not stored on
    // the index at all; it is implied by the template argument and rediscovered
    // from the field schema at every call site.
    return DataType::NONE;
}

template <typename T>
int64_t
VectorMemReader<T>::MemoryUsage() const {
    // Pure self-description (§4.2). Today's counterpart is
    // `IndexBase::ComputeByteSize()` overridden per family; note §13.3 — that
    // number is a CACHED value that a growing segment never refreshes, which is
    // a correctness problem the moment growing indexes are first-class.
    return 0;
}

template <typename T>
void
VectorMemReader<T>::Search(const DatasetPtr& dataset,
                           const VectorSearchParams& params,
                           const BitsetView& bitset,
                           milvus::OpContext* op_ctx,
                           SearchResult& result) const {
    // TODO: move existing logic here (see VectorMemIndex.cpp:721-815 —
    // `VectorMemIndex<T>::Query`). Verbatim, with `SearchInfo` replaced by the
    // narrow §12.1(a) type: the body reads only search_params_/metric_type_/
    // topk_/trace_ctx_, which is what the audit found.
}

template <typename T>
knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
VectorMemReader<T>::Iterators(const DatasetPtr& dataset,
                              const knowhere::Json& json,
                              const BitsetView& bitset,
                              milvus::OpContext* op_ctx) const {
    // TODO: move existing logic here (see VectorMemIndex.cpp:234-291 —
    // `VectorIterators`). SHAPE UNCHANGED ON PURPOSE (§12.1(b)).
    return knowhere::expected<
        std::vector<knowhere::IndexNode::IteratorPtr>>::Err(
        knowhere::Status::not_implemented, "skeleton");
}

template <typename T>
bool
VectorMemReader<T>::RefineEnabled() const {
    // TODO: move existing logic here (see VectorMemIndex.cpp:826-835).
    return engine_.RefineEnabled();
}

template <typename T>
bool
VectorMemReader<T>::HasRawData() const {
    // TODO: move existing logic here (see VectorMemIndex.cpp:816-825).
    return engine_.HasRawData();
}

template <typename T>
std::vector<uint8_t>
VectorMemReader<T>::GetVector(const DatasetPtr& dataset) const {
    // TODO: move existing logic here (see VectorMemIndex.cpp:836-858).
    return {};
}

template <typename T>
std::unique_ptr<const knowhere::sparse::SparseRow<SparseValueType>[]>
VectorMemReader<T>::GetSparseVector(const DatasetPtr& dataset) const {
    // TODO: move existing logic here (see VectorMemIndex.cpp:889-909).
    //
    // CONTRACT NOTE: `VectorValueReader` bundles `GetVector` and
    // `GetSparseVector`, so a dense-only family must still define the sparse one.
    // The in-memory family is the lucky one — it really implements both. DiskANN
    // is not; see VectorDiskReader.h.
    return nullptr;
}

template <typename T>
MetricType
VectorMemReader<T>::Metric() const {
    return engine_.Metric();
}

template <typename T>
IndexType
VectorMemReader<T>::KnowhereIndexType() const {
    return engine_.KnowhereIndexType();
}

template <typename T>
int64_t
VectorMemReader<T>::Dim() const {
    return engine_.Dim();
}

template <typename T>
knowhere::Json
VectorMemReader<T>::PrepareSearchParams(
    const VectorSearchParams& params) const {
    return engine_.PrepareSearchParams(params);
}

template <typename T>
bool
VectorMemReader<T>::HasValidData() const {
    return valid_.Enabled();
}

template <typename T>
int64_t
VectorMemReader<T>::ValidCount() const {
    return valid_.ValidCount();
}

template <typename T>
bool
VectorMemReader<T>::IsRowValid(int64_t logical_offset) const {
    return valid_.IsRowValid(logical_offset);
}

template <typename T>
int64_t
VectorMemReader<T>::PhysicalOffset(int64_t logical_offset) const {
    return valid_.PhysicalOffset(logical_offset);
}

template <typename T>
int64_t
VectorMemReader<T>::LogicalOffset(int64_t physical_offset) const {
    return valid_.LogicalOffset(physical_offset);
}

template <typename T>
const milvus::OffsetMapping&
VectorMemReader<T>::OffsetMapping() const {
    return valid_.Mapping();
}

template <typename T>
knowhere::expected<knowhere::DataSetPtr>
VectorMemReader<T>::CalcDistByIDs(const knowhere::DataSetPtr& query_dataset,
                                  const BitsetView& bitset,
                                  const int64_t* labels,
                                  size_t labels_len,
                                  bool is_cosine,
                                  milvus::OpContext* op_ctx) const {
    // TODO: move existing logic here (see VectorMemIndex.cpp:1191-...).
    return knowhere::expected<knowhere::DataSetPtr>::Err(
        knowhere::Status::not_implemented, "skeleton");
}

template <typename T>
std::pair<std::vector<uint8_t>, std::vector<size_t>>
VectorMemReader<T>::GetEmbListByIds(const DatasetPtr& dataset,
                                    const std::string& metric_type) const {
    // TODO: move existing logic here (see VectorMemIndex.cpp:859-888).
    return {};
}

// Same instantiation set as today's `VectorMemIndex<T>`
// (VectorMemIndex.cpp:1202-1207).
template class VectorMemReader<float>;
template class VectorMemReader<bin1>;
template class VectorMemReader<float16>;
template class VectorMemReader<bfloat16>;
template class VectorMemReader<int8>;
template class VectorMemReader<sparse_u32_f32>;

}  // namespace milvus::index
