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

#include "index/vector/VectorDiskReader.h"

// SKELETON — bodies are the re-homing map (§11.3). Line references are to the
// tree before refactor phase 1 (master e255009e01).

namespace milvus::index {

template <typename T>
milvus::ResourceUsage
VectorDiskReader<T>::CellByteSize() const {
    // §12.3 — undefined yardstick, deliberately left as a hole.
    return {};
}

template <typename T>
ReaderCaps
VectorDiskReader<T>::Caps() const {
    // Must equal what `VectorDiskLoader::DeriveCaps` produced (§4.1).
    return {};
}

template <typename T>
Domain
VectorDiskReader<T>::CoordDomain() const {
    return Domain::Row;
}

template <typename T>
int64_t
VectorDiskReader<T>::Count() const {
    // TODO: move existing logic here (see VectorDiskIndex.h:217-227).
    return 0;
}

template <typename T>
DataType
VectorDiskReader<T>::ValueType() const {
    return DataType::NONE;
}

template <typename T>
int64_t
VectorDiskReader<T>::MemoryUsage() const {
    return 0;
}

template <typename T>
void
VectorDiskReader<T>::Search(const DatasetPtr& dataset,
                            const VectorSearchParams& params,
                            const BitsetView& bitset,
                            milvus::OpContext* op_ctx,
                            SearchResult& result) const {
    // TODO: move existing logic here (see VectorDiskIndex.cpp:680-783 —
    // `VectorDiskAnnIndex<T>::Query`), including the beam-width handling that
    // reads `search_beamwidth_`.
}

template <typename T>
knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
VectorDiskReader<T>::Iterators(const DatasetPtr& dataset,
                               const knowhere::Json& json,
                               const BitsetView& bitset,
                               milvus::OpContext* op_ctx) const {
    // TODO: move existing logic here (see VectorDiskIndex.cpp:784-821). Shape
    // unchanged on purpose (§12.1(b)).
    return knowhere::expected<
        std::vector<knowhere::IndexNode::IteratorPtr>>::Err(
        knowhere::Status::not_implemented, "skeleton");
}

template <typename T>
bool
VectorDiskReader<T>::RefineEnabled() const {
    // TODO: move existing logic here (see VectorDiskIndex.cpp:832-841).
    return engine_.RefineEnabled();
}

template <typename T>
bool
VectorDiskReader<T>::HasRawData() const {
    // TODO: move existing logic here (see VectorDiskIndex.cpp:822-831).
    return engine_.HasRawData();
}

template <typename T>
std::vector<uint8_t>
VectorDiskReader<T>::GetVector(const DatasetPtr& dataset) const {
    // TODO: move existing logic here (see VectorDiskIndex.cpp:842-865).
    return {};
}

template <typename T>
std::unique_ptr<const knowhere::sparse::SparseRow<SparseValueType>[]>
VectorDiskReader<T>::GetSparseVector(const DatasetPtr& dataset) const {
    // RE-HOMED VERBATIM, INCLUDING THE THROW (VectorDiskIndex.h:263-267):
    //   ThrowInfo(Unsupported, "get sparse vector not supported for disk index")
    // and flagged at the top of the header as a contract gap: the interface
    // bundles a capability this family does not have, which §3 principle 3
    // forbids expressing by exception. Not silently "fixed" here — that is the
    // contract owner's call (§11.3 says re-home without redesigning).
    return nullptr;
}

template <typename T>
MetricType
VectorDiskReader<T>::Metric() const {
    return engine_.Metric();
}

template <typename T>
IndexType
VectorDiskReader<T>::KnowhereIndexType() const {
    return engine_.KnowhereIndexType();
}

template <typename T>
int64_t
VectorDiskReader<T>::Dim() const {
    return engine_.Dim();
}

template <typename T>
knowhere::Json
VectorDiskReader<T>::PrepareSearchParams(
    const VectorSearchParams& params) const {
    return engine_.PrepareSearchParams(params);
}

template <typename T>
bool
VectorDiskReader<T>::HasValidData() const {
    return valid_.Enabled();
}

template <typename T>
int64_t
VectorDiskReader<T>::ValidCount() const {
    return valid_.ValidCount();
}

template <typename T>
bool
VectorDiskReader<T>::IsRowValid(int64_t logical_offset) const {
    return valid_.IsRowValid(logical_offset);
}

template <typename T>
int64_t
VectorDiskReader<T>::PhysicalOffset(int64_t logical_offset) const {
    return valid_.PhysicalOffset(logical_offset);
}

template <typename T>
int64_t
VectorDiskReader<T>::LogicalOffset(int64_t physical_offset) const {
    return valid_.LogicalOffset(physical_offset);
}

template <typename T>
const milvus::OffsetMapping&
VectorDiskReader<T>::OffsetMapping() const {
    return valid_.Mapping();
}

template <typename T>
knowhere::expected<knowhere::DataSetPtr>
VectorDiskReader<T>::CalcDistByIDs(const knowhere::DataSetPtr& query_dataset,
                                   const BitsetView& bitset,
                                   const int64_t* labels,
                                   size_t labels_len,
                                   bool is_cosine,
                                   milvus::OpContext* op_ctx) const {
    // TODO: move existing logic here (see VectorDiskIndex.cpp:944-...).
    return knowhere::expected<knowhere::DataSetPtr>::Err(
        knowhere::Status::not_implemented, "skeleton");
}

template <typename T>
std::pair<std::vector<uint8_t>, std::vector<size_t>>
VectorDiskReader<T>::GetEmbListByIds(const DatasetPtr& dataset,
                                     const std::string& metric_type) const {
    // TODO: move existing logic here (see VectorDiskIndex.cpp:866-896).
    return {};
}

// Same instantiation set as today's `VectorDiskAnnIndex<T>`
// (VectorDiskIndex.cpp:954-959).
template class VectorDiskReader<float>;
template class VectorDiskReader<float16>;
template class VectorDiskReader<bfloat16>;
template class VectorDiskReader<bin1>;
template class VectorDiskReader<sparse_u32_f32>;
template class VectorDiskReader<int8>;

}  // namespace milvus::index
