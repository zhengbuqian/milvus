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

#include <limits>
#include <memory>
#include <type_traits>
#include <utility>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "common/RangeSearchHelper.h"
#include "common/Utils.h"
#include "index/vector/RangeSearchParams.h"
#include "index/vector/VectorReaderValidation.h"
#include "knowhere/segcore_error_code.h"

// SKELETON — the bodies are the re-homing map (§11.3: move verbatim, do not
// redesign). Line references are to the tree before refactor phase 1 (master
// e255009e01).

namespace milvus::index {

template <typename T>
cachinglayer::ResourceUsage
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
    return engine_.PhysicalType();
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
    const auto shape = detail::ValidateQueryDataset(
        dataset, engine_.PhysicalType(), engine_.Dim());
    const auto topk = params.topk_;
    const auto result_count = detail::CheckedResultCount(shape, topk);
    auto search_conf = engine_.PrepareSearchParams(params);

    const bool all_null = valid_.Enabled() && valid_.ValidCount() == 0;
    if (all_null || engine_.IsEmptyEmbListIndex()) {
        result.seg_offsets_.assign(result_count, INVALID_SEG_OFFSET);
        result.distances_.assign(result_count, 0.0F);
        result.total_nq_ = shape.logical_nq;
        result.unity_topK_ = topk;
        return;
    }

    auto final = [&] {
        if (CheckAndUpdateKnowhereRangeSearchParam(
                params, topk, engine_.Metric(), search_conf)) {
            milvus::tracer::AddEvent("start_knowhere_index_range_search");
            auto search =
                engine_.Raw().RangeSearch(dataset, search_conf, bitset, op_ctx);
            milvus::tracer::AddEvent("finish_knowhere_index_range_search");
            if (!search.has_value()) {
                const auto status = search.error();
                ThrowInfo(knowhere::ToSegcoreErrorCode(status),
                          "failed to range search: status {} ({}), detail: {}",
                          static_cast<int>(status),
                          knowhere::Status2String(status),
                          search.what());
            }
            auto regenerated = milvus::ReGenRangeSearchResult(
                search.value(), topk, shape.logical_nq, engine_.Metric());
            milvus::tracer::AddEvent("finish_ReGenRangeSearchResult");
            return regenerated;
        }

        milvus::tracer::AddEvent("start_knowhere_index_search");
        auto search =
            engine_.Raw().Search(dataset, search_conf, bitset, op_ctx);
        milvus::tracer::AddEvent("finish_knowhere_index_search");
        if (!search.has_value()) {
            const auto status = search.error();
            ThrowInfo(knowhere::ToSegcoreErrorCode(status),
                      "failed to search: config={} status {} ({}), detail: {}",
                      milvus::EscapeBraces(search_conf.dump()),
                      static_cast<int>(status),
                      knowhere::Status2String(status),
                      search.what());
        }
        return search.value();
    }();

    detail::ValidateRegularSearchResult(final, shape, topk, result_count);
    const auto* ids = final->GetIds();
    const auto* distances = final->GetDistance();
    const auto id_bytes =
        detail::CheckedSystemBytes(result_count, sizeof(int64_t), "search id");
    const auto distance_bytes = detail::CheckedSystemBytes(
        result_count, sizeof(float), "search distance");

    result.seg_offsets_.resize(result_count);
    result.distances_.resize(result_count);
    result.total_nq_ = shape.logical_nq;
    result.unity_topK_ = topk;
    if (result_count > 0) {
        milvus::fastmem::FastMemcpy(result.seg_offsets_.data(), ids, id_bytes);
        milvus::fastmem::FastMemcpy(
            result.distances_.data(), distances, distance_bytes);
    }
}

template <typename T>
knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
VectorMemReader<T>::Iterators(const DatasetPtr& dataset,
                              const knowhere::Json& json,
                              const BitsetView& bitset,
                              milvus::OpContext* op_ctx) const {
    const auto shape = detail::ValidateQueryDataset(
        dataset, engine_.PhysicalType(), engine_.Dim());
    const bool all_null = valid_.Enabled() && valid_.ValidCount() == 0;
    if (all_null || engine_.IsEmptyEmbListIndex()) {
        return detail::MakeEmptyVectorIterators(shape.logical_nq_size);
    }
    return engine_.Raw().AnnIterator(dataset, json, bitset, false, op_ctx);
}

template <typename T>
bool
VectorMemReader<T>::RefineEnabled() const {
    const bool all_null = valid_.Enabled() && valid_.ValidCount() == 0;
    if (all_null || engine_.IsEmptyEmbListIndex()) {
        return false;
    }
    return engine_.RefineEnabled();
}

template <typename T>
bool
VectorMemReader<T>::HasRawData() const {
    const bool all_null = valid_.Enabled() && valid_.ValidCount() == 0;
    if (all_null || engine_.IsEmptyEmbListIndex()) {
        return true;
    }
    return engine_.HasRawData();
}

template <typename T>
std::vector<uint8_t>
VectorMemReader<T>::GetVector(const DatasetPtr& dataset) const {
    if constexpr (std::is_same_v<T, sparse_u32_f32>) {
        ThrowInfo(Unsupported,
                  "dense vector retrieval is not supported for a sparse "
                  "index");
    } else {
        const auto request = detail::ValidateIdRequest(dataset, "get vector");
        if (request.rows_size == 0) {
            return {};
        }

        auto retrieved = engine_.Raw().GetVectorByIds(dataset);
        if (!retrieved.has_value()) {
            detail::ThrowRetrievalError("get vector", retrieved);
        }
        detail::ValidateDenseRetrievalResult(
            retrieved.value(), request, engine_.Dim(), "vector retrieval");
        return DecodeVectorByIdsResult<T>(retrieved.value());
    }
}

template <typename T>
std::unique_ptr<const knowhere::sparse::SparseRow<SparseValueType>[]>
VectorMemReader<T>::GetSparseVector(const DatasetPtr& dataset) const {
    if constexpr (!std::is_same_v<T, sparse_u32_f32>) {
        ThrowInfo(Unsupported,
                  "sparse vector retrieval is not supported for a dense "
                  "index");
    } else {
        const auto request =
            detail::ValidateIdRequest(dataset, "get sparse vector");
        if (request.rows_size == 0) {
            return nullptr;
        }

        auto retrieved = engine_.Raw().GetVectorByIds(dataset);
        if (!retrieved.has_value()) {
            detail::ThrowRetrievalError("get sparse vector", retrieved);
        }
        const auto& result = retrieved.value();
        if (result == nullptr) {
            ThrowInfo(KnowhereError,
                      "knowhere returned a null sparse-vector dataset");
        }
        if (result->GetRows() != request.rows) {
            ThrowInfo(KnowhereError,
                      "knowhere sparse-vector row count {} disagrees with "
                      "requested count {}",
                      result->GetRows(),
                      request.rows);
        }
        if (!result->GetIsSparse()) {
            ThrowInfo(KnowhereError,
                      "knowhere sparse-vector result is not marked sparse");
        }
        const auto* tensor =
            static_cast<const knowhere::sparse::SparseRow<SparseValueType>*>(
                result->GetTensor());
        if (tensor == nullptr) {
            ThrowInfo(KnowhereError,
                      "knowhere sparse-vector result has no tensor");
        }

        // The successful SPARSE_*_CC producer owns a SparseRow[] tensor and no
        // other raw buffers. Detach only that entry: if SetTensor throws, the
        // dataset still owns it; after it succeeds, unique_ptr construction is
        // non-throwing and the dataset continues to own any other entries.
        result->SetTensor(nullptr);
        return std::unique_ptr<
            const knowhere::sparse::SparseRow<SparseValueType>[]>(tensor);
    }
}

template <typename T>
MetricType VectorMemReader<T>::Metric() const {
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
    return engine_.Raw().CalcDistByIDs(
        query_dataset, bitset, labels, labels_len, is_cosine, op_ctx);
}

template <typename T>
std::pair<std::vector<uint8_t>, std::vector<size_t>>
VectorMemReader<T>::GetEmbListByIds(const DatasetPtr& dataset,
                                    const std::string& metric_type) const {
    if constexpr (std::is_same_v<T, sparse_u32_f32>) {
        ThrowInfo(Unsupported,
                  "sparse vectors are not supported as embedding-list "
                  "elements");
    } else {
        if (engine_.ElemType() == DataType::NONE) {
            ThrowInfo(Unsupported,
                      "embedding-list retrieval requires an embedding-list "
                      "index");
        }

        const auto request =
            detail::ValidateIdRequest(dataset, "get embedding list");
        if (request.rows_size == 0) {
            return {{}, {0}};
        }

        if (engine_.IsEmptyEmbListIndex()) {
            const auto& offsets = engine_.EmptyEmbListOffsets();
            const auto emb_list_count = offsets.size() - 1;
            for (size_t i = 0; i < request.rows_size; ++i) {
                if (request.ids[i] < 0 ||
                    static_cast<uint64_t>(request.ids[i]) >= emb_list_count) {
                    ThrowInfo(ConfigInvalid,
                              "embedding-list id {} is out of range [0, {})",
                              request.ids[i],
                              emb_list_count);
                }
            }
            if (request.rows_size == std::numeric_limits<size_t>::max()) {
                ThrowInfo(ConfigInvalid,
                          "embedding-list result offset count overflows");
            }
            const auto offset_count = request.rows_size + 1;
            detail::CheckedInputProduct(
                offset_count, sizeof(size_t), "embedding-list result offset");
            return {{}, std::vector<size_t>(offset_count, 0)};
        }

        auto retrieved = engine_.Raw().GetEmbListByIds(dataset, metric_type);
        if (!retrieved.has_value()) {
            detail::ThrowRetrievalError("get embedding list", retrieved);
        }
        detail::ValidateDenseRetrievalResult(retrieved.value(),
                                             request,
                                             engine_.Dim(),
                                             "embedding-list retrieval");
        return DecodeEmbListByIdsResult<T>(retrieved.value());
    }
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
