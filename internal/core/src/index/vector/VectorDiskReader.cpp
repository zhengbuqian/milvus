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

#include <limits>
#include <type_traits>
#include <utility>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "common/RangeSearchHelper.h"
#include "common/Utils.h"
#include "index/Meta.h"
#include "index/vector/RangeSearchParams.h"
#include "index/vector/VectorReaderValidation.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/segcore_error_code.h"

// SKELETON — bodies are the re-homing map (§11.3). Line references are to the
// tree before refactor phase 1 (master e255009e01).

namespace milvus::index {
namespace {

constexpr uint32_t kMinDiskAnnBeamwidth = 1;
constexpr uint32_t kMaxDiskAnnBeamwidth = 128;

bool
IsEmptyEngine(const KnowhereEngine& engine, const VectorValidData& valid) {
    return (valid.Enabled() && valid.ValidCount() == 0) ||
           engine.IsEmptyEmbListIndex();
}

void
ValidateDiskAnnBeamwidth(const KnowhereEngine& engine, uint32_t beamwidth) {
    if (engine.KnowhereIndexType() != knowhere::IndexEnum::INDEX_DISKANN) {
        return;
    }
    if (beamwidth < kMinDiskAnnBeamwidth || beamwidth > kMaxDiskAnnBeamwidth) {
        ThrowInfo(ConfigInvalid,
                  "DiskANN beamwidth {} is outside [{}, {}]",
                  beamwidth,
                  kMinDiskAnnBeamwidth,
                  kMaxDiskAnnBeamwidth);
    }
}

[[noreturn]] void
ThrowSearchError(const char* operation,
                 const knowhere::expected<knowhere::DataSetPtr>& result,
                 const knowhere::Json* config = nullptr) {
    const auto status = result.error();
    if (config == nullptr) {
        ThrowInfo(knowhere::ToSegcoreErrorCode(status),
                  "failed to {}: status {} ({}), detail: {}",
                  operation,
                  static_cast<int>(status),
                  knowhere::Status2String(status),
                  result.what());
    }
    ThrowInfo(knowhere::ToSegcoreErrorCode(status),
              "failed to {}: config={} status {} ({}), detail: {}",
              operation,
              milvus::EscapeBraces(config->dump()),
              static_cast<int>(status),
              knowhere::Status2String(status),
              result.what());
}

}  // namespace

template <typename T>
VectorDiskReader<T>::VectorDiskReader(KnowhereEngine engine,
                                      VectorValidData valid,
                                      uint32_t search_beamwidth,
                                      std::shared_ptr<const void> staging_owner)
    : staging_owner_(std::move(staging_owner)),
      engine_(std::move(engine)),
      valid_(std::move(valid)),
      search_beamwidth_(search_beamwidth) {
    ValidateDiskAnnBeamwidth(engine_, search_beamwidth_);
}

template <typename T>
cachinglayer::ResourceUsage
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
    return engine_.PhysicalType();
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
    const auto shape = detail::ValidateQueryDataset(
        dataset, engine_.PhysicalType(), engine_.Dim());
    if (params.metric_type_ != engine_.Metric()) {
        ThrowInfo(MetricTypeInvalid,
                  "vector query metric {} disagrees with index metric {}",
                  params.metric_type_,
                  engine_.Metric());
    }
    const auto topk = params.topk_;
    const auto result_count = detail::CheckedResultCount(shape, topk);
    auto search_config = engine_.PrepareSearchParams(params);

    if (IsEmptyEngine(engine_, valid_)) {
        result.seg_offsets_.assign(result_count, INVALID_SEG_OFFSET);
        result.distances_.assign(result_count, 0.0F);
        result.total_nq_ = shape.logical_nq;
        result.unity_topK_ = topk;
        return;
    }

    if (engine_.KnowhereIndexType() == knowhere::IndexEnum::INDEX_DISKANN) {
        ValidateDiskAnnBeamwidth(engine_, search_beamwidth_);
        if (params.search_params_.contains(DISK_ANN_QUERY_LIST)) {
            search_config[DISK_ANN_SEARCH_LIST_SIZE] =
                params.search_params_.at(DISK_ANN_QUERY_LIST);
        }
        search_config[DISK_ANN_QUERY_BEAMWIDTH] =
            static_cast<int32_t>(search_beamwidth_);
        search_config[DISK_ANN_PQ_CODE_BUDGET] = 0.0;
    }

    auto final = [&] {
        if (CheckAndUpdateKnowhereRangeSearchParam(
                params, topk, engine_.Metric(), search_config)) {
            auto search = engine_.Raw().RangeSearch(
                dataset, search_config, bitset, op_ctx);
            if (!search.has_value()) {
                ThrowSearchError("range search", search);
            }
            return milvus::ReGenRangeSearchResult(
                search.value(), topk, shape.logical_nq, engine_.Metric());
        }

        auto search =
            engine_.Raw().Search(dataset, search_config, bitset, op_ctx);
        if (!search.has_value()) {
            ThrowSearchError("search", search, &search_config);
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

    // Knowhere offsets stay in the engine's physical coordinate system. The
    // query consumer owns bitset transformation and result remapping.
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
VectorDiskReader<T>::Iterators(const DatasetPtr& dataset,
                               const knowhere::Json& json,
                               const BitsetView& bitset,
                               milvus::OpContext* op_ctx) const {
    const auto shape = detail::ValidateQueryDataset(
        dataset, engine_.PhysicalType(), engine_.Dim());
    if (IsEmptyEngine(engine_, valid_)) {
        return detail::MakeEmptyVectorIterators(shape.logical_nq_size);
    }
    // The caller supplied an already prepared iterator config. Preserve the
    // baseline behavior: query-only DiskANN overrides are not injected here.
    return engine_.Raw().AnnIterator(dataset, json, bitset, false, op_ctx);
}

template <typename T>
bool
VectorDiskReader<T>::RefineEnabled() const {
    if (IsEmptyEngine(engine_, valid_)) {
        return false;
    }
    return engine_.RefineEnabled();
}

template <typename T>
bool
VectorDiskReader<T>::HasRawData() const {
    if (IsEmptyEngine(engine_, valid_)) {
        return true;
    }
    return engine_.HasRawData();
}

template <typename T>
std::vector<uint8_t>
VectorDiskReader<T>::GetVector(const DatasetPtr& dataset) const {
    if constexpr (std::is_same_v<T, sparse_u32_f32>) {
        ThrowInfo(Unsupported,
                  "dense vector retrieval is not supported for a sparse "
                  "disk index");
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
VectorDiskReader<T>::GetSparseVector(const DatasetPtr& dataset) const {
    (void)dataset;
    // No current UseDiskLoad backend exposes sparse retrieval. The inherited
    // VectorValueReader method remains the contract gap documented in the
    // header; return no fabricated ownership-bearing sparse buffer.
    ThrowInfo(Unsupported,
              "sparse vector retrieval is not supported for disk indexes");
}

template <typename T>
MetricType VectorDiskReader<T>::Metric() const {
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
    return engine_.Raw().CalcDistByIDs(
        query_dataset, bitset, labels, labels_len, is_cosine, op_ctx);
}

template <typename T>
std::pair<std::vector<uint8_t>, std::vector<size_t>>
VectorDiskReader<T>::GetEmbListByIds(const DatasetPtr& dataset,
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

// Same instantiation set as today's `VectorDiskAnnIndex<T>`
// (VectorDiskIndex.cpp:954-959).
template class VectorDiskReader<float>;
template class VectorDiskReader<float16>;
template class VectorDiskReader<bfloat16>;
template class VectorDiskReader<bin1>;
template class VectorDiskReader<sparse_u32_f32>;
template class VectorDiskReader<int8>;

}  // namespace milvus::index
