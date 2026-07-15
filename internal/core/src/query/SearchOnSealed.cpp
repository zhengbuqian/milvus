// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <folly/ExceptionWrapper.h>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Utils.h"
#include "common/ArrayOffsets.h"
#include "common/BitsetView.h"
#include "common/Chunk.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/OffsetMapping.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "exec/operator/Utils.h"
#include "index/Index.h"
#include "index/VectorIndex.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "mmap/ChunkedColumnInterface.h"
#include "query/CachedSearchIterator.h"
#include "query/SearchBruteForce.h"
#include "query/SearchOnSealed.h"
#include "query/SubSearchResult.h"
#include "query/Utils.h"
#include "query/helper.h"
#include "segcore/SealedIndexingRecord.h"

namespace milvus::query {

void
SearchOnSealedIndex(const Schema& schema,
                    const segcore::SealedIndexingEntry& entry,
                    const SearchInfo& search_info,
                    const void* query_data,
                    int64_t num_queries,
                    const BitsetView& bitset,
                    milvus::OpContext* op_context,
                    SearchResult& search_result) {
    auto topK = search_info.topk_;
    auto round_decimal = search_info.round_decimal_;

    auto field_id = search_info.field_id_;
    auto& field = schema[field_id];
    auto is_sparse = field.get_data_type() == DataType::VECTOR_SPARSE_U32_F32;
    // TODO(SPARSE): see todo in PlanImpl.h::PlaceHolder.
    auto dim = is_sparse ? 0 : field.get_dim();

    AssertInfo(entry.metric_type_ == search_info.metric_type_,
               "Metric type of field index isn't the same with search info,"
               "field index: {}, search info: {}",
               entry.metric_type_,
               search_info.metric_type_);

    auto dataset = knowhere::GenDataSet(num_queries, dim, query_data);
    dataset->SetIsSparse(is_sparse);
    auto accessor = SemiInlineGet(entry.indexing_->PinCells(op_context, {0}));
    auto vec_index =
        dynamic_cast<index::VectorIndex*>(accessor->get_cell_of(0));

    const auto& offset_mapping = vec_index->GetOffsetMapping();
    const bool is_element_level_search = search_info.array_offsets_ != nullptr;
    TargetBitmap transformed_bitset;
    BitsetView search_bitset = bitset;
    const auto has_offset_mapping =
        offset_mapping.IsEnabled() && !is_element_level_search;
    if (has_offset_mapping) {
        if (offset_mapping.GetValidCount() == 0) {
            FillEmptySearchResult(search_result, num_queries, topK);
            return;
        }
        if (!bitset.empty()) {
            auto status =
                offset_mapping.TransformBitset(bitset, transformed_bitset);
            if (status == OffsetMapping::BitsetTransformStatus::AllFiltered) {
                FillEmptySearchResult(search_result, num_queries, topK);
                return;
            }
            search_bitset =
                status == OffsetMapping::BitsetTransformStatus::NoFilter
                    ? BitsetView{}
                    : search_result.PinBitset(std::move(transformed_bitset));
        }
    }

    if (search_info.iterator_v2_info_.has_value()) {
        CachedSearchIterator cached_iter(
            *vec_index, dataset, search_info, search_bitset, op_context);
        cached_iter.NextBatch(search_info, search_result);
        TransformOffset(search_result.seg_offsets_, offset_mapping);
        return;
    }

    bool use_iterator =
        milvus::exec::PrepareVectorIteratorsFromIndex(search_info,
                                                      num_queries,
                                                      dataset,
                                                      search_result,
                                                      search_bitset,
                                                      *vec_index,
                                                      op_context);
    if (!use_iterator) {
        vec_index->Query(
            dataset, search_info, search_bitset, op_context, search_result);
        float* distances = search_result.distances_.data();
        auto total_num = num_queries * topK;
        if (round_decimal != -1) {
            const float multiplier = pow(10.0, round_decimal);
            for (int i = 0; i < total_num; i++) {
                distances[i] =
                    std::round(distances[i] * multiplier) / multiplier;
            }
        }
    }
    TransformOffset(search_result.seg_offsets_, offset_mapping);
    search_result.total_nq_ = num_queries;
    search_result.unity_topK_ = topK;
}

void
SearchOnSealedColumn(const Schema& schema,
                     ChunkedColumnInterface* column,
                     const SearchInfo& search_info,
                     const std::map<std::string, std::string>& index_info,
                     const void* query_data,
                     int64_t num_queries,
                     int64_t row_count,
                     const BitsetView& bitview,
                     milvus::OpContext* op_context,
                     SearchResult& result) {
    auto field_id = search_info.field_id_;
    auto& field = schema[field_id];

    // TODO(SPARSE): see todo in PlanImpl.h::PlaceHolder.
    auto dim = field.get_data_type() == DataType::VECTOR_SPARSE_FLOAT
                   ? 0
                   : field.get_dim();

    query::dataset::SearchDataset query_dataset{search_info.metric_type_,
                                                num_queries,
                                                search_info.topk_,
                                                search_info.round_decimal_,
                                                dim,
                                                query_data};

    auto data_type = field.get_data_type();
    CheckBruteForceSearchParam(field, search_info);

    if (column->IsNullable()) {
        column->BuildValidRowIds(op_context);
    }

    // Check for nullable vector field with all null values - must be done before creating iterators
    const auto& offset_mapping = column->GetOffsetMapping();
    // Element-level VECTOR_ARRAY search has already expanded the row bitset
    // to element IDs. OffsetMapping is row-level, so only use it for row-level
    // vector searches.
    bool is_element_level_search =
        field.get_data_type() == DataType::VECTOR_ARRAY &&
        search_info.array_offsets_ != nullptr;
    TargetBitmap transformed_bitset;
    BitsetView search_bitview = bitview;
    const auto has_offset_mapping =
        offset_mapping.IsEnabled() && !is_element_level_search;
    if (has_offset_mapping) {
        if (offset_mapping.GetValidCount() == 0) {
            // All vectors are null, return empty result
            FillEmptySearchResult(result, num_queries, search_info.topk_);
            return;
        }
        if (!bitview.empty()) {
            auto status =
                offset_mapping.TransformBitset(bitview, transformed_bitset);
            if (status == OffsetMapping::BitsetTransformStatus::AllFiltered) {
                FillEmptySearchResult(result, num_queries, search_info.topk_);
                return;
            }
            search_bitview =
                status == OffsetMapping::BitsetTransformStatus::NoFilter
                    ? BitsetView{}
                    : result.PinBitset(std::move(transformed_bitset));
        }
    }

    if (search_info.iterator_v2_info_.has_value()) {
        CachedSearchIterator cached_iter(
            column, query_dataset, search_info, index_info, bitview, data_type);
        cached_iter.NextBatch(search_info, result);
        if (offset_mapping.IsEnabled()) {
            TransformOffset(result.seg_offsets_, offset_mapping);
        }
        return;
    }

    const bool use_vector_iterator =
        milvus::exec::UseVectorIterator(search_info);
    auto num_chunk = column->num_chunks();

    SubSearchResult final_qr(num_queries,
                             search_info.topk_,
                             search_info.metric_type_,
                             search_info.round_decimal_);

    auto offset = 0;
    auto vector_chunks = column->GetAllChunks(op_context);
    for (int i = 0; i < num_chunk; ++i) {
        const auto& pw = vector_chunks[i];
        auto vec_data = pw.get()->Data();
        auto chunk_size = column->chunk_row_nums(i);
        auto raw_dataset =
            query::dataset::RawDataset{offset, dim, chunk_size, vec_data};
        if (milvus::exec::UseVectorIterator(search_info)) {
            auto sub_qr =
                PackBruteForceSearchIteratorsIntoSubResult(query_dataset,
                                                           raw_dataset,
                                                           search_info,
                                                           index_info,
                                                           search_bitview,
                                                           data_type);
            final_qr.merge(sub_qr);
        } else {
            auto sub_qr = BruteForceSearch(query_dataset,
                                           raw_dataset,
                                           search_info,
                                           index_info,
                                           bitview,
                                           data_type);
            final_qr.merge(sub_qr);
        }
        offset += chunk_size;
    }
    if (use_vector_iterator) {
        bool larger_is_closer = PositivelyRelated(search_info.metric_type_);
        result.AssembleChunkVectorIterators(num_queries,
                                            num_chunk,
                                            column->GetNumRowsUntilChunk(),
                                            final_qr.chunk_iterators(),
                                            offset_mapping,
                                            larger_is_closer);
    } else {
        result.distances_ = std::move(final_qr.mutable_distances());
        result.seg_offsets_ = std::move(final_qr.mutable_seg_offsets());
    }
    result.unity_topK_ = query_dataset.topk;
    result.total_nq_ = query_dataset.num_queries;
}

}  // namespace milvus::query
