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

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <numeric>
#include <string>

#include "common/BitsetView.h"
#include "common/Consts.h"
#include "common/FieldMeta.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "query/Utils.h"

namespace milvus::segcore {

inline bool
UseMockAnnRandomResults() {
    auto flag = std::getenv("MILVUS_MOCK_ANN_RANDOM_RESULTS");
    return flag != nullptr && std::string(flag) == "1";
}

inline uint64_t
MixMockAnnSeed(uint64_t value) {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

inline uint64_t
GetMockAnnSeed() {
    auto seed = 0x853c49e6748fea9bULL;
    auto seed_env = std::getenv("MILVUS_MOCK_ANN_RANDOM_SEED");
    if (seed_env != nullptr) {
        seed = std::strtoull(seed_env, nullptr, 10);
    }
    return seed;
}

inline bool
CanUseMockAnnRandomResults(const SearchInfo& search_info,
                           const FieldMeta& field_meta,
                           const size_t* query_offsets) {
    if (!UseMockAnnRandomResults()) {
        return false;
    }
    if (field_meta.is_nullable() ||
        field_meta.get_data_type() == DataType::VECTOR_ARRAY) {
        return false;
    }
    if (query_offsets != nullptr || search_info.array_offsets_ != nullptr ||
        search_info.iterator_v2_info_.has_value() ||
        search_info.group_by_field_id_.has_value() ||
        search_info.iterative_filter_execution) {
        return false;
    }
    return true;
}

inline int64_t
PickMockAnnStride(uint64_t seed, int64_t modulus) {
    if (modulus <= 1) {
        return 1;
    }

    auto stride = static_cast<int64_t>((seed % (modulus - 1)) + 1);
    for (int64_t i = 0; i < std::min<int64_t>(modulus, 1024); ++i) {
        if (std::gcd(stride, modulus) == 1) {
            return stride;
        }
        stride++;
        if (stride >= modulus) {
            stride = 1;
        }
    }
    return 1;
}

inline float
MockAnnDistance(const SearchInfo& search_info,
                int64_t topk,
                int64_t rank,
                uint64_t seed) {
    auto unique = static_cast<float>(MixMockAnnSeed(seed) & 0xFFF);
    constexpr float kRankStride = 4096.0F;
    if (PositivelyRelated(search_info.metric_type_)) {
        return static_cast<float>(topk - rank) * kRankStride - unique;
    }
    return static_cast<float>(rank) * kRankStride + unique;
}

inline void
FillMockAnnRandomResults(const SearchInfo& search_info,
                         int64_t query_count,
                         int64_t row_count,
                         const BitsetView& bitset,
                         int64_t segment_id,
                         SearchResult& output) {
    auto topk = search_info.topk_;
    if (query_count <= 0 || topk <= 0 || row_count <= 0) {
        query::FillEmptySearchResult(output,
                                     std::max<int64_t>(query_count, 0),
                                     std::max<int64_t>(topk, 0));
        return;
    }

    auto sample_count = row_count;
    if (!bitset.empty()) {
        sample_count = std::min<int64_t>(sample_count,
                                         static_cast<int64_t>(bitset.size()));
    }
    if (sample_count <= 0) {
        query::FillEmptySearchResult(output, query_count, topk);
        return;
    }
    if (!bitset.empty() &&
        bitset.get_first_valid_index() >= static_cast<size_t>(sample_count)) {
        query::FillEmptySearchResult(output, query_count, topk);
        return;
    }

    auto total = query_count * topk;
    output.seg_offsets_.assign(total, INVALID_SEG_OFFSET);
    output.distances_.assign(total, 0.0F);
    output.total_nq_ = query_count;
    output.unity_topK_ = topk;
    output.topk_per_nq_prefix_sum_.clear();
    output.primary_keys_.clear();
    output.result_offsets_.clear();
    output.output_fields_data_.clear();
    output.group_by_values_.reset();
    output.group_size_.reset();
    output.vector_iterators_.reset();
    output.element_level_ = false;
    output.element_indices_.clear();
    output.chunk_buffers_.clear();

    auto seed =
        MixMockAnnSeed(GetMockAnnSeed()) ^
        MixMockAnnSeed(static_cast<uint64_t>(segment_id)) ^
        MixMockAnnSeed(static_cast<uint64_t>(search_info.field_id_.get()));
    for (int64_t query_index = 0; query_index < query_count; ++query_index) {
        auto query_seed =
            MixMockAnnSeed(seed ^ static_cast<uint64_t>(query_index));
        auto base = static_cast<int64_t>(query_seed %
                                         static_cast<uint64_t>(sample_count));
        auto stride = PickMockAnnStride(MixMockAnnSeed(query_seed),
                                        static_cast<int64_t>(sample_count));

        int64_t filled = 0;
        auto offset = base;
        for (int64_t attempt = 0; attempt < sample_count && filled < topk;
             ++attempt) {
            if (!bitset.empty() && bitset.test(offset)) {
                offset += stride;
                if (offset >= sample_count) {
                    offset %= sample_count;
                }
                continue;
            }
            auto result_index = query_index * topk + filled;
            output.seg_offsets_[result_index] = offset;
            output.distances_[result_index] =
                MockAnnDistance(search_info,
                                topk,
                                filled,
                                query_seed ^ static_cast<uint64_t>(offset));
            filled++;
            offset += stride;
            if (offset >= sample_count) {
                offset %= sample_count;
            }
        }
    }
}

}  // namespace milvus::segcore
