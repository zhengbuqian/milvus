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

#include <limits>
#include <memory>
#include <string>

#include <utility>
#include <vector>

#include "common/StructElementOffsets.h"
#include "common/BitsetView.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "common/OffsetMapping.h"
#include "common/QueryResult.h"
#include "common/QueryInfo.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/VectorArray.h"
#include "knowhere/array_store.h"

namespace milvus::query {

struct FlattenedVectorArrayRows {
    std::unique_ptr<uint8_t[]> payload;
    int64_t element_count{0};
    std::vector<size_t> row_offsets;
};

// Growing VECTOR_ARRAY chunks hold one VectorArrayView per row. Flatten the
// rows' compact payloads for Knowhere, optionally retaining row boundaries
// for embedding-list search.
inline FlattenedVectorArrayRows
FlattenVectorArrayRows(const VectorArrayView* rows,
                       int64_t row_count,
                       bool include_row_offsets) {
    AssertInfo(row_count >= 0 && (row_count == 0 || rows != nullptr),
               "invalid VECTOR_ARRAY row range");
    size_t payload_bytes = 0;
    for (int64_t i = 0; i < row_count; ++i) {
        payload_bytes += rows[i].byte_size();
    }

    FlattenedVectorArrayRows result;
    result.payload = std::make_unique<uint8_t[]>(payload_bytes);
    if (include_row_offsets) {
        result.row_offsets.reserve(row_count + 1);
        result.row_offsets.push_back(0);
    }
    auto* dst = result.payload.get();
    for (int64_t i = 0; i < row_count; ++i) {
        const auto row_bytes = rows[i].byte_size();
        if (row_bytes > 0) {
            milvus::fastmem::FastMemcpy(dst, rows[i].data(), row_bytes);
            dst += row_bytes;
        }
        result.element_count += rows[i].physical_length();
        if (include_row_offsets) {
            result.row_offsets.push_back(result.element_count);
        }
    }
    return result;
}

inline bool
CanUseStrictGroupFilteredIterator(const SearchInfo& info, int64_t nq) {
    return info.strict_group_acceptance_threshold_ > 0 &&
           info.strict_group_size_ && info.group_size_ > 1 && info.topk_ > 0 &&
           nq == 1 && !info.element_level() &&
           info.group_by_field_ids_.size() == 1;
}

inline void
FillEmptySearchResult(SearchResult& result, int64_t num_queries, int64_t topk) {
    auto total_num = num_queries * topk;
    result.seg_offsets_.resize(total_num, INVALID_SEG_OFFSET);
    result.distances_.resize(total_num, 0.0f);
    result.total_nq_ = num_queries;
    result.unity_topK_ = topk;
}

inline BitsetView
AttachOffsetMappingIds(const BitsetView& bitset,
                       const OffsetMappingIdView& ids) {
    auto mapped = bitset;
    if (!ids.empty()) {
        // BF scans local physical ids. The mapping view is already clipped to
        // one contiguous p2l window, so the backend can use ids directly.
        knowhere::IdArray out_ids(ids.data, static_cast<size_t>(ids.count));
        mapped.set_id_offset(0);
        mapped.set_out_ids(out_ids, out_ids.size());
        mapped.set_vector_count(static_cast<size_t>(ids.count));
    }
    return mapped;
}

inline const void*
AdvanceVectorDataPointer(const void* data,
                         DataType data_type,
                         int64_t dim,
                         int64_t rows) {
    if (rows == 0) {
        return data;
    }
    if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
        return static_cast<const knowhere::sparse::SparseRow<SparseValueType>*>(
                   data) +
               rows;
    }
    return static_cast<const uint8_t*>(data) +
           rows * static_cast<int64_t>(GetDataTypeSize(data_type, dim));
}

// Map VECTOR_ARRAY element IDs returned by Knowhere to (doc_id, elem_idx)
// pairs via StructElementOffsets. This is element-space only; row-level nullable
// mapping is handled before or inside Knowhere search.
inline std::pair<std::vector<int64_t>, std::vector<int32_t>>
ApplyElementIDMapping(
    const std::vector<int64_t>& element_ids,
    const milvus::IStructElementOffsets& struct_element_offsets) {
    std::vector<int64_t> doc_offsets;
    std::vector<int32_t> element_indices;
    doc_offsets.reserve(element_ids.size());
    element_indices.reserve(element_ids.size());
    for (size_t i = 0; i < element_ids.size(); i++) {
        if (element_ids[i] == INVALID_SEG_OFFSET) {
            doc_offsets.push_back(INVALID_SEG_OFFSET);
            element_indices.push_back(-1);
        } else {
            auto [doc_id, elem_index] =
                struct_element_offsets.ElementIDToRowID(element_ids[i]);
            doc_offsets.push_back(doc_id);
            element_indices.push_back(elem_index);
        }
    }
    return std::make_pair(std::move(doc_offsets), std::move(element_indices));
}

// Convert VECTOR_ARRAY element IDs to (row_id, elem_idx). Row-level vector
// search already receives logical IDs from Knowhere: indexed paths use IdMap,
// raw BF paths pass physical->logical IDs through BitsetView.
inline void
FinalizeVectorSearchOffsets(
    SearchResult& result,
    const milvus::IStructElementOffsets* struct_element_offsets) {
    if (struct_element_offsets != nullptr) {
        auto [doc_offsets, elem_indices] =
            ApplyElementIDMapping(result.seg_offsets_, *struct_element_offsets);
        result.seg_offsets_ = std::move(doc_offsets);
        result.element_indices_ = std::move(elem_indices);
        result.element_level_ = true;
    }
}

template <typename T, typename U>
inline bool
Match(const T& x, const U& y, OpType op) {
    ThrowInfo(NotImplemented, "not supported");
}

template <>
inline bool
Match<std::string>(const std::string& str, const std::string& val, OpType op) {
    switch (op) {
        case OpType::PrefixMatch:
            return PrefixMatch(str, val);
        case OpType::PostfixMatch:
            return PostfixMatch(str, val);
        case OpType::InnerMatch:
            return InnerMatch(str, val);
        default:
            ThrowInfo(OpTypeInvalid, "not supported");
    }
}

template <>
inline bool
Match<std::string_view>(const std::string_view& str,
                        const std::string& val,
                        OpType op) {
    switch (op) {
        case OpType::PrefixMatch:
            return PrefixMatch(str, val);
        case OpType::PostfixMatch:
            return PostfixMatch(str, val);
        case OpType::InnerMatch:
            return InnerMatch(str, val);
        default:
            ThrowInfo(OpTypeInvalid, "not supported");
    }
}

// Overloads for string_view combinations used when CompareExpr operands
// hold string_view in the data_access_type variant (chunk access), or a
// mix of string (index access) and string_view (chunk access).
inline bool
Match(const std::string_view& str, const std::string_view& val, OpType op) {
    switch (op) {
        case OpType::PrefixMatch:
            return PrefixMatch(str, val);
        case OpType::PostfixMatch:
            return PostfixMatch(str, val);
        case OpType::InnerMatch:
            return InnerMatch(str, val);
        default:
            ThrowInfo(OpTypeInvalid, "not supported");
    }
}

inline bool
Match(const std::string& str, const std::string_view& val, OpType op) {
    switch (op) {
        case OpType::PrefixMatch:
            return PrefixMatch(str, val);
        case OpType::PostfixMatch:
            return PostfixMatch(str, val);
        case OpType::InnerMatch:
            return InnerMatch(str, val);
        default:
            ThrowInfo(OpTypeInvalid, "not supported");
    }
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline bool
gt_ub(int64_t t) {
    return t > std::numeric_limits<T>::max();
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline bool
lt_lb(int64_t t) {
    return t < std::numeric_limits<T>::min();
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline bool
out_of_range(int64_t t) {
    return gt_ub<T>(t) || lt_lb<T>(t);
}

inline bool
dis_closer(float dis1, float dis2, const MetricType& metric_type) {
    if (PositivelyRelated(metric_type))
        return dis1 > dis2;
    return dis1 < dis2;
}

}  // namespace milvus::query
