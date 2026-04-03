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

#include "common/VortexChunk.h"

#include <memory>
#include <string>

#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Types.h"
#include "common/VectorTrait.h"
#include "common/VortexDataView.h"
#include "fmt/core.h"

namespace milvus {

namespace {

VortexDataViewCore
MakeCore(std::shared_ptr<milvus_storage::api::Reader> reader,
         std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader,
         const std::vector<int64_t>& chunk_indices,
         int column_in_batch,
         FieldId field_id,
         int64_t row_start,
         int64_t total_rows,
         bool nullable) {
    return VortexDataViewCore{
        std::move(reader),
        std::move(chunk_reader),
        chunk_indices,
        column_in_batch,
        std::to_string(field_id.get()),
        row_start,
        total_rows,
        nullable,
    };
}

}  // namespace

std::vector<std::string>
VortexChunk::BulkOwnData(const int64_t* local_offsets, int64_t count) const {
    // Translate chunk-local offsets to segment-level indices, sort and dedup.
    std::vector<std::pair<int64_t, int64_t>> idx_pairs;
    idx_pairs.reserve(count);
    for (int64_t i = 0; i < count; ++i) {
        idx_pairs.emplace_back(row_start_ + local_offsets[i], i);
    }
    std::sort(idx_pairs.begin(), idx_pairs.end());

    std::vector<int64_t> sorted_indices;
    std::vector<std::vector<int64_t>> dedup_to_orig;
    sorted_indices.reserve(count);
    dedup_to_orig.reserve(count);
    for (auto& [idx, orig] : idx_pairs) {
        if (sorted_indices.empty() || sorted_indices.back() != idx) {
            sorted_indices.push_back(idx);
            dedup_to_orig.push_back({orig});
        } else {
            dedup_to_orig.back().push_back(orig);
        }
    }

    auto result = reader_->take(sorted_indices);
    AssertInfo(result.ok(),
               "VortexChunk::BulkOwnData: take() failed: {}",
               result.status().ToString());
    auto table = *result;

    auto field_name = GetFieldName();
    int col_idx = table->schema()->GetFieldIndex(field_name);
    AssertInfo(col_idx >= 0,
               "VortexChunk::BulkOwnData: field '{}' not found in result",
               field_name);
    auto col = table->column(col_idx);

    // Flatten Arrow column into owned strings in sorted (deduped) order.
    std::vector<std::string> sorted_strings;
    sorted_strings.reserve(sorted_indices.size());
    for (int ci = 0; ci < col->num_chunks(); ++ci) {
        auto arr = col->chunk(ci);
        for (int64_t j = 0; j < arr->length(); ++j) {
            auto sv = GetBinaryView(arr, j);
            sorted_strings.emplace_back(sv.data(), sv.size());
        }
    }

    // Scatter into output order; duplicates are copied, last gets moved.
    std::vector<std::string> out(count);
    for (size_t di = 0; di < dedup_to_orig.size(); ++di) {
        auto& positions = dedup_to_orig[di];
        for (size_t k = 0; k + 1 < positions.size(); ++k) {
            out[positions[k]] = sorted_strings[di];
        }
        out[positions.back()] = std::move(sorted_strings[di]);
    }
    return out;
}

AnyDataView
VortexChunk::GetAnyDataView(int64_t offset, int64_t length) const {
    AssertInfo(offset >= 0 && length > 0 && offset + length <= row_nums_,
               "VortexChunk::GetAnyDataView: range [{}, {}) out of bounds ({})",
               offset, offset + length, row_nums_);

    auto data_type = field_meta_.get_data_type();
    bool nullable = field_meta_.is_nullable();

    // Adjust row_start to skip 'offset' rows within this chunk
    auto core = MakeCore(reader_, chunk_reader_, chunk_indices_,
                         column_in_batch_, field_id_,
                         row_start_ + offset, length, nullable);

    switch (data_type) {
        case DataType::BOOL:
            return AnyDataView(
                std::make_shared<VortexBoolDataView>(std::move(core)));
        case DataType::INT8:
            return AnyDataView(
                std::make_shared<VortexNumericDataView<int8_t>>(
                    std::move(core)));
        case DataType::INT16:
            return AnyDataView(
                std::make_shared<VortexNumericDataView<int16_t>>(
                    std::move(core)));
        case DataType::INT32:
            return AnyDataView(
                std::make_shared<VortexNumericDataView<int32_t>>(
                    std::move(core)));
        case DataType::INT64:
            return AnyDataView(
                std::make_shared<VortexNumericDataView<int64_t>>(
                    std::move(core)));
        case DataType::FLOAT:
            return AnyDataView(
                std::make_shared<VortexNumericDataView<float>>(
                    std::move(core)));
        case DataType::DOUBLE:
            return AnyDataView(
                std::make_shared<VortexNumericDataView<double>>(
                    std::move(core)));
        case DataType::VARCHAR:
        case DataType::STRING:
        case DataType::JSON:
        case DataType::ARRAY:
            // All variable-length types return string_view DataView.
            // Callers use as<Json>()/as<ArrayView>() adapter conversion.
            return AnyDataView(
                std::make_shared<VortexStringDataView>(std::move(core)));
        default:
            ThrowInfo(
                NotImplemented,
                "VortexChunk::GetAnyDataView(range): unsupported data type {}",
                static_cast<int>(data_type));
    }
}

AnyDataView
VortexChunk::GetAnyDataView(const FixedVector<int32_t>& offsets) const {
    ThrowInfo(NotImplemented,
              "VortexChunk::GetAnyDataView(offsets) is not implemented");
}

AnyDataView
VortexChunk::GetAnyDataView() const {
    auto data_type = field_meta_.get_data_type();
    bool nullable = field_meta_.is_nullable();

    auto core = MakeCore(reader_, chunk_reader_, chunk_indices_,
                         column_in_batch_, field_id_, row_start_, row_nums_,
                         nullable);

    switch (data_type) {
        case DataType::BOOL:
            return AnyDataView(
                std::make_shared<VortexBoolDataView>(std::move(core)));
        case DataType::INT8:
            return AnyDataView(
                std::make_shared<VortexNumericDataView<int8_t>>(
                    std::move(core)));
        case DataType::INT16:
            return AnyDataView(
                std::make_shared<VortexNumericDataView<int16_t>>(
                    std::move(core)));
        case DataType::INT32:
            return AnyDataView(
                std::make_shared<VortexNumericDataView<int32_t>>(
                    std::move(core)));
        case DataType::INT64:
            return AnyDataView(
                std::make_shared<VortexNumericDataView<int64_t>>(
                    std::move(core)));
        case DataType::FLOAT:
            return AnyDataView(
                std::make_shared<VortexNumericDataView<float>>(
                    std::move(core)));
        case DataType::DOUBLE:
            return AnyDataView(
                std::make_shared<VortexNumericDataView<double>>(
                    std::move(core)));
        case DataType::VARCHAR:
        case DataType::STRING:
        case DataType::JSON:
        case DataType::ARRAY:
            return AnyDataView(
                std::make_shared<VortexStringDataView>(std::move(core)));
        case DataType::VECTOR_FLOAT:
            return AnyDataView(
                std::make_shared<VortexVectorDataView<FloatVector>>(
                    std::move(core), field_meta_.get_dim()));
        case DataType::VECTOR_BINARY:
            return AnyDataView(
                std::make_shared<VortexVectorDataView<BinaryVector>>(
                    std::move(core), field_meta_.get_dim()));
        case DataType::VECTOR_FLOAT16:
            return AnyDataView(
                std::make_shared<VortexVectorDataView<Float16Vector>>(
                    std::move(core), field_meta_.get_dim()));
        case DataType::VECTOR_BFLOAT16:
            return AnyDataView(
                std::make_shared<VortexVectorDataView<BFloat16Vector>>(
                    std::move(core), field_meta_.get_dim()));
        default:
            ThrowInfo(
                NotImplemented,
                "VortexChunk::GetAnyDataView: unsupported data type {}",
                static_cast<int>(data_type));
    }
}

const std::type_info&
VortexChunk::GetTypeInfo() const {
    auto data_type = field_meta_.get_data_type();
    switch (data_type) {
        case DataType::BOOL:
            return typeid(bool);
        case DataType::INT8:
            return typeid(int8_t);
        case DataType::INT16:
            return typeid(int16_t);
        case DataType::INT32:
            return typeid(int32_t);
        case DataType::INT64:
            return typeid(int64_t);
        case DataType::FLOAT:
            return typeid(float);
        case DataType::DOUBLE:
            return typeid(double);
        case DataType::VARCHAR:
        case DataType::STRING:
            return typeid(std::string_view);
        case DataType::JSON:
            return typeid(Json);
        case DataType::ARRAY:
            return typeid(std::string_view);
        case DataType::VECTOR_FLOAT:
            return typeid(FloatVector);
        case DataType::VECTOR_BINARY:
            return typeid(BinaryVector);
        case DataType::VECTOR_FLOAT16:
            return typeid(Float16Vector);
        case DataType::VECTOR_BFLOAT16:
            return typeid(BFloat16Vector);
        default:
            return typeid(void);
    }
}

}  // namespace milvus
