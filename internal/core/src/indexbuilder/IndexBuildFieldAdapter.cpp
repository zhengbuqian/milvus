// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "indexbuilder/IndexBuildFieldAdapter.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "storage/FileManager.h"
#include "storage/Util.h"

namespace milvus::indexbuilder {

BuildFieldSpec
AdaptBuildField(const storage::FileManagerContext& context) {
    const auto& schema = context.fieldDataMeta.field_schema;
    BuildFieldSpec result;
    result.field_type = static_cast<DataType>(schema.data_type());
    result.element_type = static_cast<DataType>(schema.element_type());
    result.nullable = schema.nullable();
    if (schema.has_default_value()) {
        result.estimated_missing_row_bytes =
            std::max<size_t>(1, schema.default_value().ByteSizeLong());
    }
    return result;
}

storage::FieldDataMeta
AdaptOptionalBuildFieldMeta(const storage::FileManagerContext& context,
                            FieldId field_id,
                            DataType field_type,
                            DataType element_type) {
    proto::schema::FieldSchema schema;
    schema.set_fieldid(field_id.get());
    schema.set_data_type(static_cast<proto::schema::DataType>(field_type));
    schema.set_element_type(static_cast<proto::schema::DataType>(element_type));
    schema.set_nullable(true);
    return {context.fieldDataMeta.collection_id,
            context.fieldDataMeta.partition_id,
            context.fieldDataMeta.segment_id,
            field_id.get(),
            std::move(schema)};
}

FieldDataPtr
CreateMissingFieldData(const storage::FileManagerContext& context,
                       int64_t rows) {
    AssertInfo(rows >= 0 && static_cast<uint64_t>(rows) <=
                                static_cast<uint64_t>(
                                    std::numeric_limits<ssize_t>::max()),
               "missing-row batch size is out of range: {}",
               rows);
    const auto& schema = context.fieldDataMeta.field_schema;
    const auto field_type = static_cast<DataType>(schema.data_type());
    const auto element_type = static_cast<DataType>(schema.element_type());
    if (IsVectorDataType(field_type)) {
        AssertInfo(schema.nullable(),
                   "missing vector rows require a nullable field");
        AssertInfo(!schema.has_default_value(),
                   "vector fields cannot synthesize a default value");
        const auto dim = context.indexMeta.dim;
        AssertInfo(dim > 0 || (dim == 0 &&
                               field_type == DataType::VECTOR_SPARSE_U32_F32),
                   "missing vector rows have invalid dimension {}",
                   dim);
        auto batch = storage::CreateFieldData(field_type,
                                              element_type,
                                              /*nullable=*/true,
                                              dim,
                                              /*total_num_rows=*/0);
        AssertInfo(batch != nullptr,
                   "failed to allocate missing-vector field-data batch");
        if (rows != 0) {
            std::vector<uint8_t> validity((static_cast<size_t>(rows) + 7) / 8,
                                          0);
            batch->FillFieldData(
                nullptr, validity.data(), static_cast<ssize_t>(rows), 0);
        }
        return batch;
    }

    std::optional<DefaultValueType> default_value;
    if (schema.has_default_value()) {
        default_value = schema.default_value();
    }

    auto batch = storage::CreateFieldData(field_type,
                                          element_type,
                                          /*nullable=*/true,
                                          /*dim=*/1,
                                          rows);
    AssertInfo(batch != nullptr,
               "failed to allocate missing-row field-data batch");
    batch->FillFieldData(default_value, static_cast<ssize_t>(rows));
    return batch;
}

}  // namespace milvus::indexbuilder
