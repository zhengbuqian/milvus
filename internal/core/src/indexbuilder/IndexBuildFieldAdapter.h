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

#pragma once

#include <cstddef>
#include <cstdint>

#include "common/FieldData.h"
#include "common/Types.h"

namespace milvus::storage {
struct FieldDataMeta;
struct FileManagerContext;
}  // namespace milvus::storage

namespace milvus::indexbuilder {

// Native projection of the field schema needed by build orchestration.
// Protobuf/default-value interpretation remains in the adapter implementation.
struct BuildFieldSpec {
    DataType field_type{DataType::NONE};
    DataType element_type{DataType::NONE};
    bool nullable{false};
    size_t estimated_missing_row_bytes{1};
};

BuildFieldSpec
AdaptBuildField(const storage::FileManagerContext& context);

// Optional-field protobuf construction stays at this explicit adapter
// boundary. The storage visitor receives a nullable-capable schema so corrupt
// NULL partition-key rows remain observable by the materializer.
storage::FieldDataMeta
AdaptOptionalBuildFieldMeta(const storage::FileManagerContext& context,
                            FieldId field_id,
                            DataType field_type,
                            DataType element_type);

FieldDataPtr
CreateMissingFieldData(const storage::FileManagerContext& context,
                       int64_t rows);

}  // namespace milvus::indexbuilder
