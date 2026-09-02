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

#include <pb/schema.pb.h>
#include <cmath>
#include <memory>
#include <string>

#include "common/EasyAssert.h"
#include "indexbuilder/IndexCreatorBase.h"
#include "index/JsonScalarIndexWrapper.h"
#include "indexbuilder/ScalarIndexCreator.h"
#include "indexbuilder/VecIndexCreator.h"
#include "indexbuilder/type_c.h"
#include "storage/Types.h"
#include "storage/FileManager.h"

namespace milvus::indexbuilder {

// RETIRING — REPLACED BY `index::BuilderRegistry<T>` (index/contracts/Registry.h),
// reached through `indexbuilder::MakeBuildDriver` (indexbuilder/BuildDriver.h).
//
// See core_refactor/01-scalar-index.md §11.2 item 4: "SPLIT THE FACTORY BY
// FAMILY — `CreateIndexInfo` is broken up and per-family loader/builder
// registries replace `IndexFactory`'s God switch." The switch below is that
// God switch's outer half (data type -> scalar vs vector creator); the inner
// half is `index::IndexFactory::CreateIndex`'s dispatch on index type, which
// the same registry replaces.
//
// Two consequences worth stating, because they are the point of the change and
// not side effects:
//   - a family registers itself, so adding one stops being an edit to a shared
//     switch that every family's translation unit already depends on;
//   - `CreateIndexInfo`'s mixed parameter bag disappears; each family reads its
//     own `index::BuildParams`.
//
// consider template factory if too many factories are needed.
class IndexFactory {
 public:
    IndexFactory() = default;
    IndexFactory(const IndexFactory&) = delete;
    IndexFactory
    operator=(const IndexFactory&) = delete;

 public:
    static IndexFactory&
    GetInstance() {
        // thread-safe enough after c++ 11
        static IndexFactory instance;
        return instance;
    }

    IndexCreatorBasePtr
    CreateIndex(DataType type,
                Config& config,
                const storage::FileManagerContext& context) {
        auto invalid_dtype_msg =
            std::string("invalid data type: ") + std::to_string(int(type));

        switch (type) {
            case DataType::BOOL:
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32:
            case DataType::INT64:
            case DataType::FLOAT:
            case DataType::DOUBLE:
            case DataType::VARCHAR:
            case DataType::STRING:
            case DataType::TEXT:
            case DataType::ARRAY:
            case DataType::JSON:
            case DataType::GEOMETRY:
            case DataType::TIMESTAMPTZ:
                return CreateScalarIndex(type, config, context);

            case DataType::VECTOR_FLOAT:
            case DataType::VECTOR_FLOAT16:
            case DataType::VECTOR_BFLOAT16:
            case DataType::VECTOR_BINARY:
            case DataType::VECTOR_SPARSE_U32_F32:
            case DataType::VECTOR_INT8:
            case DataType::VECTOR_ARRAY:
                return std::make_unique<VecIndexCreator>(type, config, context);

            default:
                ThrowInfo(DataTypeInvalid,
                          fmt::format("invalid type is {}", invalid_dtype_msg));
        }
    }
};

}  // namespace milvus::indexbuilder
