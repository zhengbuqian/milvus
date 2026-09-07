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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "common/FieldData.h"
#include "common/Types.h"

namespace milvus::indexbuilder {

using VectorScalarInfo =
    std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>>;

// Ordinary-vector logical-to-engine layout retained only when a builder
// declares optional scalar input. The bitmap is packed; no random rank table
// is built. Consumers traverse it once with Cursor.
class VectorPrimaryLayout {
 public:
    struct Row {
        bool valid{false};
        size_t physical_id{0};
    };

    class Cursor {
     public:
        bool
        Done() const;

        Row
        Next();

     private:
        friend class VectorPrimaryLayout;

        explicit Cursor(const VectorPrimaryLayout* layout) : layout_(layout) {
        }

        const VectorPrimaryLayout* layout_{nullptr};
        size_t logical_row_{0};
        size_t physical_row_{0};
    };

    void
    Append(size_t logical_rows, const bool* valid);

    size_t
    LogicalRows() const {
        return logical_rows_;
    }

    size_t
    PhysicalRows() const {
        return physical_rows_;
    }

    Cursor
    NewCursor() const {
        return Cursor(this);
    }

 private:
    bool
    IsValid(size_t logical_row) const;

    std::vector<uint8_t> validity_;
    size_t logical_rows_{0};
    size_t physical_rows_{0};
};

bool
IsSupportedVectorScalarInfoType(DataType type);

// Converts one optional scalar field into Knowhere's physical-vector category
// lists. Batches are consumed synchronously and never retained.
class VectorScalarInfoAccumulator {
 public:
    struct Impl;

    VectorScalarInfoAccumulator(FieldId field_id,
                                DataType field_type,
                                const VectorPrimaryLayout& layout);
    ~VectorScalarInfoAccumulator();

    VectorScalarInfoAccumulator(VectorScalarInfoAccumulator&&) noexcept;
    VectorScalarInfoAccumulator&
    operator=(VectorScalarInfoAccumulator&&) noexcept;

    VectorScalarInfoAccumulator(const VectorScalarInfoAccumulator&) = delete;
    VectorScalarInfoAccumulator&
    operator=(const VectorScalarInfoAccumulator&) = delete;

    void
    Add(const FieldDataPtr& batch);

    VectorScalarInfo
    Finish() &&;

 private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace milvus::indexbuilder
