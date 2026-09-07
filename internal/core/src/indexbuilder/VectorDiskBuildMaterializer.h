// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "common/FieldData.h"
#include "common/Types.h"
#include "indexbuilder/VectorBuildMaterializer.h"

namespace milvus::index {
class VectorDiskLocalFiles;
}  // namespace milvus::index

namespace milvus::indexbuilder {

// Complete, immutable input staging handed to the family-local disk driver.
// scalar_info_path distinguishes three states: nullopt means no side input was
// delivered, an empty string means delivery completed but the old format needs
// no file, and a non-empty path names the v0 optional-field file.
struct VectorDiskBuildInputs {
    std::shared_ptr<index::VectorDiskLocalFiles> owner;
    std::string raw_path;
    std::string valid_path;
    std::string offsets_path;
    std::optional<std::string> scalar_info_path;
};

// Streams one disk-vector primary column into a uniquely owned local
// generation. FieldData and its payload pointers are borrowed only during Add.
class VectorDiskBuildMaterializer final {
 public:
    VectorDiskBuildMaterializer(std::string staging_parent,
                                DataType field_type,
                                DataType value_type,
                                int64_t dim,
                                bool nullable,
                                int64_t expected_rows,
                                bool side_input_declared);
    ~VectorDiskBuildMaterializer();

    VectorDiskBuildMaterializer(VectorDiskBuildMaterializer&&) noexcept;
    VectorDiskBuildMaterializer&
    operator=(VectorDiskBuildMaterializer&&) noexcept;

    VectorDiskBuildMaterializer(const VectorDiskBuildMaterializer&) = delete;
    VectorDiskBuildMaterializer&
    operator=(const VectorDiskBuildMaterializer&) = delete;

    void
    Add(const FieldDataPtr& batch);

    // Ends primary input exactly once. No Add is valid after this transition.
    void
    FinishPrimary();

    bool
    RequiresEngineBuild() const;

    const VectorPrimaryLayout&
    PrimaryLayout() const;

    void
    SetScalarInfo(VectorScalarInfo scalar_info);

    VectorDiskBuildInputs
    TakeInputs() &&;

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace milvus::indexbuilder
