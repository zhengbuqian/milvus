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
#include <string>
#include <string_view>
#include <vector>

#include "index/contracts/IndexBuilder.h"
#include "index/scalar/spatial/RTreeEngine.h"
#include "storage/artifact/Artifact.h"

// The BUILDER of the spatial family. §6.1, §6.1.1 (form **B, fully resident** —
// `bulk_load_from_field_data`, RTreeIndex.cpp:366), §8.
//
// `T = std::string_view` because the input is WKB bytes (§6.1: variable-length
// values are expressed as view types).

namespace milvus::index {

struct RTreeBuildParams {
    // Local directory the .bgi / .meta.json pair is written into before it is
    // handed to a `storage::FileSink`.
    std::string local_dir;
};

class RTreeIndexBuilder final : public IndexBuilder<std::string_view> {
 public:
    explicit RTreeIndexBuilder(RTreeBuildParams params);

    ~RTreeIndexBuilder() override;

    BuilderInputSpec
    InputSpec() const override;

    // WKB decoding lives here, not in the engine (see RTreeEngine.h). A row
    // whose WKB fails to parse contributes no entry but still advances the
    // offset — that skip is what keeps index offsets aligned with row numbers.
    void
    Add(size_t n, const std::string_view* values, const bool* valid) override;

    storage::ArtifactPtr
    Seal() && override;

 private:
    RTreeBuildParams params_;
    std::unique_ptr<RTreeBuildEngine> engine_;
    std::vector<size_t> null_offsets_;
    int64_t total_num_rows_{0};
};

}  // namespace milvus::index
