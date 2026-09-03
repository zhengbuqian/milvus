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
#include <string>
#include <string_view>
#include <vector>

#include "common/Types.h"
#include "index/contracts/IndexBuilder.h"
#include "index/fmindex/FMIndex.h"
#include "storage/artifact/Artifact.h"

// The BUILDER of the FM-index family. See §6.1, §6.1.1 (form **B, fully
// resident**: "FM concatenates all docs and hands them to libsais",
// `FMIndex.cpp:172`) and §8.

namespace milvus::index {

// Build-only knobs. Today's `FMIndexParams` (`index/IndexInfo.h:30-33`), which
// reaches the index through `CreateIndexInfo::fmindex_params`.
//
// NOTE that the query-time `cost_ratio` is deliberately NOT here: it is read on
// the query path, not the build path, and merging build-time and query-time
// parameters into one struct is how `CreateIndexInfo` became a parameter bag
// (§11.2 rule 4). It is a constructor argument of the READER instead.
struct FmIndexBuildParams {
    uint32_t sa_sample_rate{8};
    uint32_t block_bytes{64};
};

class FmIndexBuilder final : public IndexBuilder<std::string_view> {
 public:
    explicit FmIndexBuilder(FmIndexBuildParams params);

    ~FmIndexBuilder() override;

    // Form B: `Add` can be single-pass, but the whole corpus must be resident
    // before the structure can take shape (libsais needs the concatenated text).
    BuilderInputSpec
    InputSpec() const override;

    void
    Add(size_t n, const std::string_view* values, const bool* valid) override;

    storage::ArtifactPtr
    Seal() && override;

 private:
    FmIndexBuildParams params_;

    // Accumulated documents + separators, fed to libsais in `Seal()`.
    std::vector<uint8_t> corpus_;
    TargetBitmap null_bitmap_;
    int64_t total_rows_{0};
};

}  // namespace milvus::index
