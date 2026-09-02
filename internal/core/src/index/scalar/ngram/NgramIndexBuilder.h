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
#include "storage/artifact/Artifact.h"
#include "tantivy-wrapper.h"

// The BUILDER of the ngram family. §6.1, §6.1.1 (form **A, truly streaming** —
// tantivy), §8.

namespace milvus::index {

struct NgramBuildParams {
    uintptr_t min_gram{0};
    uintptr_t max_gram{0};
    // Non-empty when building on a JSON path.
    std::string nested_path;
    std::string local_dir;
};

class NgramIndexBuilder final : public IndexBuilder<std::string_view> {
 public:
    explicit NgramIndexBuilder(NgramBuildParams params);

    ~NgramIndexBuilder() override;

    BuilderInputSpec
    InputSpec() const override;

    // Absorbs `BuildWithFieldData` (NgramInvertedIndex.cpp:113-149) — including
    // the running average row size it computes for the query-time cost policy
    // — and `BuildWithJsonFieldData` (:151-193). The JSON variant differs only
    // in how a value is extracted from the raw document before it is fed in,
    // and that extraction now happens in the caller's projection, not in a
    // second build method.
    void
    Add(size_t n, const std::string_view* values, const bool* valid) override;

    storage::ArtifactPtr
    Seal() && override;

 private:
    NgramBuildParams params_;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::vector<size_t> null_offsets_;
    size_t total_bytes_{0};
    int64_t count_{0};
};

}  // namespace milvus::index
