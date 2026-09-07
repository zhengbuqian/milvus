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

#include "common/Types.h"
#include "index/contracts/IndexBuilder.h"
#include "index/scalar/ngram/JsonProjectedString.h"
#include "storage/artifact/Artifact.h"

// The BUILDER of the ngram family. §6.1, §6.1.1 (form **A, truly streaming** —
// tantivy), §8.

namespace milvus::tantivy {
struct TantivyIndexWrapper;
}

namespace milvus::index {

class NgramIndexDirectory;
class NgramBuilderCore;

struct NgramBuildParams {
    std::string field_name;
    DataType value_type{DataType::VARCHAR};
    uintptr_t min_gram{0};
    uintptr_t max_gram{0};
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
        Seal() &&
        override;

 private:
    std::unique_ptr<NgramBuilderCore> core_;
};

// JSON path projection has three states that cannot be encoded by
// IndexBuilder<string_view>'s value + validity pair: a field NULL, a present
// row with no projected string, and a projected string (including ""). Keep
// that shape family-local while reusing the exact same NGRAM writer core.
class JsonNgramIndexBuilder final : public IndexBuilder<JsonProjectedString> {
 public:
    explicit JsonNgramIndexBuilder(NgramBuildParams params);

    ~JsonNgramIndexBuilder() override;

    BuilderInputSpec
    InputSpec() const override;

    // `values[i].state` is the only null/presence source. `valid` must be null;
    // supplying a second validity channel is a protocol error.
    void
    Add(size_t n,
        const JsonProjectedString* values,
        const bool* valid) override;

    storage::ArtifactPtr
        Seal() &&
        override;

 private:
    std::unique_ptr<NgramBuilderCore> core_;
};

}  // namespace milvus::index
