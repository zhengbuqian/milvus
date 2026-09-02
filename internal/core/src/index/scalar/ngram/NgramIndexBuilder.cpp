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

#include "index/scalar/ngram/NgramIndexBuilder.h"

#include <utility>

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/ngram/NgramIndexArtifact.h"

namespace milvus::index {

NgramIndexBuilder::NgramIndexBuilder(NgramBuildParams params)
    : params_(std::move(params)) {
    // TODO: move existing logic here (see NgramInvertedIndex.cpp:84-103, build
    // branch only — the `params.loading_index` branch at :92 is the loader).
}

NgramIndexBuilder::~NgramIndexBuilder() = default;

BuilderInputSpec
NgramIndexBuilder::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Streaming,
                            .needs_second_pass = false};
}

void
NgramIndexBuilder::Add(size_t n, const std::string_view* values,
                       const bool* valid) {
    // TODO: move existing logic here (see NgramInvertedIndex.cpp:113-149 and
    // :151-193), accumulating `total_bytes_` for the avg-row-size statistic.
}

storage::ArtifactPtr
NgramIndexBuilder::Seal() && {
    // TODO: move existing logic here (see NgramInvertedIndex.cpp:200-216
    // Upload's `finish()` + build-duration log half).
    //
    // WHILE MOVING, one trap disappears: `NgramInvertedIndex::finish()`
    // (NgramInvertedIndex.h:103-106) SHADOWS the non-virtual protected
    // `InvertedIndexTantivy::finish()` (InvertedIndexTantivy.h:329-330) rather
    // than overriding it, so which body runs depends on the static type of the
    // caller. With no inheritance there is one `finish`.
    return nullptr;
}

namespace {

const bool kNgramBuilderRegistered = [] {
    BuilderRegistry<std::string_view>::Instance().Register(
        families::kNgram, [](const BuildParams& params) {
            // TODO: read MIN_GRAM / MAX_GRAM (index/Meta.h:61-62).
            return std::make_unique<NgramIndexBuilder>(NgramBuildParams{});
        });
    return true;
}();

}  // namespace

}  // namespace milvus::index
