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

#include "index/scalar/inverted/InvertedIndexBuilder.h"

#include <utility>

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/inverted/InvertedIndexArtifact.h"

namespace milvus::index {

template <typename T>
InvertedIndexBuilder<T>::InvertedIndexBuilder(InvertedBuildParams params)
    : params_(std::move(params)) {
    // TODO: move existing logic here (see InvertedIndexTantivy.cpp:72-92
    // InitForBuildIndex and the build half of the ctor at :94-114).
    //
    // The ctor's `if (ctx.for_loading_index) return;` branch (:110-112) is what
    // made ONE constructor serve both build and load. Loading is
    // InvertedIndexLoader; there is no flag here.
}

template <typename T>
InvertedIndexBuilder<T>::~InvertedIndexBuilder() = default;

template <typename T>
BuilderInputSpec
InvertedIndexBuilder<T>::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Streaming,
                            .needs_second_pass = false};
}

template <typename T>
void
InvertedIndexBuilder<T>::Add(size_t n, const T* values, const bool* valid) {
    // TODO: move existing logic here (see InvertedIndexTantivy.cpp:644-734
    // BuildWithFieldData — the `add_data` slice loop at :686 and the
    // null-offset collection).
}

template <typename T>
storage::ArtifactPtr
InvertedIndexBuilder<T>::Seal() && {
    // TODO: move existing logic here (see InvertedIndexTantivy.cpp:131-135
    // finish(), i.e. commit + merge), then hand the local directory, the null
    // offsets and `params_.nested` to an InvertedIndexArtifact.
    return nullptr;
}

namespace {

template <typename T>
bool
RegisterInvertedBuilder() {
    BuilderRegistry<T>::Instance().Register(
        families::kInverted, [](const BuildParams& params) {
            // TODO: read TANTIVY_INDEX_VERSION / SCALAR_INDEX_ENGINE_VERSION
            // (index/Meta.h:54-58) out of `params`.
            return std::make_unique<InvertedIndexBuilder<T>>(
                InvertedBuildParams{});
        });
    return true;
}

const bool kRegistered = RegisterInvertedBuilder<bool>() &&
                         RegisterInvertedBuilder<int8_t>() &&
                         RegisterInvertedBuilder<int16_t>() &&
                         RegisterInvertedBuilder<int32_t>() &&
                         RegisterInvertedBuilder<int64_t>() &&
                         RegisterInvertedBuilder<float>() &&
                         RegisterInvertedBuilder<double>() &&
                         RegisterInvertedBuilder<std::string_view>();

}  // namespace

#define INSTANTIATE_INVERTED_BUILDER(T) template class InvertedIndexBuilder<T>;
INSTANTIATE_INVERTED_BUILDER(bool)
INSTANTIATE_INVERTED_BUILDER(int8_t)
INSTANTIATE_INVERTED_BUILDER(int16_t)
INSTANTIATE_INVERTED_BUILDER(int32_t)
INSTANTIATE_INVERTED_BUILDER(int64_t)
INSTANTIATE_INVERTED_BUILDER(float)
INSTANTIATE_INVERTED_BUILDER(double)
INSTANTIATE_INVERTED_BUILDER(std::string_view)
#undef INSTANTIATE_INVERTED_BUILDER

}  // namespace milvus::index
