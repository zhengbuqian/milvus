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

#include "index/scalar/bitmap/BitmapIndexBuilder.h"

#include <utility>

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/bitmap/BitmapIndexArtifact.h"

namespace milvus::index {

template <typename T>
BitmapIndexBuilder<T>::BitmapIndexBuilder(BitmapBuildParams params)
    : params_(std::move(params)) {
}

template <typename T>
BitmapIndexBuilder<T>::~BitmapIndexBuilder() = default;

template <typename T>
BuilderInputSpec
BitmapIndexBuilder<T>::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Contiguous,
                            .needs_second_pass = false};
}

template <typename T>
void
BitmapIndexBuilder<T>::Add(size_t n, const T* values, const bool* valid) {
    // TODO: move existing logic here (see BitmapIndex.cpp:95-127 and :129-145).
    // Nested rows: BitmapIndex.cpp:215-246, whose per-element `valid_bitset_ =
    // TargetBitmap(total, true)` convention (:245) must be preserved.
}

template <typename T>
storage::ArtifactPtr
BitmapIndexBuilder<T>::Seal() && {
    // TODO: hand `postings_`, `valid_bitset_`, `total_num_rows_` and
    // `params_.nested` to a BitmapIndexArtifact.
    return nullptr;
}

template <typename T>
size_t
BitmapIndexBuilder<T>::DistinctCount() const {
    return postings_.size();
}

namespace {

template <typename T>
bool
RegisterBitmapBuilder() {
    BuilderRegistry<T>::Instance().Register(
        families::kBitmap, [](const BuildParams& params) {
            return std::make_unique<BitmapIndexBuilder<T>>(BitmapBuildParams{});
        });
    return true;
}

const bool kRegistered = RegisterBitmapBuilder<bool>() &&
                         RegisterBitmapBuilder<int8_t>() &&
                         RegisterBitmapBuilder<int16_t>() &&
                         RegisterBitmapBuilder<int32_t>() &&
                         RegisterBitmapBuilder<int64_t>() &&
                         RegisterBitmapBuilder<float>() &&
                         RegisterBitmapBuilder<double>() &&
                         RegisterBitmapBuilder<std::string_view>();

}  // namespace

#define INSTANTIATE_BITMAP_BUILDER(T) template class BitmapIndexBuilder<T>;
INSTANTIATE_BITMAP_BUILDER(bool)
INSTANTIATE_BITMAP_BUILDER(int8_t)
INSTANTIATE_BITMAP_BUILDER(int16_t)
INSTANTIATE_BITMAP_BUILDER(int32_t)
INSTANTIATE_BITMAP_BUILDER(int64_t)
INSTANTIATE_BITMAP_BUILDER(float)
INSTANTIATE_BITMAP_BUILDER(double)
INSTANTIATE_BITMAP_BUILDER(std::string_view)
#undef INSTANTIATE_BITMAP_BUILDER

}  // namespace milvus::index
