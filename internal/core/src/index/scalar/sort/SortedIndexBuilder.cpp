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

#include "index/scalar/sort/SortedIndexBuilder.h"

#include <utility>

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/sort/SortedIndexArtifact.h"

namespace milvus::index {

template <typename T>
SortedIndexBuilder<T>::SortedIndexBuilder(SortedBuildParams params)
    : params_(std::move(params)) {
}

template <typename T>
SortedIndexBuilder<T>::~SortedIndexBuilder() = default;

template <typename T>
BuilderInputSpec
SortedIndexBuilder<T>::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Contiguous,
                            .needs_second_pass = false};
}

template <typename T>
void
SortedIndexBuilder<T>::Add(size_t n, const T* values, const bool* valid) {
    // TODO: move existing logic here (see ScalarIndexSort.cpp:114-147 and
    // :149-198; nested rows at :200-251, whose all-valid bitset convention is
    // explained at :222).
    //
    // KNOWN BUG IN THE CODE BEING MOVED — do not transcribe it silently.
    // ScalarIndexSort.cpp:186-189 carries `TODO: there is an existing bug here`
    // and SKIPS rows past a limit. The cause is in `IndexStructure`: its
    // constructor takes `size_t idx` but stores `int32_t idx_`
    // (IndexStructure.h:24-25,47), so an index past INT32_MAX narrows.
}

template <typename T>
storage::ArtifactPtr
SortedIndexBuilder<T>::Seal() && {
    // TODO: sort `data_`, materialize the idx->offset array, hand everything to
    // a SortedIndexArtifact.
    return nullptr;
}

SortedStringIndexBuilder::SortedStringIndexBuilder(SortedBuildParams params)
    : params_(std::move(params)) {
}

SortedStringIndexBuilder::~SortedStringIndexBuilder() = default;

BuilderInputSpec
SortedStringIndexBuilder::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Contiguous,
                            .needs_second_pass = false};
}

void
SortedStringIndexBuilder::Add(size_t n, const std::string_view* values,
                              const bool* valid) {
    // TODO: move existing logic here (see StringIndexSort.cpp:796-814
    // BuildFromRawData, :816-841 BuildFromFieldData, :843-870
    // BuildFromArrayDataNested, all funnelling into :769-794 BuildFromMap).
    //
    // Note `StringIndexSort::BuildWithArrayDataNested` (StringIndexSort.h:80-81)
    // is DECLARED AND NEVER DEFINED — a dead public declaration. The real
    // nested path goes through the impl. It does not migrate.
}

storage::ArtifactPtr
SortedStringIndexBuilder::Seal() && {
    // TODO: hand the unique values, posting lists and validity to a
    // SortedStringIndexArtifact.
    return nullptr;
}

namespace {

template <typename T>
bool
RegisterSortedBuilder() {
    BuilderRegistry<T>::Instance().Register(
        families::kSort, [](const BuildParams& params) {
            return std::make_unique<SortedIndexBuilder<T>>(SortedBuildParams{});
        });
    return true;
}

const bool kRegistered = RegisterSortedBuilder<bool>() &&
                         RegisterSortedBuilder<int8_t>() &&
                         RegisterSortedBuilder<int16_t>() &&
                         RegisterSortedBuilder<int32_t>() &&
                         RegisterSortedBuilder<int64_t>() &&
                         RegisterSortedBuilder<float>() &&
                         RegisterSortedBuilder<double>();

const bool kStringRegistered = [] {
    BuilderRegistry<std::string_view>::Instance().Register(
        families::kSort, [](const BuildParams& params) {
            return std::make_unique<SortedStringIndexBuilder>(
                SortedBuildParams{});
        });
    return true;
}();

}  // namespace

#define INSTANTIATE_SORTED_BUILDER(T) template class SortedIndexBuilder<T>;
INSTANTIATE_SORTED_BUILDER(bool)
INSTANTIATE_SORTED_BUILDER(int8_t)
INSTANTIATE_SORTED_BUILDER(int16_t)
INSTANTIATE_SORTED_BUILDER(int32_t)
INSTANTIATE_SORTED_BUILDER(int64_t)
INSTANTIATE_SORTED_BUILDER(float)
INSTANTIATE_SORTED_BUILDER(double)
#undef INSTANTIATE_SORTED_BUILDER

}  // namespace milvus::index
