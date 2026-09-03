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

#include "index/scalar/auto/AutoIndexBuilder.h"

#include <utility>

#include "index/Families.h"
#include "index/contracts/Registry.h"

namespace milvus::index {

template <typename T>
AutoIndexBuilder<T>::AutoIndexBuilder(AutoBuildParams params)
    : params_(std::move(params)) {
    // TODO: move existing logic here (see HybridScalarIndex.cpp:250-300
    // Build(Config), the config-parsing half only: the cardinality limit at
    // :257-258, the scalar index engine version at :261-263 and the
    // low/high family selection at :267-277).
    //
    // That block is COPY-PASTED into JsonHybridScalarIndex.h:97-112. Parsing it
    // once, into `AutoBuildParams`, removes the copy.
}

template <typename T>
AutoIndexBuilder<T>::~AutoIndexBuilder() = default;

template <typename T>
BuilderInputSpec
AutoIndexBuilder<T>::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Contiguous,
                            .needs_second_pass = true};
}

template <typename T>
void
AutoIndexBuilder<T>::Add(size_t n, const T* values, const bool* valid) {
    // TODO: pass 1 counts distinct values, honouring `valid` (see the header's
    // note on the null-inflation bug), and stops counting once
    // `params_.cardinality_limit` is reached; pass 2 forwards to `delegate_`.
    // See HybridScalarIndex.cpp:108-119, :121-137, :139-169.
    //
    // NOTE the array counter (:139-169) currently IGNORES the configured
    // families and hardcodes inverted/bitmap (:160-167, with a comment saying
    // so). Decide whether that stays a documented special case or becomes a
    // parameter — do not let it survive as an accident.
}

template <typename T>
std::string
AutoIndexBuilder<T>::SelectFamily(size_t distinct_count) const {
    // TODO: move existing logic here (see HybridScalarIndex.cpp:86-106),
    // returning a family NAME (index/Families.h) rather than a
    // `ScalarIndexType` enumerator.
    //
    // THE ENUM ORDINAL MUST NOT BE PERSISTED, and today it is: the chosen
    // `ScalarIndexType` is written as a raw one-byte ordinal in THREE places
    // and three encodings — a `BinarySet` entry keyed `INDEX_TYPE`
    // (HybridScalarIndex.cpp:310-312), a standalone remote file
    // (:317-334 SerializeIndexType, recovered by scanning basenames at
    // :361-375), and a V3 meta key (:432, where the literal "index_type" is
    // retyped instead of using the constant). Reordering
    // `ScalarIndexType` (ScalarIndex.h:39-50) silently rewrites the meaning of
    // every artifact ever written. A family NAME is self-describing and is what
    // the registry is keyed on anyway (index/Families.h).
    return {};
}

template <typename T>
storage::ArtifactPtr
AutoIndexBuilder<T>::Seal() && {
    // TODO: `SelectFamily(distinct_count_)`, then return
    // `std::move(*delegate_).Seal()` — the CHOSEN family's artifact, tagged
    // with that family. §6.3: "the loader opens the concrete reader directly".
    //
    // Everything else in the old class disappears with it: `GetInternalIndex`
    // (HybridScalarIndex.cpp:186-240, including the `std::string`
    // specialization) becomes `BuilderRegistry<T>::Create(family, params)`;
    // `SerializeIndexType` / `DeserializeIndexType` / `GetRemoteIndexTypeFile`
    // become one metadata key; and the whole forwarding query surface
    // (HybridScalarIndex.h:69-165) becomes nothing at all.
    return nullptr;
}

namespace {

template <typename T>
bool
RegisterAutoBuilder() {
    BuilderRegistry<T>::Instance().Register(
        families::kAuto, [](const BuildParams& params) {
            return std::make_unique<AutoIndexBuilder<T>>(AutoBuildParams{});
        });
    return true;
}

const bool kRegistered = RegisterAutoBuilder<bool>() &&
                         RegisterAutoBuilder<int8_t>() &&
                         RegisterAutoBuilder<int16_t>() &&
                         RegisterAutoBuilder<int32_t>() &&
                         RegisterAutoBuilder<int64_t>() &&
                         RegisterAutoBuilder<float>() &&
                         RegisterAutoBuilder<double>() &&
                         RegisterAutoBuilder<std::string_view>();

}  // namespace

#define INSTANTIATE_AUTO_BUILDER(T) template class AutoIndexBuilder<T>;
INSTANTIATE_AUTO_BUILDER(bool)
INSTANTIATE_AUTO_BUILDER(int8_t)
INSTANTIATE_AUTO_BUILDER(int16_t)
INSTANTIATE_AUTO_BUILDER(int32_t)
INSTANTIATE_AUTO_BUILDER(int64_t)
INSTANTIATE_AUTO_BUILDER(float)
INSTANTIATE_AUTO_BUILDER(double)
INSTANTIATE_AUTO_BUILDER(std::string_view)
#undef INSTANTIATE_AUTO_BUILDER

}  // namespace milvus::index
