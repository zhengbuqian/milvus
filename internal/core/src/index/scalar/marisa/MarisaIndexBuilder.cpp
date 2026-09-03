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

#include "index/scalar/marisa/MarisaIndexBuilder.h"

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/marisa/MarisaIndexArtifact.h"

namespace milvus::index {

MarisaIndexBuilder::MarisaIndexBuilder() = default;

MarisaIndexBuilder::~MarisaIndexBuilder() = default;

BuilderInputSpec
MarisaIndexBuilder::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Contiguous,
                            .needs_second_pass = false};
}

void
MarisaIndexBuilder::Add(size_t n, const std::string_view* values,
                        const bool* valid) {
    // TODO: move existing logic here (see StringIndexMarisa.cpp:217-244
    // Build(n, values, valid) and :172-215 BuildWithFieldData).
    //
    // The `ThrowInfo(IndexAlreadyBuild)` guards (:162, :222) disappear:
    // `Seal() &&` is rvalue-qualified, so building twice is a compile error
    // rather than a runtime one.
}

storage::ArtifactPtr
MarisaIndexBuilder::Seal() && {
    // TODO: move existing logic here — `trie_.build(keyset_)` plus the second
    // pass that fills the row->key-id array (StringIndexMarisa.cpp:718-732
    // fill_str_ids) and the CSR inversion (:734-773 fill_offsets, including its
    // uint32 bound assertions at :738-743).
    return nullptr;
}

namespace {

const bool kMarisaBuilderRegistered = [] {
    BuilderRegistry<std::string_view>::Instance().Register(
        families::kMarisa,
        [](const BuildParams& params) {
            return std::make_unique<MarisaIndexBuilder>();
        });
    return true;
}();

}  // namespace

}  // namespace milvus::index
