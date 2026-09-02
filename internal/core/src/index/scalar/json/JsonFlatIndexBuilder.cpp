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

#include "index/scalar/json/JsonFlatIndexBuilder.h"

#include <utility>

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/json/JsonFlatIndexArtifact.h"
#include "index/scalar/json/JsonValueProjection.h"

namespace milvus::index {

JsonFlatIndexBuilder::JsonFlatIndexBuilder(JsonFlatBuildParams params)
    : params_(std::move(params)) {
    // TODO: create a writer-mode tantivy wrapper with
    // `user_specified_doc_id = false` — the one setting where this family
    // differs from the plain inverted builder (JsonFlatIndex.h:743-744).
}

JsonFlatIndexBuilder::~JsonFlatIndexBuilder() = default;

BuilderInputSpec
JsonFlatIndexBuilder::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Streaming,
                            .needs_second_pass = false};
}

void
JsonFlatIndexBuilder::Add(size_t n, const std::string_view* values,
                          const bool* valid) {
    // TODO: move existing logic here (see JsonFlatIndex.cpp:28-83):
    // null rows -> null_offsets_ plus an empty array entry (:39-43); the
    // path-exists gate (:45-49); whole-document case (:51-53); sub-path case
    // via `at_pointer` + `to_json_string` into the reused padded scratch buffer
    // (:55-78, note the NUL termination the Rust FFI requires at :74-75).
}

storage::ArtifactPtr
JsonFlatIndexBuilder::Seal() && {
    // TODO: finish/commit the tantivy writer, then hand the directory and the
    // null offsets to a JsonFlatIndexArtifact.
    return nullptr;
}

namespace {

const bool kJsonFlatBuilderRegistered = [] {
    BuilderRegistry<std::string_view>::Instance().Register(
        families::kJsonFlat, [](const BuildParams& params) {
            return std::make_unique<JsonFlatIndexBuilder>(
                JsonFlatBuildParams{});
        });
    return true;
}();

}  // namespace

}  // namespace milvus::index
