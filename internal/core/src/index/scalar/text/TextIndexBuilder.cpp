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

#include "index/scalar/text/TextIndexBuilder.h"

#include <utility>

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/text/TextIndexArtifact.h"

namespace milvus::index {

TextIndexBuilder::TextIndexBuilder(TextIndexBuildParams params)
    : params_(std::move(params)) {
    // TODO: move existing logic here — writer-mode wrapper construction plus
    // `RegisterAnalyzer`, from TextMatchIndex.cpp:79-104 (build service),
    // TextMatchIndex.cpp:59-77 (sealed in-place, on-disk) and
    // TextMatchIndex.cpp:36-57 (RAM directory). The three collapse into one
    // body whose only branch is `params_.local_dir.empty()`.
}

TextIndexBuilder::~TextIndexBuilder() = default;

BuilderInputSpec
TextIndexBuilder::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Streaming,
                            .needs_second_pass = false};
}

void
TextIndexBuilder::Add(size_t n,
                      const std::string_view* values,
                      const bool* valid) {
    // TODO: move existing logic here (see TextMatchIndex.cpp:256-265
    // AddTextSealed, :268-274 AddNullSealed, :298-338 BuildIndexFromFieldData).
    //
    // Gone with the split: `BuildIndexFromFieldData` took an explicit
    // `nullable` flag "because schema_ may be uninitialized"
    // (TextMatchIndex.cpp:297) — an artifact of one class serving four
    // lifecycles. Here validity is per-value and always present.
    //
    // Also gone: the auto-commit / `shouldTriggerCommit` logic
    // (TextMatchIndex.cpp:345-351) that `AddTextsGrowing` needed. A builder
    // commits exactly once, in Seal().
}

storage::ArtifactPtr
TextIndexBuilder::Seal() && {
    // TODO: move existing logic here (see TextMatchIndex.cpp:340-343 Finish ->
    // InvertedIndexTantivy.cpp:131-135 finish(), i.e. commit + merge-all), then
    // hand `engine_`, `null_offsets_` and the local directory to a
    // TextIndexArtifact.
    //
    // What must NOT come along: `Upload` (TextMatchIndex.cpp:116-163) and
    // `UploadUnified` (:165-179). Upload orchestration is the indexbuilder
    // service's (§6.2); the artifact only knows how to write itself into a
    // `storage::FileSink`. The `text_log/` vs `index_files/` prefix choice that
    // `is_index_file_ = false` (TextMatchIndex.cpp:89,112) encoded is a
    // PROPERTY OF THE SINK the service constructs, not of the index.
    return nullptr;
}

namespace {

// Self-registration (§11.2 rule 4). See index/Registry.cpp for the
// static-registration hazard note.
const bool kTextBuilderRegistered = [] {
    BuilderRegistry<std::string_view>::Instance().Register(
        families::kText, [](const BuildParams& params) {
            // TODO: parse the analyzer/tokenizer keys out of `params` — see
            // IndexFactory.cpp:372 for today's parameter extraction.
            return std::make_unique<TextIndexBuilder>(TextIndexBuildParams{});
        });
    return true;
}();

}  // namespace

}  // namespace milvus::index
