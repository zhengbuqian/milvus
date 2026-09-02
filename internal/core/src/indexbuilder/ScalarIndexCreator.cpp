// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "indexbuilder/ScalarIndexCreator.h"

#include <utility>

namespace milvus::indexbuilder {

ScalarIndexCreator::ScalarIndexCreator(
    BuildRequest request, storage::FileManagerContext& file_manager_context)
    : service_(std::move(request), file_manager_context) {
    // Nothing else. The 80 lines of parameter translation that used to sit in
    // this constructor (`indexbuilder/ScalarIndexCreator.cpp:106-190` before
    // W1: the ngram min_gram/max_gram check, `ParseFMIndexParam`'s
    // range/power-of-two validation for `sa_sample_rate` and `block_bytes`,
    // `scalar_index_engine_version`, `tantivy_index_version`, `is_text_match`,
    // `analyzer_extra_info`, and the JSON cast type/path/function fields) were
    // building an `index::CreateIndexInfo` parameter bag.
    //
    // TODO: move that translation into the `BuildRequest` adapter in
    // `index_c.cpp`. Two reasons it belongs there and not here:
    //   - it reads a `milvus::Config` that came straight off
    //     `proto::indexcgo::BuildIndexInfo`, and README §5 rule 2 confines
    //     proto-shaped data to the named adapter files and capi;
    //   - §11.2 item 4 breaks `CreateIndexInfo` up: each family's builder takes
    //     its own parameters through `index::BuilderRegistry<T>`'s
    //     `BuildParams`, so there is no single bag left to fill.
    // The VALIDATION itself must not be lost in the move — it is
    // defence-in-depth kept in lockstep with Go's create-index checker (which
    // uses `strconv.Atoi`, hence the tolerated leading '+'), so a request
    // accepted synchronously cannot fail later in the asynchronous build.
}

void
ScalarIndexCreator::Build() {
    // TODO: move existing logic here — the old body was
    // `index_->Build(config_)` wrapped in a `DataIsEmpty` catch
    // (ScalarIndexCreator.cpp:212-222 before W1). New shape:
    //   artifact_ = service_.RunToArtifact();
    // i.e. push the manifest batches through the driver and `Seal()`.
}

storage::ArtifactStats
ScalarIndexCreator::Upload() {
    // TODO: move existing logic here — the old body was the
    // `UploadUnified`-vs-`Upload` fork on `SCALAR_INDEX_ENGINE_VERSION`
    // (ScalarIndexCreator.cpp:242-254 before W1). New shape:
    //   return service_.Publish(*artifact_);
    return {};
}

}  // namespace milvus::indexbuilder
