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

#include "index/scalar/spatial/RTreeIndexBuilder.h"

#include <utility>

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/spatial/RTreeIndexArtifact.h"

namespace milvus::index {

RTreeIndexBuilder::RTreeIndexBuilder(RTreeBuildParams params)
    : params_(std::move(params)) {
    // TODO: move existing logic here (see RTreeIndex.cpp:72-91
    // InitForBuildIndex, sealed half only).
    //
    // The growing half of that method (`is_growing == true`, empty path,
    // RTreeIndex.cpp:76-77) does NOT come here. It is the Appender face, and
    // it is reached today by segcore reaching in with a `dynamic_cast` plus a
    // call to `InitForBuildIndex(true)` (segcore/FieldIndexing.cpp:766-808) —
    // a downcast that exists only because one class serves both faces.
}

RTreeIndexBuilder::~RTreeIndexBuilder() = default;

BuilderInputSpec
RTreeIndexBuilder::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Contiguous,
                            .needs_second_pass = false};
}

void
RTreeIndexBuilder::Add(size_t n, const std::string_view* values,
                       const bool* valid) {
    // TODO: move existing logic here (see RTreeIndex.cpp:294-336
    // BuildWithFieldData — the null-offset collection at :304-323 and the WKB
    // decoding half of RTreeIndexWrapper.cpp:105-169), plus
    // RTreeIndex.cpp:606-637 AddGeometry for the incremental shape.
    //
    // `use_bulk_load` (RTreeIndex.cpp:300) was a hardcoded `true` local — not a
    // choice, so it does not become a parameter.
    //
    // BUG NOT TO CARRY OVER: `BuildWithStrings` (RTreeIndex.cpp:575-604) pushes
    // to `null_offset_` at :593 WITHOUT the lock every other writer takes. In a
    // one-shot builder the lock question disappears entirely.
}

storage::ArtifactPtr
RTreeIndexBuilder::Seal() && {
    // TODO: move existing logic here (see RTreeIndex.cpp:338-345 finish() ->
    // RTreeBuildEngine::Finish), then hand the local directory and the null
    // offsets to an RTreeIndexArtifact.
    return nullptr;
}

namespace {

const bool kRTreeBuilderRegistered = [] {
    BuilderRegistry<std::string_view>::Instance().Register(
        families::kRTree, [](const BuildParams& params) {
            return std::make_unique<RTreeIndexBuilder>(RTreeBuildParams{});
        });
    return true;
}();

}  // namespace

}  // namespace milvus::index
