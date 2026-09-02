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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"
#include "tantivy-wrapper.h"

// The ARTIFACT of the text family: what `TextIndexBuilder::Seal()` produces.
//
// See 01-scalar-index.md §6 (why serialization lives on the artifact and not on
// a symmetric codec) and §11.2 rule 1 (the artifact pipeline sinks to L1, which
// is why the base class is `storage::Artifact` and not an `IndexArtifact`).
//
// Text is a FILE-SHAPED family: the tantivy writer has already put bytes into a
// local directory by the time `Seal()` returns, so `Serialize` is not an
// encoding step — it hands the existing file set to the sink. §6 reason 2 says
// exactly this: "for file-shaped families the write direction cannot be split
// one layer further; inventing a codec there would be a fictional abstraction".

namespace milvus::index {

class TextIndexArtifact final : public storage::Artifact {
 public:
    TextIndexArtifact(
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::string local_dir,
        std::vector<int64_t> null_offsets,
        int64_t count);

    ~TextIndexArtifact() override;

    // In-place open, without a round trip through storage — the sealed interim
    // build path (`ChunkedSegmentSealedImpl.cpp:5374`) uses this and never
    // serializes at all. Paired with `TextIndexLoader::OpenIndex`, which is the
    // same reader reached from bytes (§6.2: "the method is named Open, not
    // Deserialize, and pairs with Artifact::OpenReader").
    std::shared_ptr<storage::LoadedArtifact>
    OpenReader() const override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::string local_dir_;
    std::vector<int64_t> null_offsets_;
    int64_t count_{0};
};

}  // namespace milvus::index
