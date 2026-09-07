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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/JsonCastType.h"
#include "index/contracts/IndexBuilder.h"
#include "storage/artifact/Artifact.h"

// The BUILDER of the JSON flat family. §6.1, §6.1.1 (form **A, truly
// streaming** — tantivy), §8.
//
// `T = std::string_view`: the input is raw JSON documents. This family copies
// each input into padded owned storage before parsing and, when rooted below
// the document, indexes that JSON subtree. It does not use the typed
// JsonValueProjection path: that belongs in front of ordinary per-path
// inverted/bitmap/sorted builders (§5.7).

namespace milvus::tantivy {
struct TantivyIndexWrapper;
}

namespace milvus::index {

class JsonFlatIndexDirectory;

struct JsonFlatBuildParams {
    // Tantivy field name. Registry construction derives this from FIELD_ID.
    std::string field_name;
    // Empty means "the whole document"; otherwise the JSON-pointer sub-path
    // this index is rooted at. Was `JsonFlatIndex::nested_path_`
    // (JsonFlatIndex.h:787).
    std::string nested_path;
    uint32_t tantivy_index_version{0};
    std::string local_dir;
};

class JsonFlatIndexBuilder final : public IndexBuilder<std::string_view> {
 public:
    explicit JsonFlatIndexBuilder(JsonFlatBuildParams params);

    ~JsonFlatIndexBuilder() override;

    BuilderInputSpec
    InputSpec() const override;

    // Values are the raw JSON documents of the field.
    //
    // This replaces `JsonFlatIndex::build_index_for_json`
    // (`JsonFlatIndex.cpp:28-83`) — which today is a VIRTUAL HOOK on
    // `InvertedIndexTantivy` (`InvertedIndexTantivy.h:340-345`, default body
    // `ThrowInfo(NotImplemented)`), dispatched from the generic
    // `BuildWithFieldData` at `InvertedIndexTantivy.cpp:725`. That hook exists
    // only because inheritance was the reuse mechanism: the base had to leave a
    // slot for a subclass that indexes documents instead of values. With
    // composition there is no base, no slot, and no NotImplemented default.
    void
    Add(size_t n, const std::string_view* values, const bool* valid) override;

    storage::ArtifactPtr
        Seal() &&
        override;

 private:
    JsonFlatBuildParams params_;
    // Declaration order is intentional: the writer is destroyed before the
    // directory it writes to.
    std::shared_ptr<JsonFlatIndexDirectory> directory_;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::vector<std::string> path_tokens_;
    std::vector<size_t> null_offsets_;
    size_t count_{0};
    bool sealed_{false};
    bool failed_{false};
};

}  // namespace milvus::index
