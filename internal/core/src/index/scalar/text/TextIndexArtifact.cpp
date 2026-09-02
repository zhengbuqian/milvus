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

#include "index/scalar/text/TextIndexArtifact.h"

#include <utility>

#include "index/Families.h"
#include "index/scalar/text/TextIndexReader.h"

namespace milvus::index {

TextIndexArtifact::TextIndexArtifact(
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::string local_dir,
    std::vector<int64_t> null_offsets,
    int64_t count)
    : engine_(std::move(engine)),
      local_dir_(std::move(local_dir)),
      null_offsets_(std::move(null_offsets)),
      count_(count) {
}

TextIndexArtifact::~TextIndexArtifact() = default;

std::shared_ptr<storage::LoadedArtifact>
TextIndexArtifact::OpenReader() const {
    // TODO: create a reader-mode wrapper over the just-built directory and wrap
    // it in a TextIndexReader (see TextMatchIndex.cpp:370-373 CreateReader for
    // the set-bitset callback installation).
    return nullptr;
}

void
TextIndexArtifact::Serialize(storage::FileSink& sink) const {
    // TODO: move existing logic here (see TextMatchIndex.cpp:116-163 Upload for
    // the V2 directory walk, and InvertedIndexTantivy.cpp:874-918 WriteEntries
    // for the V3 packed layout). ONLY the "turn my files into entries" half
    // moves; the remote-path composition and the upload itself do not (§6.2).
    //
    // Must also write, via `sink.PutMeta`, the load-time metadata that
    // `TextIndexLoader::DeriveCaps` reads WITHOUT opening the index (§4.1):
    //   families::kFamilyMetaKey     -> families::kText
    //   families::kValueTypeMetaKey  -> VARCHAR
    //   families::kCoordDomainMetaKey-> "row"
    //   families::kCountMetaKey      -> count_
    // and the null-offset entry (`InvertedIndexTantivy.h:49`
    // INDEX_NULL_OFFSET_FILE_NAME, written today by
    // InvertedIndexTantivy.cpp:137-155 Serialize).
}

}  // namespace milvus::index
