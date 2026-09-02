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

#include "index/scalar/ngram/NgramIndexArtifact.h"

#include <utility>

#include "index/Families.h"

namespace milvus::index {

NgramIndexArtifact::NgramIndexArtifact(
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::string local_dir,
    std::vector<size_t> null_offsets,
    uintptr_t min_gram,
    uintptr_t max_gram,
    size_t avg_row_size)
    : engine_(std::move(engine)),
      local_dir_(std::move(local_dir)),
      null_offsets_(std::move(null_offsets)),
      min_gram_(min_gram),
      max_gram_(max_gram),
      avg_row_size_(avg_row_size) {
}

NgramIndexArtifact::~NgramIndexArtifact() = default;

std::shared_ptr<storage::LoadedArtifact>
NgramIndexArtifact::OpenReader() const {
    return nullptr;
}

void
NgramIndexArtifact::Serialize(storage::FileSink& sink) const {
    // TODO: move existing logic here (see NgramInvertedIndex.cpp:218-227
    // WriteEntries — the base tantivy entries plus the `avg_row_size` entry,
    // NGRAM_AVG_ROW_SIZE_FILE_NAME at :55), plus min_gram/max_gram and the
    // families::k*MetaKey set (§4.1).
}

}  // namespace milvus::index
