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

// The ARTIFACT of the ngram family (§6). Like the other tantivy families it is
// file-shaped; the one extra thing it persists is the average row size that the
// reader's cost policy needs.

namespace milvus::index {

class NgramIndexArtifact final : public storage::Artifact {
 public:
    NgramIndexArtifact(
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::string local_dir,
        std::vector<size_t> null_offsets,
        uintptr_t min_gram,
        uintptr_t max_gram,
        size_t avg_row_size);

    ~NgramIndexArtifact() override;

    std::shared_ptr<storage::LoadedArtifact>
    OpenReader() const override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::string local_dir_;
    std::vector<size_t> null_offsets_;
    uintptr_t min_gram_{0};
    uintptr_t max_gram_{0};
    size_t avg_row_size_{0};
};

}  // namespace milvus::index
