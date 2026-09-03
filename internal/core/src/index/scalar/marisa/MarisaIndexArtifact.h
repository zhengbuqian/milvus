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
#include <vector>

#include <marisa.h>

#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"

// The ARTIFACT of the marisa family (§6). Memory-shaped, with one wrinkle: the
// trie itself can only be written through marisa's own file API, so
// `Serialize` writes it to a temporary file and streams that into the sink.

namespace milvus::index {

class MarisaIndexArtifact final : public storage::Artifact {
 public:
    MarisaIndexArtifact(marisa::Trie trie,
                        std::vector<int64_t> str_ids,
                        std::vector<uint32_t> csr_index,
                        std::vector<uint32_t> csr_offsets);

    ~MarisaIndexArtifact() override;

    std::shared_ptr<storage::LoadedArtifact>
    OpenReader() const override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    marisa::Trie trie_;
    std::vector<int64_t> str_ids_;
    std::vector<uint32_t> csr_index_;
    std::vector<uint32_t> csr_offsets_;
};

}  // namespace milvus::index
