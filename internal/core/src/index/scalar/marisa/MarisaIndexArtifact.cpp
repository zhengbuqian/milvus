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

#include "index/scalar/marisa/MarisaIndexArtifact.h"

#include <utility>

#include "index/Families.h"

namespace milvus::index {

MarisaIndexArtifact::MarisaIndexArtifact(marisa::Trie trie,
                                         std::vector<int64_t> str_ids,
                                         std::vector<uint32_t> csr_index,
                                         std::vector<uint32_t> csr_offsets)
    : trie_(std::move(trie)),
      str_ids_(std::move(str_ids)),
      csr_index_(std::move(csr_index)),
      csr_offsets_(std::move(csr_offsets)) {
}

MarisaIndexArtifact::~MarisaIndexArtifact() = default;

std::shared_ptr<storage::LoadedArtifact>
MarisaIndexArtifact::OpenReader() const {
    return nullptr;
}

void
MarisaIndexArtifact::Serialize(storage::FileSink& sink) const {
    // TODO: move existing logic here (see StringIndexMarisa.cpp:825-871
    // WriteEntries — the trie written through a file descriptor, then the
    // MARISA_STR_IDS / MARISA_CSR_INDEX / MARISA_CSR_OFFSETS entries and the
    // `marisa_csr_format_version` + `csr_num_keys` meta), plus the
    // families::k*MetaKey set (§4.1).
    //
    // The trie's temp file goes through `sink.WriteEntryFromLocalFile`
    // (storage/artifact/FileSink.h) instead of the hand-rolled `/tmp/<uuid>`
    // path in `Serialize` (StringIndexMarisa.cpp:246-275) — the sink is what
    // knows where local scratch lives.
    //
    // The entry key names are the persisted constants in index/Meta.h:30-33,
    // marked "will be persistent, do not edit".
}

}  // namespace milvus::index
