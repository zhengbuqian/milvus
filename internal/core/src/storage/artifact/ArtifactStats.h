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
#include <string>
#include <utility>
#include <vector>

// What a `Artifact::Serialize(FileSink&)` produced: the list of files that were
// written, plus the in-memory footprint. Zero index semantics.
//
// See core_refactor/01-scalar-index.md §11.2 rule 1 (the pipeline sinks to L1)
// and §12.2 (naming still open).
//
// Modelled on today's `index/IndexStats.h`, with three deliberate differences:
//
//  1. `index::IndexStats::SerializeAt(milvus::ProtoLayout*)` does NOT come
//     along. Projecting the result into proto is an adapter concern
//     (README §5 rule 2 — pb only in adapters and capi); the indexbuilder
//     service does it at its own boundary.
//  2. Value type, not a move-only heap object behind `IndexStatsPtr`
//     (`std::unique_ptr`). Nothing about this data needs identity.
//  3. `SerializedIndexFileInfo` loses the `Index` infix along with the rest of
//     the prefix (§11.2: the `Index` prefix stops making sense at L1).
//
// The rest — `(file_name, file_size)` pairs plus a memory size — is copied
// verbatim, because `index/IndexStats.h` already depends on nothing but
// `common/protobuf_utils.h` and carries no index semantics whatsoever. §1 makes
// exactly this point: "the whole cost of the migration is demoting this class".

namespace milvus::storage {

struct SerializedFileInfo {
    SerializedFileInfo() = default;

    SerializedFileInfo(std::string file_name, int64_t file_size)
        : file_name(std::move(file_name)), file_size(file_size) {
    }

    std::string file_name;
    int64_t file_size{0};
};

class ArtifactStats {
 public:
    ArtifactStats() = default;

    ArtifactStats(int64_t mem_size, std::vector<SerializedFileInfo> files)
        : mem_size_(mem_size), files_(std::move(files)) {
    }

    void
    Append(SerializedFileInfo info) {
        files_.emplace_back(std::move(info));
    }

    const std::vector<SerializedFileInfo>&
    Files() const {
        return files_;
    }

    std::vector<std::string>
    FileNames() const;

    // Bytes the opened artifact occupies in memory.
    //
    // NOTE: this is the *build*-side report (what the builder measured), not the
    // load-side cache accounting. The latter is `LoadedArtifact::CellByteSize()`
    // and its yardstick is still undefined — see §12.3 and the comment there.
    // Do not silently unify the two; §12.3 says the unification has to be a
    // decision, not a rename.
    int64_t
    MemSize() const {
        return mem_size_;
    }

    // Sum of `file_size` over `files_`.
    int64_t
    SerializedSize() const;

 private:
    int64_t mem_size_{0};
    std::vector<SerializedFileInfo> files_;
};

}  // namespace milvus::storage
