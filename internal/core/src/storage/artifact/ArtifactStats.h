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

// What an `Artifact::Serialize(FileSink&)` produced: the published files plus
// the legacy build/upload size statistic projected through the C ABI.
//
// See core_refactor/01-scalar-index.md §11.2 rule 1 (the pipeline sinks to L1)
// and §12.2 (naming still open).
//
// Projection into proto remains an adapter concern. This value type contains
// only byte-accounting data and has no index semantics.

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

    // Legacy `IndexStats::mem_size`: the serialized bytes handled by the build
    // and upload path (DiskFileManager local file bytes plus MemFileManager
    // named-buffer bytes), preserved for the existing C-ABI projection. It is
    // neither opened-reader resident memory nor a load-admission estimate.
    // Opened ownership is reported only by LoadedArtifact::CellByteSize().
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
