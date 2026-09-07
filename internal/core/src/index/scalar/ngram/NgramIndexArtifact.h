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
#include <vector>

#include "common/Types.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"

// The ARTIFACT of the ngram family (§6). Like the other tantivy families it is
// file-shaped; the one extra thing it persists is the average row size that the
// reader's cost policy needs.

namespace milvus::index {

// A family-created child directory. Configured paths are parents and are never
// removed by the index.
class NgramIndexDirectory final {
 public:
    static std::shared_ptr<NgramIndexDirectory>
    Create(const std::string& parent);

    ~NgramIndexDirectory();

    NgramIndexDirectory(const NgramIndexDirectory&) = delete;
    NgramIndexDirectory&
    operator=(const NgramIndexDirectory&) = delete;

    const std::string&
    Path() const;

    size_t
    ByteSize() const;

    size_t
    HeapBytes() const;

 private:
    explicit NgramIndexDirectory(std::string path);

    std::string path_;
    bool created_{false};
};

class NgramIndexArtifact final : public storage::Artifact {
 public:
    NgramIndexArtifact(std::shared_ptr<NgramIndexDirectory> directory,
                       std::vector<size_t> null_offsets,
                       DataType value_type,
                       uintptr_t min_gram,
                       uintptr_t max_gram,
                       size_t avg_row_size);

    NgramIndexArtifact(std::shared_ptr<NgramIndexDirectory> directory,
                       std::shared_ptr<const std::vector<size_t>> null_offsets,
                       DataType value_type,
                       uintptr_t min_gram,
                       uintptr_t max_gram,
                       size_t avg_row_size);

    ~NgramIndexArtifact() override;

    std::unique_ptr<storage::LoadedArtifact>
    OpenReader() const override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    std::shared_ptr<NgramIndexDirectory> directory_;
    std::shared_ptr<const std::vector<size_t>> null_offsets_;
    DataType value_type_{DataType::VARCHAR};
    uintptr_t min_gram_{0};
    uintptr_t max_gram_{0};
    size_t avg_row_size_{0};
};

}  // namespace milvus::index
