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
#include <memory>
#include <string>
#include <vector>

#include "common/Types.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"

namespace milvus::index {

// Shared by an artifact and any mmap reader opened from it. The path is always
// a unique child created by this family; configured paths are only parent
// directories and are never removed.
class InvertedIndexDirectory final {
 public:
    static std::shared_ptr<InvertedIndexDirectory>
    Create(const std::string& parent);

    ~InvertedIndexDirectory();

    InvertedIndexDirectory(const InvertedIndexDirectory&) = delete;
    InvertedIndexDirectory&
    operator=(const InvertedIndexDirectory&) = delete;

    const std::string&
    Path() const;

    size_t
    HeapBytes() const;

    size_t
    PathHeapBytes() const;

 private:
    explicit InvertedIndexDirectory(std::string path);

    std::string path_;
};

class InvertedIndexArtifact final : public storage::Artifact {
 public:
    InvertedIndexArtifact(std::shared_ptr<InvertedIndexDirectory> directory,
                          std::vector<size_t> null_offsets,
                          DataType value_type,
                          bool nested);

    InvertedIndexArtifact(
        std::shared_ptr<InvertedIndexDirectory> directory,
        std::shared_ptr<const std::vector<size_t>> null_offsets,
        DataType value_type,
        bool nested);

    ~InvertedIndexArtifact() override;

    std::shared_ptr<storage::LoadedArtifact>
    OpenReader() const override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    std::shared_ptr<InvertedIndexDirectory> directory_;
    std::shared_ptr<const std::vector<size_t>> null_offsets_;
    DataType value_type_{DataType::NONE};
    bool nested_{false};
};

}  // namespace milvus::index
