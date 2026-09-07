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
#include <variant>
#include <vector>

#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"

// The ARTIFACT of the JSON flat family (§6). File-shaped (tantivy).

namespace milvus::index {

class JsonFlatIndexReaderState;

// Shared by a built artifact and every reader that still depends on its local
// Tantivy files.  The configured directory is only a parent; this owner always
// represents one family-created child and never removes the parent itself.
class JsonFlatIndexDirectory final {
 public:
    static std::shared_ptr<JsonFlatIndexDirectory>
    Create(const std::string& parent);

    ~JsonFlatIndexDirectory();

    JsonFlatIndexDirectory(const JsonFlatIndexDirectory&) = delete;
    JsonFlatIndexDirectory&
    operator=(const JsonFlatIndexDirectory&) = delete;

    const std::string&
    Path() const;

    size_t
    HeapBytes() const;

    size_t
    PathHeapBytes() const;

 private:
    explicit JsonFlatIndexDirectory(std::string path);

    std::string path_;
};

class JsonFlatIndexArtifact final : public storage::Artifact {
 public:
    JsonFlatIndexArtifact(std::shared_ptr<JsonFlatIndexDirectory> directory,
                          std::vector<size_t> null_offsets,
                          std::string nested_path);

    JsonFlatIndexArtifact(
        std::shared_ptr<JsonFlatIndexDirectory> directory,
        std::vector<std::string> engine_files,
        std::shared_ptr<const JsonFlatIndexReaderState> state);

    ~JsonFlatIndexArtifact() override;

    std::shared_ptr<storage::LoadedArtifact>
    OpenReader() const override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    struct BuilderArtifactState {
        std::shared_ptr<JsonFlatIndexDirectory> directory;
        std::shared_ptr<const std::vector<size_t>> null_offsets;
        std::string nested_path;
    };

    struct LoadedArtifactState {
        // State releases its engine before the final publication directory.
        std::shared_ptr<JsonFlatIndexDirectory> directory;
        std::vector<std::string> engine_files;
        std::shared_ptr<const JsonFlatIndexReaderState> state;
    };

    std::variant<BuilderArtifactState, LoadedArtifactState> state_;
};

}  // namespace milvus::index
