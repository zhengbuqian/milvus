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
#include <string_view>
#include <variant>
#include <vector>

#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"

// The ARTIFACT of the spatial family (§6, §11.2 rule 1). File-shaped: the
// engine has already written `<path>.bgi` and `<path>.meta.json` by the time
// `Seal()` returns, so `Serialize` hands those borrowed files to the sink. A
// builder-produced artifact owns its unique staging directory.

namespace milvus::index {

class RTreeIndexState;

// Owns an optional private R-Tree staging directory. FromPath preserves the
// builder's existing borrowed/owned distinction; CreateOwned always creates
// and removes one unique child below the borrowed parent.
class RTreeIndexDirectory final {
 public:
    static std::shared_ptr<RTreeIndexDirectory>
    FromPath(std::string path, bool remove_on_destroy);

    static std::shared_ptr<RTreeIndexDirectory>
    CreateOwned(const std::string& parent, std::string_view label);

    ~RTreeIndexDirectory();

    RTreeIndexDirectory(const RTreeIndexDirectory&) = delete;
    RTreeIndexDirectory&
    operator=(const RTreeIndexDirectory&) = delete;

    const std::string&
    Path() const;

 private:
    RTreeIndexDirectory(std::string path, bool remove_on_destroy);

    std::string path_;
    bool remove_on_destroy_{false};
};

class RTreeIndexArtifact final : public storage::Artifact {
 public:
    RTreeIndexArtifact(std::string local_dir,
                       std::vector<size_t> null_offsets,
                       int64_t total_num_rows,
                       bool owns_local_dir = false);

    RTreeIndexArtifact(std::shared_ptr<RTreeIndexDirectory> directory,
                       std::vector<std::string> engine_files,
                       std::shared_ptr<const RTreeIndexState> loaded_state);

    ~RTreeIndexArtifact() override;

    std::shared_ptr<storage::LoadedArtifact>
    OpenReader() const override;

    void
    Serialize(storage::FileSink& sink) const override;

    // The legacy C BinarySet contains only the native size_t NULL sidecar;
    // engine files are published through the production artifact path.
    void
    SerializeLegacyBinary(storage::FileSink& sink) const;

 private:
    struct BuilderArtifactState {
        std::shared_ptr<RTreeIndexDirectory> directory;
        std::vector<std::string> engine_files;
        std::shared_ptr<const std::vector<size_t>> null_offsets;
        int64_t total_num_rows{0};
    };

    struct LoadedArtifactState {
        // State releases its engine before the final directory owner.
        std::shared_ptr<RTreeIndexDirectory> directory;
        std::vector<std::string> engine_files;
        std::shared_ptr<const RTreeIndexState> state;
    };

    const std::shared_ptr<RTreeIndexDirectory>&
    Directory() const;

    const std::vector<std::string>&
    EngineFiles() const;

    const std::vector<size_t>&
    NullOffsets() const;

    std::variant<BuilderArtifactState, LoadedArtifactState> state_{
        BuilderArtifactState{}};
};

}  // namespace milvus::index
