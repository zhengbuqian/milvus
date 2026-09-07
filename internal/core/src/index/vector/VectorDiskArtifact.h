// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
#include <variant>
#include <vector>

#include "common/Types.h"
#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorDiskArtifactFile.h"
#include "index/vector/VectorValidData.h"
#include "nlohmann/json.hpp"
#include "storage/artifact/Artifact.h"

namespace milvus::index {

class VectorDiskBuildFileManager;
class VectorDiskLocalFiles;

// A sealed or rehydrated disk-vector generation. Builder artifacts keep only
// immutable construction metadata/files and open lazily. Rewrite artifacts
// keep one validated immutable node plus the exact completed local files.
template <typename T>
class VectorDiskArtifact final : public storage::Artifact {
 public:
    VectorDiskArtifact(DataType elem_type,
                       IndexType index_type,
                       MetricType metric_type,
                       IndexVersion version,
                       int64_t dim,
                       VectorValidData valid,
                       std::vector<size_t> empty_emb_list_offsets,
                       Config load_params,
                       std::shared_ptr<VectorDiskLocalFiles> local_files_owner,
                       std::vector<VectorDiskArtifactFile> local_files);

    // Rehydrates one fully validated loader generation. The reader and
    // artifact copy only immutable knowhere/validity handles and retain the
    // same owned local files; no second Deserialize is performed.
    VectorDiskArtifact(const KnowhereEngine& engine,
                       const VectorValidData& valid,
                       uint32_t search_beamwidth,
                       std::shared_ptr<VectorDiskLocalFiles> local_files_owner,
                       std::shared_ptr<VectorDiskBuildFileManager> file_manager,
                       std::vector<VectorDiskArtifactFile> local_files);

    ~VectorDiskArtifact() override = default;

    // A builder artifact lazily opens a fresh node. A loader artifact copies
    // the already-open immutable node handle, so rewrite loading deserializes
    // exactly once.
    std::shared_ptr<storage::LoadedArtifact>
    OpenReader() const override;

    // The sink owns physical slicing and publication. Paths are borrowed and
    // remain owned by this artifact/its readers.
    void
    Serialize(storage::FileSink& sink) const override;

 private:
    struct BuilderState {
        BuilderState(DataType elem_type,
                     IndexType index_type,
                     MetricType metric_type,
                     IndexVersion version,
                     int64_t dim,
                     VectorValidData valid,
                     std::vector<size_t> empty_emb_list_offsets,
                     Config load_params,
                     std::shared_ptr<VectorDiskLocalFiles> local_files_owner,
                     std::vector<VectorDiskArtifactFile> local_files);

        DataType elem_type{DataType::NONE};
        IndexType index_type;
        MetricType metric_type;
        IndexVersion version{0};
        int64_t dim{0};
        std::shared_ptr<VectorDiskLocalFiles> local_files_owner;
        VectorValidData valid;
        std::shared_ptr<const std::vector<size_t>> empty_emb_list_offsets;
        Config load_params;
        std::vector<VectorDiskArtifactFile> local_files;
    };

    struct LoadedState {
        LoadedState(std::shared_ptr<VectorDiskLocalFiles> local_files_owner,
                    std::shared_ptr<VectorDiskBuildFileManager> file_manager,
                    const KnowhereEngine& engine,
                    const VectorValidData& valid,
                    uint32_t search_beamwidth,
                    std::vector<VectorDiskArtifactFile> local_files);

        // Owners precede the engine so reverse destruction releases the node
        // before its FileManager and local directory.
        std::shared_ptr<VectorDiskLocalFiles> local_files_owner;
        std::shared_ptr<VectorDiskBuildFileManager> file_manager;
        KnowhereEngine engine;
        VectorValidData valid;
        uint32_t search_beamwidth{0};
        std::vector<VectorDiskArtifactFile> local_files;
    };

    std::variant<BuilderState, LoadedState> state_;
};

}  // namespace milvus::index
