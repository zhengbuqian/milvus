// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
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
#include <optional>
#include <string>

#include "common/NamedBuffer.h"
#include "index/IndexTypeAdapter.h"
#include "indexbuilder/IndexBuildService.h"

namespace milvus::indexbuilder {

// Native lifecycle holder for one production or direct index build. Source
// materialization and artifact publication remain in IndexBuildService; this
// class preserves either a built artifact or a loaded query reader. It
// intentionally has no protobuf or legacy creator dependency.
class BuildSession final {
 public:
    BuildSession(BuildRequest request,
                 storage::FileManagerContext file_manager_context);

    BuildSession(DataType source_type, index::AdaptedIndexType adapted);

    BuildSession(const BuildSession&) = delete;
    BuildSession&
    operator=(const BuildSession&) = delete;

    BuildSession(BuildSession&&) = delete;
    BuildSession&
    operator=(BuildSession&&) = delete;

    ~BuildSession();

    // Consumes the configured source exactly once. Any exception poisons this
    // session; a successfully built artifact remains available for Publish.
    void
    BuildProduction();

    // Build one complete decoded direct input. Scalar hybrid selection and the
    // chosen family consume the same owning FieldData.
    void
    BuildDirect(const FieldDataPtr& batch);

    // Open a physical V1/V2 BinarySet transactionally into a query reader.
    // Disk sessions ignore it and production-load normalized remote files;
    // direct disk sessions have no usable remote context and reject the load.
    void
    LoadPhysical(NamedBufferSet buffers);

    // Serialize only a built artifact or an explicitly skipped-empty result.
    const NamedBufferSet&
    SerializePhysical();

    // Publish is retryable for built artifacts: a sink failure does not discard
    // or poison the immutable retained generation.
    storage::ArtifactStats
    Publish() const;

    bool
    IsDirect() const noexcept;

    DataType
    SourceType() const noexcept;

    // Returns the single normalized dimension used by a direct vector
    // session. Sparse dimensions may be zero and inferred from physical rows.
    int64_t
    DirectDimension() const;

 private:
    enum class State {
        Ready,
        Artifact,
        Loaded,
        SkippedEmpty,
        Failed,
    };

    enum class Mode {
        Production,
        Direct,
    };

    Mode mode_{Mode::Production};
    DataType source_type_{DataType::NONE};
    bool binary_serialize_empty_{false};
    std::unique_ptr<IndexBuildService> service_;
    std::optional<index::AdaptedIndexType> direct_spec_;
    std::optional<BuildProduct> product_;
    std::optional<NamedBufferSet> physical_buffers_;
    std::unique_ptr<index::IndexReaderBase> loaded_reader_;
    State state_{State::Ready};
};

}  // namespace milvus::indexbuilder
