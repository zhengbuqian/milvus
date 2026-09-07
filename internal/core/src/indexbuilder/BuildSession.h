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
#include <map>
#include <memory>
#include <optional>
#include <string>

#include "index/IndexTypeAdapter.h"
#include "indexbuilder/IndexBuildService.h"
#include "storage/artifact/NamedBuffer.h"

namespace milvus::indexbuilder {

// Neutral C-ABI physical representation. Unlike storage::NamedBufferSet, a
// null data owner with a non-zero size is valid and denotes a disk artifact's
// already-published remote-file descriptor.
struct PhysicalBinaryEntry {
    std::shared_ptr<uint8_t[]> data;
    size_t size{0};
};

using PhysicalBinarySet = std::map<std::string, PhysicalBinaryEntry>;

// Native lifecycle holder for one production or direct index build. Source
// materialization and artifact publication remain in IndexBuildService; this
// class only preserves the result between the existing Build and Upload C-ABI
// phases. It intentionally has no protobuf or legacy creator dependency.
class BuildSession final {
 public:
    enum class State {
        Ready,
        Artifact,
        Loaded,
        SkippedEmpty,
        Failed,
    };

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
    // session; a successfully sealed artifact remains available for Publish.
    void
    BuildProduction();

    // Build one decoded direct batch. Scalar AUTO performs both passes over
    // the same owning FieldData; disk vectors are materialized locally once.
    void
    BuildDirect(const FieldDataPtr& batch);

    // Open a physical V1/V2 BinarySet transactionally. Scalar/memory sessions
    // retain its exact slices and SLICE_META for replay. Disk sessions ignore
    // it and production-load the normalized remote index_files; direct disk
    // sessions have no usable remote context and reject the operation.
    void
    LoadPhysical(PhysicalBinarySet buffers);

    // Scalar/memory generations preserve exact physical slices without a
    // second Disassemble. Production disk generations publish their retained
    // artifact and expose the resulting remote descriptors.
    const PhysicalBinarySet&
    SerializePhysical();

    // Publish is retryable for built and production-loaded artifacts: a sink
    // failure does not discard or poison the immutable retained generation.
    storage::ArtifactStats
    Publish() const;

    // Baseline scalar/text cleanup is nonterminal. Family-owned staging must
    // remain alive for a later Publish and is released by RAII with the
    // session.
    void
    CleanLocalData() noexcept;

    State
    GetState() const noexcept;

    bool
    IsDirect() const noexcept;

    DataType
    SourceType() const noexcept;

    // Returns the single normalized dimension used by a direct vector
    // session. Sparse dimensions may be zero and inferred from physical rows.
    int64_t
    DirectDimension() const;

 private:
    struct LoadedState;

    enum class Mode {
        Production,
        Direct,
    };

    void
    ValidateDirectInputSpec(const index::BuilderInputSpec& spec,
                            bool allow_second_pass) const;

    Mode mode_{Mode::Production};
    DataType source_type_{DataType::NONE};
    bool binary_serialize_empty_{false};
    std::unique_ptr<IndexBuildService> service_;
    std::optional<index::AdaptedIndexType> direct_spec_;
    std::optional<BuildProduct> product_;
    std::optional<PhysicalBinarySet> physical_buffers_;
    std::unique_ptr<LoadedState> loaded_state_;
    State state_{State::Ready};
};

}  // namespace milvus::indexbuilder
