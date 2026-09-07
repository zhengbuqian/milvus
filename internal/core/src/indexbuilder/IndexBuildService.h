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

#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "common/Types.h"
#include "index/contracts/Registry.h"
#include "indexbuilder/BuildDriver.h"
#include "indexbuilder/IndexBuildFieldAdapter.h"
#include "storage/FileManager.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/ArtifactStats.h"
#include "storage/artifact/FileSink.h"

namespace milvus::indexbuilder {

struct V1BinlogBuildSource {
    std::vector<std::string> files;
};

struct StorageV2BuildSource {
    std::vector<std::vector<std::string>> files;
};

struct ManifestBuildSource {
    std::string manifest_path;
};

using BuildSource = std::
    variant<V1BinlogBuildSource, StorageV2BuildSource, ManifestBuildSource>;

struct BuildOutputSpec {
    storage::Generation generation{storage::Generation::V1V2};
    storage::ArtifactStoragePath storage_path{
        storage::ArtifactStoragePath::Index};

    // Required for V3 and unused for V1/V2. The sink owns remote naming.
    std::string packed_file_name;
};

// Native request assembled by the C-ABI adapter in a later migration step.
// The source shape and output transport are explicit; neither is inferred from
// a family or engine version here.
struct BuildRequest {
    index::IndexFamily family;
    index::BuildParams params;
    DataType value_type{DataType::NONE};
    FieldId field_id;
    BuildSource source{V1BinlogBuildSource{}};

    // Authoritative logical row count, including missing leading rows.
    int64_t expected_rows{0};
    int64_t missing_rows{0};

    // Borrowed parent for family-owned build staging. The service injects it
    // into normalized builder parameters but never removes it.
    std::string staging_parent;
    BuildOutputSpec output;

    // Authoritative JSON root for this request. Empty denotes the document
    // root; any path aliases already present in params must match it.
    std::string json_path;
};

class BuildProduct {
 public:
    enum class Kind {
        Artifact,
        SkippedEmpty,
    };

    static BuildProduct
    FromArtifact(storage::ArtifactPtr artifact);

    static BuildProduct
    SkippedEmpty();

    BuildProduct(BuildProduct&&) noexcept = default;
    BuildProduct&
    operator=(BuildProduct&&) noexcept = default;

    BuildProduct(const BuildProduct&) = delete;
    BuildProduct&
    operator=(const BuildProduct&) = delete;

    Kind
    GetKind() const {
        return kind_;
    }

    bool
    IsSkippedEmpty() const {
        return kind_ == Kind::SkippedEmpty;
    }

    const storage::Artifact&
    GetArtifact() const;

 private:
    BuildProduct(Kind kind, storage::ArtifactPtr artifact)
        : kind_(kind), artifact_(std::move(artifact)) {
    }

    Kind kind_;
    storage::ArtifactPtr artifact_;
};

// L5 orchestration: materialize one declared source, feed the typed driver,
// seal an immutable artifact, and publish it through an explicit storage sink.
class IndexBuildService {
 public:
    IndexBuildService(BuildRequest request,
                      const storage::FileManagerContext& file_manager_context);

    BuildProduct
    RunToArtifact();

    storage::ArtifactStats
    Publish(const BuildProduct& product) const;

    storage::ArtifactStats
    Run();

    const BuildRequest&
    Request() const noexcept;

    const storage::FileManagerContext&
    Context() const noexcept;

 private:
    enum class FeedOutcome { Exhausted, Stopped };

    void
    NormalizeAndValidateRequest();

    FeedOutcome
    Feed(BuildDriver& driver, int64_t& rows_fed) const;

    FeedControl
    FeedBatch(BuildDriver& driver,
              const FieldDataPtr& batch,
              int64_t& rows_fed) const;

    FeedOutcome
    FeedMissingRows(BuildDriver& driver, int64_t& rows_fed) const;

    void
    ValidateInputSpec(const index::BuilderInputSpec& spec,
                      bool allow_second_pass) const;

    std::unique_ptr<storage::FileSink>
    MakeSink() const;

    BuildRequest req_;
    storage::FileManagerContext file_manager_context_;
    BuildFieldSpec field_spec_;
    bool run_started_{false};
};

}  // namespace milvus::indexbuilder
