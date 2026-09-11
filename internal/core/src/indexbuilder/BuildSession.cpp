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

#include "indexbuilder/BuildSession.h"

#include <limits>
#include <utility>

#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/json/JsonProjectedIndexLoad.h"
#include "indexbuilder/BuildInputMaterializer.h"
#include "storage/artifact/FileSink.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus::indexbuilder {
namespace {

NamedBufferSet
FromPublishedFiles(const storage::ArtifactStats& stats) {
    NamedBufferSet physical;
    for (const auto& file : stats.Files()) {
        AssertInfo(file.file_size >= 0,
                   "published artifact file {} has negative size",
                   file.file_name);
        const auto size = static_cast<uint64_t>(file.file_size);
        AssertInfo(
            size <= static_cast<uint64_t>(std::numeric_limits<size_t>::max()),
            "published artifact file {} exceeds size_t",
            file.file_name);
        const auto inserted =
            physical
                .emplace(file.file_name,
                         NamedBuffer{nullptr,
                                     static_cast<size_t>(file.file_size)})
                .second;
        AssertInfo(inserted,
                   "published artifact contains duplicate file {}",
                   file.file_name);
    }
    return physical;
}

}  // namespace

BuildSession::BuildSession(BuildRequest request,
                           storage::FileManagerContext file_manager_context)
    : binary_serialize_empty_(request.family == index::families::kFmIndex),
      service_(std::make_unique<IndexBuildService>(std::move(request),
                                                   file_manager_context)) {
}

BuildSession::BuildSession(DataType source_type,
                           index::AdaptedIndexType adapted)
    : mode_(Mode::Direct),
      source_type_(source_type),
      binary_serialize_empty_(adapted.family == index::families::kFmIndex),
      direct_spec_(std::move(adapted)) {
    // FMINDEX parameters are part of the create-time C API contract. Other
    // families construct their materializers only when Build runs.
    if (direct_spec_->family == index::families::kFmIndex) {
        auto validator =
            MakeScalarBuildInputMaterializer(source_type_,
                                             direct_spec_->value_type,
                                             0,
                                             direct_spec_->family,
                                             direct_spec_->params);
        AssertInfo(validator != nullptr,
                   "direct scalar index has no build materializer");
    }
}

BuildSession::~BuildSession() = default;

void
BuildSession::BuildProduction() {
    try {
        AssertInfo(state_ == State::Ready,
                   "index-build session cannot build more than once");
        AssertInfo(mode_ == Mode::Production && service_ != nullptr,
                   "direct index-build session cannot build a production "
                   "source");
        product_.emplace(service_->RunToArtifact());
        state_ =
            product_->IsSkippedEmpty() ? State::SkippedEmpty : State::Artifact;
    } catch (...) {
        product_.reset();
        state_ = State::Failed;
        throw;
    }
}

void
BuildSession::BuildDirect(const FieldDataPtr& batch) {
    try {
        AssertInfo(mode_ == Mode::Direct && direct_spec_.has_value(),
                   "production index-build session cannot build direct data");
        AssertInfo(state_ == State::Ready,
                   "direct index-build session cannot build more than once");
        AssertInfo(batch != nullptr,
                   "direct index-build received a null field-data batch");

        const bool vector_input = IsVectorDataType(source_type_);
        if (vector_input) {
            AssertInfo(batch->get_data_type() == source_type_,
                       "direct vector session for type {} received type {}",
                       source_type_,
                       batch->get_data_type());
            // This is the authoritative normalized configuration reused by a
            // later load. A failure after this point poisons the session, so
            // no rollback can expose a half-updated reusable configuration.
            direct_spec_->params["nullable"] = batch->IsNullable();
        }

        product_.emplace(BuildDirectInput(source_type_, *direct_spec_, batch));
        state_ =
            product_->IsSkippedEmpty() ? State::SkippedEmpty : State::Artifact;
    } catch (const SegcoreError& error) {
        product_.reset();
        if (error.get_error_code() == DataIsEmpty) {
            product_.emplace(BuildProduct::SkippedEmpty());
            state_ = State::SkippedEmpty;
            return;
        }
        state_ = State::Failed;
        throw;
    } catch (...) {
        product_.reset();
        state_ = State::Failed;
        throw;
    }
}

void
BuildSession::LoadPhysical(NamedBufferSet buffers) {
    AssertInfo(state_ != State::Failed,
               "failed index-build session cannot be reloaded");

    index::IndexFamily requested_family;
    const index::BuildParams* normalized_params = nullptr;
    const storage::FileManagerContext* file_manager_context = nullptr;
    std::string staging_parent;
    if (mode_ == Mode::Direct) {
        AssertInfo(direct_spec_.has_value(),
                   "direct index-build session has no adapted load spec");
        requested_family = direct_spec_->family;
        normalized_params = &direct_spec_->params;
    } else {
        AssertInfo(service_ != nullptr,
                   "production index-build session has no build service");
        const auto& request = service_->Request();
        requested_family = request.family;
        normalized_params = &request.params;
        file_manager_context = &service_->Context();
        staging_parent = request.staging_parent;
    }
    AssertInfo(normalized_params != nullptr,
               "index-build session has no normalized load parameters");

    if (requested_family == index::families::kRTree ||
        requested_family == index::families::kFmIndex) {
        ThrowInfo(Unsupported,
                  "index family {} has no BinarySet load representation",
                  requested_family);
    }
    storage::LoadOptions options;
    options.enable_mmap = false;
    options.mmap_dir_path = staging_parent;
    options.params = *normalized_params;

    std::unique_ptr<index::IndexReaderBase> reader;
    if (requested_family == index::families::kVectorDisk) {
        AssertInfo(mode_ == Mode::Production && service_ != nullptr &&
                       file_manager_context != nullptr &&
                       file_manager_context->Valid(),
                   "direct disk vector load has no remote file-manager "
                   "context");
        const auto index_files =
            index::GetValueFromConfig<std::vector<std::string>>(
                *normalized_params, index::INDEX_FILES);
        AssertInfo(index_files.has_value() && !index_files->empty(),
                   "disk vector load requires non-empty index_files");
        const auto loader =
            index::LoaderRegistry::Instance().Lookup(requested_family);
        AssertInfo(static_cast<bool>(loader),
                   "no index loader is registered for family {}",
                   requested_family);
        storage::V1RemoteSource source(*file_manager_context,
                                       *index_files,
                                       options,
                                       storage::ArtifactStoragePath::Index,
                                       storage::V1SourceLayout::DiskFiles);
        reader = loader.open(source, options);
    } else {
        storage::NamedBufferSource source(buffers);
        const auto resolved_family =
            index::ResolveLoadFamily(requested_family, source);
        const auto loader =
            index::LoaderRegistry::Instance().Lookup(resolved_family);
        AssertInfo(static_cast<bool>(loader),
                   "no index loader is registered for family {}",
                   resolved_family);
        if (requested_family != index::families::kJsonFlat) {
            options.params = index::AnnotateJsonProjectionCompleteness(
                std::move(options.params), source);
        }
        reader = loader.open(source, options);
    }
    AssertInfo(reader != nullptr,
               "index loader for family {} returned a null reader",
               requested_family);

    // Commit only after every fallible operation above has completed. The
    // following pointer/optional resets and pointer move cannot fail.
    product_.reset();
    physical_buffers_.reset();
    loaded_reader_ = std::move(reader);
    state_ = State::Loaded;
}

const NamedBufferSet&
BuildSession::SerializePhysical() {
    AssertInfo(state_ == State::Artifact || state_ == State::SkippedEmpty,
               "index-build session has no built result to serialize");
    if (physical_buffers_.has_value()) {
        return *physical_buffers_;
    }

    try {
        index::IndexFamily family;
        if (mode_ == Mode::Direct) {
            AssertInfo(direct_spec_.has_value(),
                       "direct index-build session has no adapted spec");
            family = direct_spec_->family;
        } else {
            AssertInfo(service_ != nullptr,
                       "production index-build session has no build service");
            family = service_->Request().family;
        }

        if (family == index::families::kVectorDisk) {
            AssertInfo(mode_ == Mode::Production && service_ != nullptr,
                       "direct disk vector serialization has no remote "
                       "publication context");
            AssertInfo(product_.has_value(),
                       "disk vector session has no build product");
            auto physical = FromPublishedFiles(service_->Publish(*product_));
            physical_buffers_.emplace(std::move(physical));
            return *physical_buffers_;
        }

        NamedBufferSet physical;
        if (state_ == State::SkippedEmpty || binary_serialize_empty_) {
            physical_buffers_.emplace(std::move(physical));
            return *physical_buffers_;
        }

        AssertInfo(product_.has_value(),
                   "built index-build session has no build product");
        storage::NamedBufferSink sink;
        const auto& artifact = product_->GetArtifact();
        artifact.Serialize(
            sink, storage::ArtifactSerializationMode::LegacyBinarySet);
        static_cast<void>(sink.Finish());
        physical = sink.Take();
        physical_buffers_.emplace(std::move(physical));
        return *physical_buffers_;
    } catch (...) {
        physical_buffers_.reset();
        throw;
    }
}

storage::ArtifactStats
BuildSession::Publish() const {
    AssertInfo(mode_ == Mode::Production && service_ != nullptr,
               "direct index-build session has no remote publish context");
    AssertInfo(state_ == State::Artifact || state_ == State::SkippedEmpty,
               "index-build session has no publishable result");
    AssertInfo(product_.has_value(),
               "publishable index-build session has no build product");
    return service_->Publish(*product_);
}

bool
BuildSession::IsDirect() const noexcept {
    return mode_ == Mode::Direct;
}

DataType
BuildSession::SourceType() const noexcept {
    return source_type_;
}

int64_t
BuildSession::DirectDimension() const {
    AssertInfo(mode_ == Mode::Direct && direct_spec_.has_value(),
               "production index-build session has no direct dimension");
    AssertInfo(IsVectorDataType(source_type_),
               "direct scalar session has no vector dimension");
    const auto dim =
        index::GetValueFromConfig<int64_t>(direct_spec_->params, DIM_KEY);
    if (!dim.has_value()) {
        AssertInfo(source_type_ == DataType::VECTOR_SPARSE_U32_F32,
                   "direct vector session has no dimension");
        return 0;
    }
    AssertInfo(*dim >= 0 && (*dim > 0 ||
                             source_type_ == DataType::VECTOR_SPARSE_U32_F32),
               "direct vector session has invalid dimension {}",
               *dim);
    return *dim;
}

}  // namespace milvus::indexbuilder
