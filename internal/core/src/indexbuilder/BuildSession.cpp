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
#include "index/scalar/auto/AutoIndexArtifact.h"
#include "index/scalar/json/JsonProjectedIndexLoad.h"
#include "index/scalar/spatial/RTreeIndexArtifact.h"
#include "indexbuilder/VectorBuildDriver.h"
#include "indexbuilder/VectorDiskBuildMaterializer.h"
#include "storage/artifact/FileSink.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus::indexbuilder {
namespace {

storage::NamedBufferSet
ToPayloadBuffers(const PhysicalBinarySet& physical) {
    storage::NamedBufferSet buffers;
    for (const auto& [name, entry] : physical) {
        AssertInfo(entry.size == 0 || entry.data != nullptr,
                   "physical payload entry {} is a remote descriptor",
                   name);
        const auto inserted =
            buffers.emplace(name, storage::NamedBuffer{entry.data, entry.size})
                .second;
        AssertInfo(inserted, "duplicate physical payload entry {}", name);
    }
    return buffers;
}

PhysicalBinarySet
FromPayloadBuffers(storage::NamedBufferSet buffers) {
    PhysicalBinarySet physical;
    for (auto& [name, entry] : buffers) {
        const auto inserted =
            physical
                .emplace(name,
                         PhysicalBinaryEntry{std::move(entry.data), entry.size})
                .second;
        AssertInfo(inserted, "duplicate physical payload entry");
    }
    return physical;
}

PhysicalBinarySet
FromPublishedFiles(const storage::ArtifactStats& stats) {
    PhysicalBinarySet physical;
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
                         PhysicalBinaryEntry{
                             nullptr, static_cast<size_t>(file.file_size)})
                .second;
        AssertInfo(inserted,
                   "published artifact contains duplicate file {}",
                   file.file_name);
    }
    return physical;
}

}  // namespace

struct BuildSession::LoadedState {
    LoadedState(PhysicalBinarySet physical_buffers,
                std::optional<BuildProduct> publication_product,
                std::shared_ptr<index::IndexReaderBase> opened_reader) noexcept
        : buffers(std::move(physical_buffers)),
          publishable(std::move(publication_product)),
          reader(std::move(opened_reader)) {
    }

    // Destruction is reader, publication artifact, physical input generation.
    PhysicalBinarySet buffers;
    std::optional<BuildProduct> publishable;
    std::shared_ptr<index::IndexReaderBase> reader;
};

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
BuildSession::ValidateDirectInputSpec(const index::BuilderInputSpec& spec,
                                      bool allow_second_pass) const {
    AssertInfo(spec.side_inputs.empty(),
               "direct index build cannot supply side inputs");
    if (spec.form == index::BuilderInputSpec::LocalFile) {
        AssertInfo(direct_spec_.has_value() && IsVectorDataType(source_type_) &&
                       direct_spec_->family == index::families::kVectorDisk,
                   "only a direct disk vector build can request a local-file "
                   "source");
    }
    AssertInfo(allow_second_pass || !spec.needs_second_pass,
               "selected direct builder requested another probe pass");
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

        auto driver = MakeBuildDriver(direct_spec_->value_type,
                                      direct_spec_->family,
                                      direct_spec_->params);
        AssertInfo(driver != nullptr,
                   "direct index-build driver factory returned null");
        const auto probe_spec = driver->InputSpec();
        ValidateDirectInputSpec(probe_spec, true);
        if (probe_spec.form == index::BuilderInputSpec::LocalFile) {
            AssertInfo(vector_input && source_type_ != DataType::VECTOR_ARRAY,
                       "direct disk materialization requires an ordinary "
                       "vector field");
            AssertInfo(!probe_spec.needs_second_pass,
                       "direct disk vector builder requested two passes");
            const auto local_dir = index::GetValueFromConfig<std::string>(
                direct_spec_->params, "local_dir");
            AssertInfo(local_dir.has_value() && !local_dir->empty(),
                       "direct disk vector build has no staging parent");
            AssertInfo(
                batch->Length() <=
                    static_cast<size_t>(std::numeric_limits<int64_t>::max()),
                "direct disk vector logical row count exceeds int64");
            VectorDiskBuildMaterializer materializer(
                *local_dir,
                source_type_,
                direct_spec_->value_type,
                DirectDimension(),
                batch->IsNullable(),
                static_cast<int64_t>(batch->Length()),
                false);
            materializer.Add(batch);
            materializer.FinishPrimary();
            DeliverVectorDiskInputs(*driver,
                                    std::move(materializer).TakeInputs());
        } else {
            const auto probe_result = driver->Feed(batch);
            if (probe_spec.needs_second_pass) {
                driver->FinishPass();
                ValidateDirectInputSpec(driver->InputSpec(), false);
                AssertInfo(driver->Feed(batch) == FeedControl::Continue,
                           "selected direct builder stopped its build pass "
                           "early");
            } else {
                AssertInfo(probe_result == FeedControl::Continue,
                           "one-pass direct builder requested an early stop");
            }
        }
        product_.emplace(BuildProduct::FromArtifact(std::move(*driver).Seal()));
        state_ = State::Artifact;
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
BuildSession::LoadPhysical(PhysicalBinarySet buffers) {
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

    std::optional<BuildProduct> publishable;
    std::shared_ptr<index::IndexReaderBase> reader;
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
        auto loader =
            index::LoaderRegistry::Instance().Lookup(requested_family);
        AssertInfo(loader != nullptr,
                   "no index loader is registered for family {}",
                   requested_family);
        storage::V1RemoteSource source(*file_manager_context,
                                       *index_files,
                                       options,
                                       storage::ArtifactStoragePath::Index,
                                       storage::V1SourceLayout::DiskFiles);
        auto rehydrated = loader->OpenForRewrite(source, options);
        AssertInfo(rehydrated.artifact != nullptr,
                   "disk vector loader returned a null rewrite artifact");
        AssertInfo(rehydrated.reader != nullptr,
                   "disk vector loader returned a null rewrite reader");
        publishable.emplace(
            BuildProduct::FromArtifact(std::move(rehydrated.artifact)));
        reader = std::move(rehydrated.reader);
        // The incoming BinarySet is a legacy placeholder for this family and
        // is intentionally not retained or replayed.
        buffers.clear();
    } else {
        auto payloads = ToPayloadBuffers(buffers);
        storage::NamedBufferSource source(payloads);
        const auto resolved =
            index::ResolveLoadFamily(requested_family, source);
        auto loader = index::LoaderRegistry::Instance().Lookup(resolved.family);
        AssertInfo(loader != nullptr,
                   "no index loader is registered for family {}",
                   resolved.family);
        if (requested_family != index::families::kJsonFlat) {
            options.params = index::AnnotateJsonProjectionCompleteness(
                std::move(options.params), source);
        }
        if (mode_ == Mode::Direct) {
            reader = loader->OpenIndex(source, options);
        } else {
            auto rehydrated = loader->OpenForRewrite(source, options);
            AssertInfo(rehydrated.artifact != nullptr,
                       "index loader for family {} returned a null rewrite "
                       "artifact",
                       resolved.family);
            AssertInfo(rehydrated.reader != nullptr,
                       "index loader for family {} returned a null rewrite "
                       "reader",
                       resolved.family);
            if (resolved.auto_selector.has_value()) {
                rehydrated.artifact =
                    std::make_unique<index::AutoIndexArtifact>(
                        std::move(rehydrated.artifact),
                        *resolved.auto_selector);
            }
            publishable.emplace(
                BuildProduct::FromArtifact(std::move(rehydrated.artifact)));
            reader = std::move(rehydrated.reader);
        }
    }
    AssertInfo(reader != nullptr,
               "index loader for family {} returned a null reader",
               requested_family);

    auto loaded = std::make_shared<LoadedState>(
        std::move(buffers), std::move(publishable), std::move(reader));
    // Commit only after every fallible operation above has completed. The
    // following shared/optional resets and pointer move cannot allocate.
    product_.reset();
    physical_buffers_.reset();
    loaded_state_ = std::move(loaded);
    state_ = State::Loaded;
}

const PhysicalBinarySet&
BuildSession::SerializePhysical() {
    AssertInfo(state_ == State::Artifact || state_ == State::Loaded ||
                   state_ == State::SkippedEmpty,
               "index-build session has no sealed result to serialize");
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
            const BuildProduct* product = nullptr;
            if (state_ == State::Loaded) {
                AssertInfo(loaded_state_ != nullptr &&
                               loaded_state_->publishable.has_value(),
                           "loaded disk vector session has no publishable "
                           "artifact");
                product = &*loaded_state_->publishable;
            } else {
                AssertInfo(product_.has_value(),
                           "disk vector session has no build product");
                product = &*product_;
            }
            auto physical = FromPublishedFiles(service_->Publish(*product));
            physical_buffers_.emplace(std::move(physical));
            return *physical_buffers_;
        }

        if (state_ == State::Loaded) {
            AssertInfo(loaded_state_ != nullptr,
                       "loaded index-build session has no loaded generation");
            return loaded_state_->buffers;
        }

        PhysicalBinarySet physical;
        if (state_ == State::SkippedEmpty || binary_serialize_empty_) {
            physical_buffers_.emplace(std::move(physical));
            return *physical_buffers_;
        }

        AssertInfo(product_.has_value(),
                   "sealed index-build session has no build product");
        storage::NamedBufferSink sink;
        const auto& artifact = product_->GetArtifact();
        if (family == index::families::kRTree) {
            const auto* rtree =
                dynamic_cast<const index::RTreeIndexArtifact*>(&artifact);
            AssertInfo(rtree != nullptr,
                       "R-Tree session holds a non-R-Tree artifact");
            rtree->SerializeLegacyBinary(sink);
        } else {
            artifact.Serialize(sink);
        }
        static_cast<void>(sink.Finish());
        physical = FromPayloadBuffers(sink.Take());
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
    AssertInfo(state_ == State::Artifact || state_ == State::Loaded ||
                   state_ == State::SkippedEmpty,
               "index-build session has no publishable result");
    if (state_ == State::Loaded) {
        AssertInfo(
            loaded_state_ != nullptr && loaded_state_->publishable.has_value(),
            "loaded production session has no publishable artifact");
        return service_->Publish(*loaded_state_->publishable);
    }
    AssertInfo(product_.has_value(),
               "publishable index-build session has no build product");
    return service_->Publish(*product_);
}

void
BuildSession::CleanLocalData() noexcept {
}

BuildSession::State
BuildSession::GetState() const noexcept {
    return state_;
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
