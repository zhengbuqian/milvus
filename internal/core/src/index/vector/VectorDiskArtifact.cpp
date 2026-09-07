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

#include "index/vector/VectorDiskArtifact.h"

#include <algorithm>
#include <charconv>
#include <filesystem>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <utility>

#include "common/EasyAssert.h"
#include "index/Meta.h"
#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorDiskBuildFileManager.h"
#include "index/vector/VectorDiskLocalFiles.h"
#include "index/vector/VectorDiskReader.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/segcore_error_code.h"

namespace milvus::index {
namespace {

constexpr uint32_t kDefaultBeamwidth = 8;
constexpr uint32_t kMinDiskAnnBeamwidth = 1;
constexpr uint32_t kMaxDiskAnnBeamwidth = 128;

int64_t
ParsePositiveInt(const nlohmann::json& value, std::string_view key) {
    int64_t parsed = 0;
    if (value.is_number_unsigned()) {
        const auto number = value.get<uint64_t>();
        if (number <=
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            parsed = static_cast<int64_t>(number);
        }
    } else if (value.is_number_integer()) {
        parsed = value.get<int64_t>();
    } else if (value.is_string()) {
        const auto text = value.get<std::string>();
        const auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), parsed);
        if (error != std::errc() || end != text.data() + text.size()) {
            parsed = 0;
        }
    }
    if (parsed <= 0 || parsed > std::numeric_limits<int32_t>::max()) {
        ThrowInfo(
            ConfigInvalid, "vector parameter {} must be a positive int32", key);
    }
    return parsed;
}

void
PrepareDirectLoadConfig(Config& config,
                        const IndexType& index_type,
                        const std::string& prefix) {
    config[DISK_ANN_PREFIX_PATH] = prefix;
    if (index_type != knowhere::IndexEnum::INDEX_DISKANN) {
        return;
    }
    config[DISK_ANN_PREPARE_WARM_UP] = false;
    config[DISK_ANN_PREPARE_USE_BFS_CACHE] = false;
    if (config.contains(DISK_ANN_LOAD_THREAD_NUM)) {
        config[DISK_ANN_THREADS_NUM] = ParsePositiveInt(
            config.at(DISK_ANN_LOAD_THREAD_NUM), DISK_ANN_LOAD_THREAD_NUM);
    } else if (config.contains(DISK_ANN_BUILD_THREAD_NUM)) {
        config[DISK_ANN_THREADS_NUM] = ParsePositiveInt(
            config.at(DISK_ANN_BUILD_THREAD_NUM), DISK_ANN_BUILD_THREAD_NUM);
    }
}

}  // namespace

template <typename T>
VectorDiskArtifact<T>::BuilderState::BuilderState(
    DataType builder_elem_type,
    IndexType builder_index_type,
    MetricType builder_metric_type,
    IndexVersion builder_version,
    int64_t builder_dim,
    VectorValidData valid,
    std::vector<size_t> empty_offsets,
    Config load_params,
    std::shared_ptr<VectorDiskLocalFiles> owner,
    std::vector<VectorDiskArtifactFile> files)
    : elem_type(builder_elem_type),
      index_type(std::move(builder_index_type)),
      metric_type(std::move(builder_metric_type)),
      version(builder_version),
      dim(builder_dim),
      local_files_owner(std::move(owner)),
      valid(std::move(valid)),
      empty_emb_list_offsets(empty_offsets.empty()
                                 ? nullptr
                                 : std::make_shared<const std::vector<size_t>>(
                                       std::move(empty_offsets))),
      load_params(std::move(load_params)),
      local_files(std::move(files)) {
}

template <typename T>
VectorDiskArtifact<T>::LoadedState::LoadedState(
    std::shared_ptr<VectorDiskLocalFiles> owner,
    std::shared_ptr<VectorDiskBuildFileManager> manager,
    const KnowhereEngine& loaded_engine,
    const VectorValidData& loaded_valid,
    uint32_t beamwidth,
    std::vector<VectorDiskArtifactFile> files)
    : local_files_owner(std::move(owner)),
      file_manager(std::move(manager)),
      engine(loaded_engine),
      valid(loaded_valid),
      search_beamwidth(beamwidth),
      local_files(std::move(files)) {
}

template <typename T>
VectorDiskArtifact<T>::VectorDiskArtifact(
    DataType elem_type,
    IndexType index_type,
    MetricType metric_type,
    IndexVersion version,
    int64_t dim,
    VectorValidData valid,
    std::vector<size_t> empty_emb_list_offsets,
    Config load_params,
    std::shared_ptr<VectorDiskLocalFiles> local_files_owner,
    std::vector<VectorDiskArtifactFile> local_files)
    : state_(std::in_place_type<BuilderState>,
             elem_type,
             std::move(index_type),
             std::move(metric_type),
             version,
             dim,
             std::move(valid),
             std::move(empty_emb_list_offsets),
             std::move(load_params),
             std::move(local_files_owner),
             std::move(local_files)) {
    const auto& state = std::get<BuilderState>(state_);
    AssertInfo(state.local_files_owner != nullptr,
               "vector disk artifact has no staging owner");
    AssertInfo(state.dim > 0 ||
                   (state.dim == 0 && PhysicalVectorDataType<T>() ==
                                          DataType::VECTOR_SPARSE_U32_F32),
               "vector disk artifact dimension is invalid");
    AssertInfo(!state.local_files.empty(),
               "vector disk artifact has no completed files");
    if (state.empty_emb_list_offsets != nullptr) {
        const auto& offsets = *state.empty_emb_list_offsets;
        AssertInfo(state.elem_type != DataType::NONE,
                   "ordinary vector artifact has embedding-list offsets");
        AssertInfo(
            !offsets.empty() && offsets.front() == 0 && offsets.back() == 0,
            "empty embedding-list artifact offsets are invalid");
    }
}

template <typename T>
VectorDiskArtifact<T>::VectorDiskArtifact(
    const KnowhereEngine& engine,
    const VectorValidData& valid,
    uint32_t search_beamwidth,
    std::shared_ptr<VectorDiskLocalFiles> local_files_owner,
    std::shared_ptr<VectorDiskBuildFileManager> file_manager,
    std::vector<VectorDiskArtifactFile> local_files)
    : state_(std::in_place_type<LoadedState>,
             std::move(local_files_owner),
             std::move(file_manager),
             engine,
             valid,
             search_beamwidth,
             std::move(local_files)) {
    const auto& state = std::get<LoadedState>(state_);
    AssertInfo(
        state.local_files_owner != nullptr && state.file_manager != nullptr,
        "loaded vector disk artifact has no staging owner");
    AssertInfo(state.engine.Dim() > 0 || (state.engine.Dim() == 0 &&
                                          PhysicalVectorDataType<T>() ==
                                              DataType::VECTOR_SPARSE_U32_F32),
               "loaded vector disk artifact dimension is invalid");
    AssertInfo(!state.local_files.empty(),
               "loaded vector disk artifact has no completed files");
}

template <typename T>
std::unique_ptr<storage::LoadedArtifact>
VectorDiskArtifact<T>::OpenReader() const {
    if (const auto* loaded = std::get_if<LoadedState>(&state_)) {
        std::shared_ptr<const void> owner = loaded->file_manager;
        return std::make_unique<VectorDiskReader<T>>(loaded->engine,
                                                     loaded->valid,
                                                     loaded->search_beamwidth,
                                                     std::move(owner));
    }

    const auto& builder = std::get<BuilderState>(state_);
    uint32_t beamwidth = kDefaultBeamwidth;
    if (builder.index_type == knowhere::IndexEnum::INDEX_DISKANN &&
        builder.load_params.contains(DISK_ANN_QUERY_BEAMWIDTH)) {
        const auto parsed =
            ParsePositiveInt(builder.load_params.at(DISK_ANN_QUERY_BEAMWIDTH),
                             DISK_ANN_QUERY_BEAMWIDTH);
        if (parsed < kMinDiskAnnBeamwidth || parsed > kMaxDiskAnnBeamwidth) {
            ThrowInfo(ConfigInvalid,
                      "DiskANN beamwidth {} is outside [{}, {}]",
                      parsed,
                      kMinDiskAnnBeamwidth,
                      kMaxDiskAnnBeamwidth);
        }
        beamwidth = static_cast<uint32_t>(parsed);
    }
    auto file_manager =
        std::make_shared<VectorDiskBuildFileManager>(builder.local_files_owner);
    auto manager = std::static_pointer_cast<milvus::FileManager>(file_manager);
    auto pack = knowhere::Pack(std::move(manager));
    KnowhereEngine engine(PhysicalVectorDataType<T>(),
                          builder.elem_type,
                          builder.index_type,
                          builder.metric_type,
                          builder.version,
                          pack,
                          true);
    engine.SetDim(builder.dim);
    if (builder.empty_emb_list_offsets != nullptr) {
        engine.SetEmptyEmbListOffsets(builder.empty_emb_list_offsets);
    }

    const bool has_engine_state =
        !(builder.valid.Enabled() && builder.valid.ValidCount() == 0) &&
        builder.empty_emb_list_offsets == nullptr;
    if (has_engine_state) {
        auto config = builder.load_params;
        PrepareDirectLoadConfig(
            config, builder.index_type, file_manager->IndexPrefix());
        const auto status =
            engine.Raw().Deserialize(knowhere::BinarySet{}, config);
        file_manager->RethrowFirstFailure();
        if (status != knowhere::Status::success) {
            ThrowInfo(knowhere::ToSegcoreErrorCode(status),
                      "failed to open built disk vector index: status {} ({})",
                      static_cast<int>(status),
                      knowhere::Status2String(status));
        }
        const auto loaded_dim = engine.Raw().Dim();
        AssertInfo(loaded_dim == builder.dim,
                   "built disk vector dimension {} changed to {} while "
                   "opening",
                   builder.dim,
                   loaded_dim);
        engine.SetDim(loaded_dim);
    }

    std::shared_ptr<const void> owner = file_manager;
    return std::make_unique<VectorDiskReader<T>>(
        std::move(engine), builder.valid, beamwidth, std::move(owner));
}

template <typename T>
void
VectorDiskArtifact<T>::Serialize(storage::FileSink& sink) const {
    if (sink.Gen() != storage::Generation::V1V2) {
        ThrowInfo(Unsupported,
                  "disk vector artifacts have no V3 persisted format");
    }

    const auto* owner = std::visit(
        [](const auto& state) { return state.local_files_owner.get(); },
        state_);
    const auto& files = std::visit(
        [](const auto& state) -> const std::vector<VectorDiskArtifactFile>& {
            return state.local_files;
        },
        state_);
    AssertInfo(owner != nullptr, "vector disk artifact owner is missing");

    std::set<std::string> names;
    for (const auto& file : files) {
        AssertInfo(file.transport == VectorDiskFileTransport::LegacySliced ||
                       file.transport == VectorDiskFileTransport::RawUnsliced,
                   "vector disk artifact file has an invalid transport");
        std::error_code error;
        if (!owner->Owns(file.path) ||
            !std::filesystem::is_regular_file(file.path, error) || error) {
            ThrowInfo(FileReadFailed,
                      "vector disk artifact file {} is unavailable{}{}",
                      file.path,
                      error ? ": " : "",
                      error ? error.message() : "");
        }
        const auto actual_size = std::filesystem::file_size(file.path, error);
        if (error || actual_size != file.size) {
            ThrowInfo(FileReadFailed,
                      "vector disk artifact file {} size changed from {}{}{}",
                      file.path,
                      file.size,
                      error ? ": " : " to ",
                      error ? error.message() : std::to_string(actual_size));
        }
        const auto name = std::filesystem::path(file.path).filename().string();
        AssertInfo(!name.empty() && names.insert(name).second,
                   "vector disk artifact has a duplicate or empty file name "
                   "{}",
                   name);
    }

    for (const auto& file : files) {
        const auto name = std::filesystem::path(file.path).filename().string();
        switch (file.transport) {
            case VectorDiskFileTransport::LegacySliced:
                sink.WriteEntryFromLocalFile(name, file.path);
                break;
            case VectorDiskFileTransport::RawUnsliced:
                sink.WriteRawEntryFromLocalFile(name, file.path);
                break;
        }
    }
}

template class VectorDiskArtifact<float>;
template class VectorDiskArtifact<float16>;
template class VectorDiskArtifact<bfloat16>;
template class VectorDiskArtifact<bin1>;
template class VectorDiskArtifact<sparse_u32_f32>;
template class VectorDiskArtifact<int8>;

}  // namespace milvus::index
