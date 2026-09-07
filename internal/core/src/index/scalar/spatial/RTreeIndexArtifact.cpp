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

#include "index/scalar/spatial/RTreeIndexArtifact.h"

#include <algorithm>
#include <cerrno>
#include <cctype>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <set>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "index/scalar/spatial/RTreeEngine.h"
#include "index/scalar/spatial/RTreeIndexReader.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

constexpr std::string_view kArchiveEntry = "index_file.bgi";
constexpr std::string_view kMetadataEntry = "index_file.meta.json";
constexpr std::string_view kNullOffsetsEntry = "index_null_offset";
constexpr std::string_view kArchiveSuffix = ".bgi";

bool
EndsWith(std::string_view value, std::string_view suffix) {
    return value.size() >= suffix.size() &&
           value.substr(value.size() - suffix.size()) == suffix;
}

void
ValidateEngineFiles(const std::vector<std::string>& files) {
    std::set<std::string> unique;
    size_t archives = 0;
    for (const auto& name : files) {
        const std::filesystem::path path(name);
        if (name.empty() || name.find('\0') != std::string::npos ||
            path.filename().string() != name || name == "." || name == ".." ||
            name == kNullOffsetsEntry || !unique.insert(name).second) {
            ThrowInfo(DataFormatBroken,
                      "invalid or duplicate R-Tree engine entry {}",
                      name);
        }
        archives += EndsWith(name, kArchiveSuffix) ? 1 : 0;
    }
    if (archives != 1) {
        ThrowInfo(DataFormatBroken,
                  "R-Tree artifact must contain exactly one .bgi archive, got "
                  "{}",
                  archives);
    }
}

std::vector<std::string>
DiscoverBuilderFiles(const std::string& directory) {
    std::vector<std::string> files;
    for (const auto name : {kArchiveEntry, kMetadataEntry}) {
        const auto path =
            (std::filesystem::path(directory) / std::string(name)).string();
        std::error_code error;
        const auto exists = std::filesystem::is_regular_file(path, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to inspect R-Tree artifact file {}: {}",
                      path,
                      error.message());
        }
        if (exists) {
            files.emplace_back(name);
        }
    }
    ValidateEngineFiles(files);
    std::sort(files.begin(), files.end());
    return files;
}

std::string
FindBasePath(const std::string& directory,
             const std::vector<std::string>& files) {
    for (const auto& name : files) {
        if (EndsWith(name, kArchiveSuffix)) {
            const auto path =
                (std::filesystem::path(directory) / name).string();
            return path.substr(0, path.size() - kArchiveSuffix.size());
        }
    }
    ThrowInfo(DataFormatBroken, "R-Tree artifact has no .bgi archive");
}

}  // namespace

RTreeIndexDirectory::RTreeIndexDirectory(std::string path,
                                         bool remove_on_destroy)
    : path_(std::move(path)), remove_on_destroy_(remove_on_destroy) {
}

std::shared_ptr<RTreeIndexDirectory>
RTreeIndexDirectory::FromPath(std::string path, bool remove_on_destroy) {
    AssertInfo(!path.empty(), "R-Tree artifact directory must not be empty");
    AssertInfo(path.find('\0') == std::string::npos,
               "R-Tree artifact directory contains an embedded NUL");
    return std::shared_ptr<RTreeIndexDirectory>(
        new RTreeIndexDirectory(std::move(path), remove_on_destroy));
}

std::shared_ptr<RTreeIndexDirectory>
RTreeIndexDirectory::CreateOwned(const std::string& parent,
                                 std::string_view label) {
    AssertInfo(parent.find('\0') == std::string::npos,
               "R-Tree staging parent contains an embedded NUL");
    std::error_code error;
    auto root = parent.empty() ? std::filesystem::temp_directory_path(error)
                               : std::filesystem::path(parent);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to locate temporary directory for R-Tree: {}",
                  error.message());
    }
    std::filesystem::create_directories(root, error);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to create R-Tree staging parent {}: {}",
                  root.string(),
                  error.message());
    }

    std::string safe_label;
    safe_label.reserve(std::min<size_t>(label.size(), 24));
    for (const auto value : label) {
        if (safe_label.size() == 24) {
            break;
        }
        const auto byte = static_cast<unsigned char>(value);
        safe_label.push_back(
            std::isalnum(byte) || value == '-' || value == '_' ? value : '_');
    }
    if (safe_label.empty()) {
        safe_label = "index";
    }
    auto pattern = (root / ("milvus-rtree-" + safe_label + "-XXXXXX")).string();
    auto result = std::shared_ptr<RTreeIndexDirectory>(
        new RTreeIndexDirectory(std::move(pattern), false));
    if (::mkdtemp(result->path_.data()) == nullptr) {
        ThrowInfo(FileCreateFailed,
                  "failed to create R-Tree staging directory under {}: {}",
                  root.string(),
                  std::strerror(errno));
    }
    result->remove_on_destroy_ = true;
    return result;
}

RTreeIndexDirectory::~RTreeIndexDirectory() {
    if (remove_on_destroy_ && !path_.empty()) {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
}

const std::string&
RTreeIndexDirectory::Path() const {
    return path_;
}

RTreeIndexArtifact::RTreeIndexArtifact(std::string local_dir,
                                       std::vector<size_t> null_offsets,
                                       int64_t total_num_rows,
                                       bool owns_local_dir) {
    auto directory =
        RTreeIndexDirectory::FromPath(std::move(local_dir), owns_local_dir);
    auto engine_files = DiscoverBuilderFiles(directory->Path());
    auto offsets =
        std::make_shared<const std::vector<size_t>>(std::move(null_offsets));
    AssertInfo(total_num_rows >= 0,
               "R-Tree artifact row count must not be negative");
    state_.emplace<BuilderArtifactState>(
        BuilderArtifactState{std::move(directory),
                             std::move(engine_files),
                             std::move(offsets),
                             total_num_rows});
}

RTreeIndexArtifact::RTreeIndexArtifact(
    std::shared_ptr<RTreeIndexDirectory> directory,
    std::vector<std::string> engine_files,
    std::shared_ptr<const RTreeIndexState> loaded_state) {
    AssertInfo(directory != nullptr,
               "loaded R-Tree artifact requires a directory owner");
    AssertInfo(loaded_state != nullptr,
               "loaded R-Tree artifact requires validated state");
    ValidateEngineFiles(engine_files);
    state_.emplace<LoadedArtifactState>(
        LoadedArtifactState{std::move(directory),
                            std::move(engine_files),
                            std::move(loaded_state)});
}

RTreeIndexArtifact::~RTreeIndexArtifact() = default;

const std::shared_ptr<RTreeIndexDirectory>&
RTreeIndexArtifact::Directory() const {
    return std::visit(
        [](const auto& state) -> const auto& { return state.directory; },
        state_);
}

const std::vector<std::string>&
RTreeIndexArtifact::EngineFiles() const {
    return std::visit(
        [](const auto& state) -> const auto& { return state.engine_files; },
        state_);
}

const std::vector<size_t>&
RTreeIndexArtifact::NullOffsets() const {
    return std::visit(
        [](const auto& state) -> const std::vector<size_t>& {
            using State = std::decay_t<decltype(state)>;
            if constexpr (std::is_same_v<State, BuilderArtifactState>) {
                return *state.null_offsets;
            } else {
                return state.state->NullOffsets();
            }
        },
        state_);
}

std::shared_ptr<storage::LoadedArtifact>
RTreeIndexArtifact::OpenReader() const {
    if (const auto* loaded = std::get_if<LoadedArtifactState>(&state_)) {
        return std::make_shared<RTreeIndexReader>(loaded->state);
    }
    const auto& builder = std::get<BuilderArtifactState>(state_);
    auto engine = std::make_shared<RTreeQueryEngine>(
        FindBasePath(builder.directory->Path(), builder.engine_files));
    engine->Load();
    auto state = RTreeIndexState::Create(
        std::move(engine), builder.null_offsets, builder.total_num_rows);
    return std::make_shared<RTreeIndexReader>(std::move(state));
}

void
RTreeIndexArtifact::Serialize(storage::FileSink& sink) const {
    const auto& directory = Directory();
    const auto& files = EngineFiles();
    const auto& null_offsets = NullOffsets();
    AssertInfo(directory != nullptr, "R-Tree artifact has no directory owner");
    ValidateEngineFiles(files);
    std::vector<std::pair<std::string, std::string>> local_files;
    local_files.reserve(files.size());
    for (const auto& name : files) {
        const auto path =
            (std::filesystem::path(directory->Path()) / name).string();
        std::error_code error;
        const auto exists = std::filesystem::is_regular_file(path, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to inspect R-Tree artifact file {}: {}",
                      path,
                      error.message());
        }
        if (!exists) {
            ThrowInfo(DataFormatBroken,
                      "R-Tree artifact is missing engine entry {}",
                      name);
        }
        local_files.emplace_back(name, std::move(path));
    }

    if (sink.Gen() == storage::Generation::V3) {
        std::vector<std::string> file_names;
        file_names.reserve(local_files.size());
        for (const auto& [name, _] : local_files) {
            file_names.push_back(name);
        }
        sink.PutMeta("file_names", file_names);
        sink.PutMeta("has_null", !null_offsets.empty());
    }
    for (const auto& [name, path] : local_files) {
        sink.WriteEntryFromLocalFile(name, path);
    }
    if (!null_offsets.empty()) {
        AssertInfo(null_offsets.size() <=
                       std::numeric_limits<size_t>::max() / sizeof(size_t),
                   "R-Tree null-offset byte size overflows size_t");
        sink.WriteEntry("index_null_offset",
                        null_offsets.data(),
                        null_offsets.size() * sizeof(size_t));
    }
}

void
RTreeIndexArtifact::SerializeLegacyBinary(storage::FileSink& sink) const {
    AssertInfo(sink.Gen() == storage::Generation::V1V2,
               "R-Tree legacy binary projection requires a V1/V2 sink");
    const auto& null_offsets = NullOffsets();
    if (null_offsets.empty()) {
        return;
    }
    AssertInfo(null_offsets.size() <=
                   std::numeric_limits<size_t>::max() / sizeof(size_t),
               "R-Tree null-offset byte size overflows size_t");
    sink.WriteEntry(kNullOffsetsEntry,
                    null_offsets.data(),
                    null_offsets.size() * sizeof(size_t));
}

}  // namespace milvus::index
