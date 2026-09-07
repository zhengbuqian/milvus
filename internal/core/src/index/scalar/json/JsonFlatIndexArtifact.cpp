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

#include "index/scalar/json/JsonFlatIndexArtifact.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <limits>
#include <set>
#include <unistd.h>
#include <utility>

#include "common/EasyAssert.h"
#include "index/Utils.h"
#include "index/scalar/json/JsonFlatIndexReader.h"
#include "nlohmann/json.hpp"
#include "tantivy-wrapper.h"

namespace milvus::index {
namespace {

constexpr std::string_view kNullOffsetsEntry = "index_null_offset";
constexpr std::string_view kFileNamesMeta = "file_names";
constexpr std::string_view kHasNullMeta = "has_null";

class EmptyDirectoryGuard {
 public:
    explicit EmptyDirectoryGuard(const char* path) : path_(path) {
    }

    ~EmptyDirectoryGuard() {
        if (path_ != nullptr) {
            rmdir(path_);
        }
    }

    void
    Release() {
        path_ = nullptr;
    }

 private:
    const char* path_;
};

size_t
DirectoryBytes(const std::string& directory) {
    size_t total = 0;
    std::error_code error;
    for (std::filesystem::directory_iterator it(directory, error), end;
         !error && it != end;
         it.increment(error)) {
        if (!it->is_regular_file(error)) {
            if (error) {
                break;
            }
            continue;
        }
        const auto bytes = it->file_size(error);
        if (error) {
            break;
        }
        if (bytes > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken,
                      "JSON flat directory byte size overflows size_t");
        }
        total += static_cast<size_t>(bytes);
    }
    if (error) {
        ThrowInfo(FileReadFailed,
                  "failed to inspect JSON flat directory {}: {}",
                  directory,
                  error.message());
    }
    return total;
}

std::vector<std::filesystem::path>
IndexFiles(const std::string& directory) {
    std::vector<std::filesystem::path> files;
    std::error_code error;
    for (std::filesystem::directory_iterator it(directory, error), end;
         !error && it != end;
         it.increment(error)) {
        if (!it->is_directory(error)) {
            if (error) {
                break;
            }
            files.push_back(it->path());
        }
    }
    if (error) {
        ThrowInfo(FileReadFailed,
                  "failed to enumerate JSON flat directory {}: {}",
                  directory,
                  error.message());
    }
    std::sort(files.begin(), files.end());
    if (files.empty()) {
        ThrowInfo(DataFormatBroken, "JSON flat artifact has no engine files");
    }
    for (const auto& file : files) {
        if (file.filename().string() == kNullOffsetsEntry) {
            ThrowInfo(DataFormatBroken,
                      "JSON flat engine file conflicts with null sidecar {}",
                      kNullOffsetsEntry);
        }
    }
    return files;
}

void
ValidateEngineFiles(const std::vector<std::string>& files) {
    if (files.empty()) {
        ThrowInfo(DataFormatBroken, "JSON flat artifact has no engine files");
    }
    std::set<std::string> unique;
    for (const auto& name : files) {
        const std::filesystem::path path(name);
        if (name.empty() || name.find('\0') != std::string::npos ||
            path.filename().string() != name || name == "." || name == ".." ||
            name == kNullOffsetsEntry || !unique.insert(name).second) {
            ThrowInfo(DataFormatBroken,
                      "invalid or duplicate JSON flat engine entry {}",
                      name);
        }
    }
}

void
ValidateMaterializedFiles(const std::vector<std::filesystem::path>& files) {
    for (const auto& file : files) {
        std::error_code error;
        const auto exists = std::filesystem::is_regular_file(file, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to inspect JSON flat engine file {}: {}",
                      file.string(),
                      error.message());
        }
        if (!exists) {
            ThrowInfo(DataFormatBroken,
                      "JSON flat artifact is missing engine file {}",
                      file.filename().string());
        }
    }
}

}  // namespace

JsonFlatIndexDirectory::JsonFlatIndexDirectory(std::string path)
    : path_(std::move(path)) {
}

std::shared_ptr<JsonFlatIndexDirectory>
JsonFlatIndexDirectory::Create(const std::string& parent) {
    std::error_code error;
    auto root = parent.empty() ? std::filesystem::temp_directory_path(error)
                               : std::filesystem::path(parent);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to locate JSON flat temporary directory: {}",
                  error.message());
    }
    std::filesystem::create_directories(root, error);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to create JSON flat staging root {}: {}",
                  root.string(),
                  error.message());
    }

    auto pattern = (root / "json_flat_XXXXXX").string();
    std::vector<char> mutable_pattern(pattern.begin(), pattern.end());
    mutable_pattern.push_back('\0');
    if (mkdtemp(mutable_pattern.data()) == nullptr) {
        ThrowInfo(FileCreateFailed,
                  "failed to create JSON flat staging directory in {}: {}",
                  root.string(),
                  std::strerror(errno));
    }
    EmptyDirectoryGuard guard(mutable_pattern.data());
    auto result = std::shared_ptr<JsonFlatIndexDirectory>(
        new JsonFlatIndexDirectory(mutable_pattern.data()));
    guard.Release();
    return result;
}

JsonFlatIndexDirectory::~JsonFlatIndexDirectory() {
    if (!path_.empty()) {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }
}

const std::string&
JsonFlatIndexDirectory::Path() const {
    return path_;
}

size_t
JsonFlatIndexDirectory::PathHeapBytes() const {
    const auto inline_capacity = std::string{}.capacity();
    if (path_.capacity() <= inline_capacity) {
        return 0;
    }
    if (path_.capacity() == std::numeric_limits<size_t>::max()) {
        ThrowInfo(DataFormatBroken,
                  "JSON flat directory path memory size overflows");
    }
    return path_.capacity() + 1;
}

size_t
JsonFlatIndexDirectory::HeapBytes() const {
    const auto path_bytes = PathHeapBytes();
    if (path_bytes >
        std::numeric_limits<size_t>::max() - sizeof(JsonFlatIndexDirectory)) {
        ThrowInfo(DataFormatBroken,
                  "JSON flat directory memory size overflows");
    }
    return sizeof(JsonFlatIndexDirectory) + path_bytes;
}

JsonFlatIndexArtifact::JsonFlatIndexArtifact(
    std::shared_ptr<JsonFlatIndexDirectory> directory,
    std::vector<size_t> null_offsets,
    std::string nested_path)
    : state_(BuilderArtifactState{
          std::move(directory),
          std::make_shared<const std::vector<size_t>>(std::move(null_offsets)),
          std::move(nested_path)}) {
    AssertInfo(std::get<BuilderArtifactState>(state_).directory != nullptr,
               "JSON flat artifact requires an owned directory");
}

JsonFlatIndexArtifact::JsonFlatIndexArtifact(
    std::shared_ptr<JsonFlatIndexDirectory> directory,
    std::vector<std::string> engine_files,
    std::shared_ptr<const JsonFlatIndexReaderState> state)
    : state_(LoadedArtifactState{
          std::move(directory), std::move(engine_files), std::move(state)}) {
    const auto& loaded = std::get<LoadedArtifactState>(state_);
    AssertInfo(loaded.directory != nullptr,
               "loaded JSON flat artifact requires a publication directory");
    AssertInfo(loaded.state != nullptr,
               "loaded JSON flat artifact requires shared reader state");
    ValidateEngineFiles(loaded.engine_files);
}

JsonFlatIndexArtifact::~JsonFlatIndexArtifact() = default;

std::shared_ptr<storage::LoadedArtifact>
JsonFlatIndexArtifact::OpenReader() const {
    if (const auto* loaded = std::get_if<LoadedArtifactState>(&state_)) {
        return std::make_shared<JsonFlatIndexReader>(loaded->state);
    }
    const auto& builder = std::get<BuilderArtifactState>(state_);
    const auto engine_bytes = DirectoryBytes(builder.directory->Path());
    auto engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
        builder.directory->Path().c_str(), true, SetBitsetSealed);
    auto state =
        JsonFlatIndexReaderState::Create(builder.directory,
                                         std::move(engine),
                                         builder.nested_path,
                                         builder.null_offsets,
                                         true,
                                         engine_bytes,
                                         builder.directory->PathHeapBytes());
    return std::make_shared<JsonFlatIndexReader>(std::move(state));
}

void
JsonFlatIndexArtifact::Serialize(storage::FileSink& sink) const {
    std::shared_ptr<JsonFlatIndexDirectory> directory;
    std::vector<std::filesystem::path> files;
    const std::vector<size_t>* null_offsets = nullptr;
    if (const auto* builder = std::get_if<BuilderArtifactState>(&state_)) {
        directory = builder->directory;
        files = IndexFiles(directory->Path());
        null_offsets = builder->null_offsets.get();
    } else {
        const auto& loaded = std::get<LoadedArtifactState>(state_);
        directory = loaded.directory;
        ValidateEngineFiles(loaded.engine_files);
        files.reserve(loaded.engine_files.size());
        for (const auto& name : loaded.engine_files) {
            files.emplace_back(std::filesystem::path(directory->Path()) / name);
        }
        null_offsets = &loaded.state->NullOffsets();
    }
    AssertInfo(directory != nullptr && null_offsets != nullptr,
               "JSON flat artifact state is incomplete");
    ValidateMaterializedFiles(files);
    if (sink.Gen() == storage::Generation::V3) {
        std::vector<std::string> file_names;
        file_names.reserve(files.size());
        for (const auto& file : files) {
            file_names.push_back(file.filename().string());
        }
        sink.PutMeta(kFileNamesMeta, nlohmann::json(file_names));
        sink.PutMeta(kHasNullMeta, nlohmann::json(!null_offsets->empty()));
    }

    for (const auto& file : files) {
        sink.WriteEntryFromLocalFile(file.filename().string(), file.string());
    }
    if (!null_offsets->empty()) {
        AssertInfo(null_offsets->size() <=
                       std::numeric_limits<size_t>::max() / sizeof(size_t),
                   "JSON flat null-offset byte size overflows size_t");
        sink.WriteEntry(kNullOffsetsEntry,
                        null_offsets->data(),
                        null_offsets->size() * sizeof(size_t));
    }
}

}  // namespace milvus::index
