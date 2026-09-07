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

#include "index/scalar/spatial/RTreeIndexLoader.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cctype>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <optional>
#include <set>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/spatial/RTreeEngine.h"
#include "index/scalar/spatial/RTreeIndexArtifact.h"
#include "index/scalar/spatial/RTreeIndexReader.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

constexpr std::string_view kArchiveSuffix = ".bgi";
constexpr std::string_view kNullOffsets = "index_null_offset";

DataType
ParseDataTypeValue(const nlohmann::json& value, std::string_view key) {
    int64_t numeric = 0;
    if (value.is_number_unsigned()) {
        const auto unsigned_numeric = value.get<uint64_t>();
        if (unsigned_numeric >
            static_cast<uint64_t>(std::numeric_limits<int>::max())) {
            ThrowInfo(DataTypeInvalid,
                      "R-Tree parameter {} is not a valid data type",
                      key);
        }
        numeric = static_cast<int64_t>(unsigned_numeric);
    } else if (value.is_number_integer()) {
        numeric = value.get<int64_t>();
    } else if (value.is_string()) {
        const auto text = value.get<std::string>();
        if (text == "GEOMETRY") {
            return DataType::GEOMETRY;
        }
        if (text == "NONE") {
            return DataType::NONE;
        }
        const auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), numeric);
        if (error != std::errc() || end != text.data() + text.size()) {
            ThrowInfo(DataTypeInvalid,
                      "unsupported R-Tree data type {} for parameter {}",
                      text,
                      key);
        }
    } else {
        ThrowInfo(
            DataTypeInvalid, "R-Tree parameter {} must be a data type", key);
    }
    if (numeric < std::numeric_limits<int>::min() ||
        numeric > std::numeric_limits<int>::max()) {
        ThrowInfo(DataTypeInvalid,
                  "R-Tree parameter {} is not a valid data type",
                  key);
    }
    return static_cast<DataType>(static_cast<int>(numeric));
}

std::optional<DataType>
ReadDataType(const Config& params, std::string_view key) {
    if (!params.contains(key) || params.at(key).is_null()) {
        return std::nullopt;
    }
    return ParseDataTypeValue(params.at(key), key);
}

bool
ParseBoolValue(const nlohmann::json& value, std::string_view key) {
    if (value.is_boolean()) {
        return value.get<bool>();
    }
    if (value.is_number_unsigned()) {
        const auto numeric = value.get<uint64_t>();
        if (numeric == 0 || numeric == 1) {
            return numeric != 0;
        }
    } else if (value.is_number_integer()) {
        const auto numeric = value.get<int64_t>();
        if (numeric == 0 || numeric == 1) {
            return numeric != 0;
        }
    } else if (value.is_string()) {
        auto text = value.get<std::string>();
        std::transform(text.begin(), text.end(), text.begin(), [](char ch) {
            return static_cast<char>(
                std::tolower(static_cast<unsigned char>(ch)));
        });
        if (text == "true" || text == "1") {
            return true;
        }
        if (text == "false" || text == "0") {
            return false;
        }
    }
    ThrowInfo(DataTypeInvalid, "R-Tree parameter {} must be a boolean", key);
}

void
ValidateGeometryParams(const Config& params) {
    for (const auto key :
         {std::string_view("field_type"), std::string_view("value_type")}) {
        const auto type = ReadDataType(params, key);
        if (type.has_value() && *type != DataType::GEOMETRY) {
            ThrowInfo(DataTypeInvalid,
                      "R-Tree parameter {} must be GEOMETRY, got {}",
                      key,
                      static_cast<int>(*type));
        }
    }
    for (const auto key : {std::string_view("array_element_type"),
                           std::string_view("element_type")}) {
        const auto type = ReadDataType(params, key);
        if (type.has_value() && *type != DataType::NONE) {
            ThrowInfo(DataTypeInvalid,
                      "R-Tree parameter {} must be NONE, got {}",
                      key,
                      static_cast<int>(*type));
        }
    }

    std::optional<bool> nested;
    std::string_view first_key;
    for (const auto key : {std::string_view("nested"),
                           std::string_view("is_nested"),
                           std::string_view("is_nested_index")}) {
        if (!params.contains(key) || params.at(key).is_null()) {
            continue;
        }
        const auto value = ParseBoolValue(params.at(key), key);
        if (nested.has_value() && *nested != value) {
            ThrowInfo(DataTypeInvalid,
                      "R-Tree nested parameters {} and {} disagree",
                      first_key,
                      key);
        }
        if (!nested.has_value()) {
            nested = value;
            first_key = key;
        }
    }
    if (nested.value_or(false)) {
        ThrowInfo(DataTypeInvalid, "R-Tree does not support nested input");
    }
}

bool
EndsWith(std::string_view value, std::string_view suffix) {
    return value.size() >= suffix.size() &&
           value.substr(value.size() - suffix.size()) == suffix;
}

int64_t
ParseRowCountValue(const nlohmann::json& value, std::string_view key) {
    if (value.is_number_unsigned()) {
        const auto count = value.get<uint64_t>();
        if (count <=
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return static_cast<int64_t>(count);
        }
    } else if (value.is_number_integer()) {
        const auto count = value.get<int64_t>();
        if (count >= 0) {
            return count;
        }
    } else if (value.is_string()) {
        const auto text = value.get<std::string>();
        int64_t count = -1;
        const auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), count);
        if (error == std::errc() && end == text.data() + text.size() &&
            count >= 0) {
            return count;
        }
    }
    ThrowInfo(UnexpectedError,
              "normalized R-Tree parameter {} is not a non-negative int64",
              key);
}

int64_t
ReadRequiredRowCount(const Config& params) {
    std::optional<int64_t> result;
    std::string_view first_key;
    for (const auto key :
         {std::string_view("num_rows"), std::string_view("index_num_rows")}) {
        if (!params.contains(key)) {
            continue;
        }
        const auto count = ParseRowCountValue(params.at(key), key);
        if (result.has_value() && *result != count) {
            ThrowInfo(UnexpectedError,
                      "normalized R-Tree row counts {} and {} disagree",
                      first_key,
                      key);
        }
        if (!result.has_value()) {
            result = count;
            first_key = key;
        }
    }
    if (!result.has_value()) {
        ThrowInfo(UnexpectedError,
                  "R-Tree loader requires authoritative runtime num_rows");
    }
    return *result;
}

template <typename T>
T
GetRequiredMeta(storage::FileSource& source, std::string_view key) {
    const auto value = source.GetMeta(key);
    if (!value.has_value()) {
        ThrowInfo(
            DataFormatBroken, "R-Tree artifact metadata {} is missing", key);
    }
    try {
        return value->get<T>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataFormatBroken,
                  "invalid R-Tree artifact metadata {}: {}",
                  key,
                  error.what());
    }
}

void
ValidateEntryNames(const std::vector<std::string>& names) {
    std::set<std::string> unique;
    size_t archives = 0;
    for (const auto& name : names) {
        if (name.empty() || name.find('\0') != std::string::npos ||
            std::filesystem::path(name).filename() != name || name == "." ||
            name == ".." || name == kNullOffsets) {
            ThrowInfo(DataFormatBroken,
                      "invalid R-Tree artifact entry name {}",
                      name);
        }
        if (!unique.insert(name).second) {
            ThrowInfo(
                DataFormatBroken, "duplicate R-Tree artifact entry {}", name);
        }
        if (EndsWith(name, kArchiveSuffix)) {
            ++archives;
        }
    }
    if (archives != 1) {
        ThrowInfo(DataFormatBroken,
                  "R-Tree artifact must contain exactly one .bgi archive, got "
                  "{}",
                  archives);
    }
}

struct PersistedEntries {
    std::vector<std::string> engine_files;
    bool has_null{false};
};

PersistedEntries
ReadPersistedEntries(storage::FileSource& source) {
    PersistedEntries result;
    if (source.Gen() == storage::Generation::V3) {
        result.engine_files =
            GetRequiredMeta<std::vector<std::string>>(source, "file_names");
        result.has_null = GetRequiredMeta<bool>(source, "has_null");
        ValidateEntryNames(result.engine_files);

        std::set<std::string> expected(result.engine_files.begin(),
                                       result.engine_files.end());
        if (result.has_null) {
            expected.emplace(kNullOffsets);
        }
        std::set<std::string> actual;
        for (const auto& name : source.EntryNames()) {
            if (name.empty() || name.find('\0') != std::string::npos ||
                std::filesystem::path(name).filename().string() != name ||
                name == "." || name == ".." || !actual.insert(name).second) {
                ThrowInfo(DataFormatBroken,
                          "invalid or duplicate R-Tree artifact entry {}",
                          name);
            }
        }
        if (actual != expected) {
            ThrowInfo(DataFormatBroken,
                      "R-Tree V3 entries disagree with file_names/has_null "
                      "metadata");
        }
    } else {
        std::set<std::string> unique;
        for (const auto& name : source.EntryNames()) {
            if (name.empty() || name.find('\0') != std::string::npos ||
                std::filesystem::path(name).filename().string() != name ||
                name == "." || name == ".." || !unique.insert(name).second) {
                ThrowInfo(DataFormatBroken,
                          "invalid or duplicate R-Tree artifact entry {}",
                          name);
            }
            if (name == kNullOffsets) {
                result.has_null = true;
            } else {
                result.engine_files.push_back(name);
            }
        }
        ValidateEntryNames(result.engine_files);
    }
    return result;
}

std::string
FindBasePath(const std::vector<std::string>& local_paths) {
    for (const auto& path : local_paths) {
        if (EndsWith(path, kArchiveSuffix)) {
            return path.substr(0, path.size() - kArchiveSuffix.size());
        }
    }
    ThrowInfo(DataFormatBroken,
              "materialized R-Tree artifact has no .bgi archive");
}

class LocalEntryGuard final {
 public:
    explicit LocalEntryGuard(std::string path) : path_(std::move(path)) {
    }

    LocalEntryGuard(const LocalEntryGuard&) = delete;
    LocalEntryGuard&
    operator=(const LocalEntryGuard&) = delete;

    ~LocalEntryGuard() {
        Remove(false);
    }

    const std::string&
    Path() const {
        return path_;
    }

    void
    RemoveChecked() {
        Remove(true);
    }

 private:
    void
    Remove(bool report_error) {
        if (path_.empty()) {
            return;
        }
        const auto path = std::exchange(path_, {});
        if (::unlink(path.c_str()) != 0 && errno != ENOENT && report_error) {
            ThrowInfo(FileWriteFailed,
                      "failed to remove R-Tree NULL staging file {}: {}",
                      path,
                      std::strerror(errno));
        }
    }

    std::string path_;
};

class FileDescriptorGuard final {
 public:
    explicit FileDescriptorGuard(int fd) : fd_(fd) {
    }

    FileDescriptorGuard(const FileDescriptorGuard&) = delete;
    FileDescriptorGuard&
    operator=(const FileDescriptorGuard&) = delete;

    ~FileDescriptorGuard() {
        if (fd_ != -1) {
            ::close(fd_);
        }
    }

    int
    Get() const {
        return fd_;
    }

    void
    CloseChecked(const std::string& path) {
        const auto fd = std::exchange(fd_, -1);
        if (::close(fd) != 0) {
            ThrowInfo(FileReadFailed,
                      "failed to close R-Tree NULL staging file {}: {}",
                      path,
                      std::strerror(errno));
        }
    }

 private:
    int fd_{-1};
};

void
ReadAll(int fd, void* output, size_t size, const std::string& path) {
    auto* cursor = static_cast<uint8_t*>(output);
    while (size != 0) {
        const auto request = std::min(
            size, static_cast<size_t>(std::numeric_limits<ssize_t>::max()));
        const auto read_size = ::read(fd, cursor, request);
        if (read_size < 0 && errno == EINTR) {
            continue;
        }
        if (read_size < 0) {
            ThrowInfo(FileReadFailed,
                      "failed to read R-Tree NULL staging file {}: {}",
                      path,
                      std::strerror(errno));
        }
        if (read_size == 0) {
            ThrowInfo(FileReadFailed,
                      "unexpected EOF while reading R-Tree NULL staging file "
                      "{}",
                      path);
        }
        cursor += read_size;
        size -= static_cast<size_t>(read_size);
    }
}

std::shared_ptr<const std::vector<size_t>>
ReadNullOffsets(storage::FileSource& source,
                bool has_null,
                int64_t total_num_rows,
                const std::string& staging_parent) {
    if (!has_null) {
        return std::make_shared<const std::vector<size_t>>();
    }
    if (!source.HasEntry(kNullOffsets)) {
        ThrowInfo(DataFormatBroken,
                  "R-Tree artifact declares nulls but has no {} entry",
                  kNullOffsets);
    }

    auto directory = RTreeIndexDirectory::CreateOwned(staging_parent, "null");
    auto path =
        (std::filesystem::path(directory->Path()) / kNullOffsets).string();
    LocalEntryGuard local(std::move(path));
    source.ReadEntryToLocalFile(kNullOffsets, local.Path());

    std::error_code error;
    const auto observed = std::filesystem::file_size(local.Path(), error);
    if (error || observed > std::numeric_limits<size_t>::max()) {
        ThrowInfo(FileReadFailed,
                  "failed to determine R-Tree NULL sidecar size {}: {}",
                  local.Path(),
                  error.message());
    }
    const auto bytes = static_cast<size_t>(observed);
    if (bytes == 0 || bytes % sizeof(size_t) != 0 ||
        bytes / sizeof(size_t) > static_cast<size_t>(total_num_rows)) {
        ThrowInfo(DataFormatBroken,
                  "R-Tree null-offset byte size {} is invalid for {} rows",
                  bytes,
                  total_num_rows);
    }

    auto offsets =
        std::make_shared<std::vector<size_t>>(bytes / sizeof(size_t));
    const auto fd = ::open(local.Path().c_str(), O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        ThrowInfo(FileOpenFailed,
                  "failed to open R-Tree NULL staging file {}: {}",
                  local.Path(),
                  std::strerror(errno));
    }
    FileDescriptorGuard descriptor(fd);
    ReadAll(descriptor.Get(), offsets->data(), bytes, local.Path());
    descriptor.CloseChecked(local.Path());
    local.RemoveChecked();
    return offsets;
}

struct RTreeLoadState {
    // The validated state releases its engine before the final directory owner.
    std::shared_ptr<RTreeIndexDirectory> directory;
    std::vector<std::string> engine_files;
    std::shared_ptr<const RTreeIndexState> state;
};

RTreeLoadState
LoadState(storage::FileSource& source, const storage::LoadOptions& opts) {
    ValidateGeometryParams(opts.params);
    const auto total_num_rows = ReadRequiredRowCount(opts.params);
    AssertInfo(opts.mmap_dir_path.find('\0') == std::string::npos,
               "R-Tree load options contain an invalid staging path");
    auto entries = ReadPersistedEntries(source);

    auto directory =
        RTreeIndexDirectory::CreateOwned(opts.mmap_dir_path, "load");
    auto local_paths =
        source.ReadEntriesToLocalDir(entries.engine_files, directory->Path());
    if (local_paths.size() != entries.engine_files.size()) {
        ThrowInfo(DataFormatBroken,
                  "R-Tree source materialized {} of {} engine entries",
                  local_paths.size(),
                  entries.engine_files.size());
    }
    for (size_t i = 0; i < local_paths.size(); ++i) {
        if (std::filesystem::path(local_paths[i]).filename().string() !=
            entries.engine_files[i]) {
            ThrowInfo(DataFormatBroken,
                      "R-Tree source materialized entry {} as {}",
                      entries.engine_files[i],
                      local_paths[i]);
        }
    }

    auto engine = std::make_shared<RTreeQueryEngine>(FindBasePath(local_paths));
    engine->Load();
    auto null_offsets = ReadNullOffsets(
        source, entries.has_null, total_num_rows, opts.mmap_dir_path);
    auto state = RTreeIndexState::Create(
        std::move(engine), std::move(null_offsets), total_num_rows);

    RTreeLoadState result;
    result.directory = std::move(directory);
    result.engine_files = std::move(entries.engine_files);
    result.state = std::move(state);
    return result;
}

}  // namespace

std::string
RTreeIndexLoader::Family() const {
    return families::kRTree;
}

ReaderCaps
RTreeIndexLoader::DeriveCaps(const Config& index_meta) const {
    ValidateGeometryParams(index_meta);
    return ReaderCaps{.spatial = true, .exact = false};
}

std::unique_ptr<IndexReaderBase>
RTreeIndexLoader::OpenIndex(storage::FileSource& source,
                            const storage::LoadOptions& opts) {
    // Boost's R-tree archive has no mmap view. `enable_mmap` is a preference;
    // preserve the baseline heap fallback and report the resulting heap bytes.
    auto loaded = LoadState(source, opts);
    return std::make_unique<RTreeIndexReader>(std::move(loaded.state));
}

RehydratedIndex
RTreeIndexLoader::OpenForRewrite(storage::FileSource& source,
                                 const storage::LoadOptions& opts) {
    auto loaded = LoadState(source, opts);
    RehydratedIndex result;
    result.artifact = std::make_unique<RTreeIndexArtifact>(
        loaded.directory, loaded.engine_files, loaded.state);
    result.reader = std::make_unique<RTreeIndexReader>(loaded.state);
    return result;
}

namespace {

const bool kRTreeLoaderRegistered = [] {
    LoaderRegistry::Instance().Register(families::kRTree,
                                        std::make_shared<RTreeIndexLoader>());
    return true;
}();

}  // namespace

}  // namespace milvus::index
