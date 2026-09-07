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

#include "index/scalar/json/JsonFlatIndexLoader.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <filesystem>
#include <fcntl.h>
#include <cstring>
#include <limits>
#include <optional>
#include <set>
#include <stdexcept>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/JsonUtils.h"
#include "common/Slice.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/json/JsonFlatIndexArtifact.h"
#include "index/scalar/json/JsonFlatIndexReader.h"
#include "nlohmann/json.hpp"
#include "tantivy-wrapper.h"

namespace milvus::index {
namespace {

constexpr std::string_view kNullOffsetsEntry = "index_null_offset";
constexpr std::string_view kFileNamesMeta = "file_names";
constexpr std::string_view kHasNullMeta = "has_null";
constexpr std::string_view kJsonPathParam = "json_path";
constexpr std::string_view kJsonCastTypeParam = "json_cast_type";

int64_t
ParseInteger(const Config& params,
             std::string_view key,
             int64_t fallback,
             bool required) {
    if (!params.is_object() || !params.contains(key)) {
        if (required) {
            ThrowInfo(
                DataTypeInvalid, "JSON flat loader requires parameter {}", key);
        }
        return fallback;
    }
    const auto& encoded = params.at(key);
    if (encoded.is_number_unsigned()) {
        const auto value = encoded.get<uint64_t>();
        if (value >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            ThrowInfo(
                DataTypeInvalid, "JSON flat parameter {} is out of range", key);
        }
        return static_cast<int64_t>(value);
    }
    if (encoded.is_number_integer()) {
        return encoded.get<int64_t>();
    }
    if (encoded.is_string()) {
        const auto text = encoded.get<std::string>();
        int64_t value = 0;
        const auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), value);
        if (error == std::errc{} && end == text.data() + text.size()) {
            return value;
        }
    }
    ThrowInfo(
        DataTypeInvalid, "JSON flat parameter {} must be an integer", key);
}

DataType
ParseDataType(const Config& params, std::string_view key) {
    if (!params.is_object() || !params.contains(key)) {
        ThrowInfo(
            DataTypeInvalid, "JSON flat loader requires parameter {}", key);
    }
    const auto& encoded = params.at(key);
    if (encoded.is_string()) {
        const auto text = encoded.get<std::string>();
        if (text == "JSON") {
            return DataType::JSON;
        }
        if (text == "NONE") {
            return DataType::NONE;
        }
        int32_t value = 0;
        const auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), value);
        if (error == std::errc{} && end == text.data() + text.size()) {
            return static_cast<DataType>(value);
        }
        ThrowInfo(DataTypeInvalid,
                  "unsupported JSON flat data type {} for parameter {}",
                  text,
                  key);
    }
    const auto value = ParseInteger(params, key, 0, true);
    if (value < std::numeric_limits<int32_t>::min() ||
        value > std::numeric_limits<int32_t>::max()) {
        ThrowInfo(
            DataTypeInvalid, "JSON flat data type in {} is out of range", key);
    }
    return static_cast<DataType>(static_cast<int32_t>(value));
}

std::string
ParseString(const Config& params,
            std::string_view key,
            std::string fallback = {}) {
    if (!params.is_object() || !params.contains(key)) {
        return fallback;
    }
    try {
        return params.at(key).get<std::string>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataTypeInvalid,
                  "invalid JSON flat parameter {}: {}",
                  key,
                  error.what());
    }
}

bool
ParseBool(const nlohmann::json& encoded, std::string_view key) {
    if (encoded.is_boolean()) {
        return encoded.get<bool>();
    }
    if (encoded.is_string()) {
        const auto text = encoded.get<std::string>();
        if (text == "true") {
            return true;
        }
        if (text == "false") {
            return false;
        }
    }
    ThrowInfo(DataTypeInvalid, "JSON flat parameter {} must be boolean", key);
}

void
ValidateRowDomain(const Config& params) {
    std::optional<bool> nested;
    std::string_view first_key;
    for (const auto key : {std::string_view("nested"),
                           std::string_view("is_nested"),
                           std::string_view("is_nested_index")}) {
        if (!params.contains(key)) {
            continue;
        }
        const auto value = ParseBool(params.at(key), key);
        if (nested.has_value() && *nested != value) {
            ThrowInfo(DataTypeInvalid,
                      "JSON flat nested parameters {} and {} disagree",
                      first_key,
                      key);
        }
        if (!nested.has_value()) {
            nested = value;
            first_key = key;
        }
    }
    if (!nested.has_value()) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat loader requires an explicit normalized nested "
                  "parameter");
    }
    if (*nested) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat indexes support only the row coordinate domain");
    }
}

std::string
ParseJsonPath(const Config& params) {
    const bool has_json_path = params.contains(kJsonPathParam);
    const bool has_nested_path = params.contains("nested_path");
    const auto json_path = ParseString(params, kJsonPathParam);
    const auto nested_path = ParseString(params, "nested_path");
    if (has_json_path && has_nested_path && json_path != nested_path) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat json_path and nested_path disagree");
    }
    auto result = has_json_path ? json_path : nested_path;
    if (result.find('\0') != std::string::npos) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat root path contains an embedded NUL");
    }
    try {
        (void)parse_json_pointer(result);
    } catch (const std::invalid_argument& error) {
        ThrowInfo(DataTypeInvalid,
                  "invalid JSON flat root path {}: {}",
                  result,
                  error.what());
    }
    return result;
}

void
ValidateTantivyVersion(const Config& params) {
    const auto scalar_version =
        ParseInteger(params, SCALAR_INDEX_ENGINE_VERSION, 1, false);
    const auto explicit_version =
        ParseInteger(params, TANTIVY_INDEX_VERSION, 0, false);
    if (scalar_version < 0 || explicit_version < 0 ||
        explicit_version > std::numeric_limits<uint32_t>::max()) {
        ThrowInfo(DataTypeInvalid,
                  "invalid JSON flat engine versions scalar={} tantivy={}",
                  scalar_version,
                  explicit_version);
    }
    if (explicit_version != 0 &&
        explicit_version != TANTIVY_INDEX_MINIMUM_VERSION &&
        explicit_version != TANTIVY_INDEX_LATEST_VERSION) {
        ThrowInfo(DataTypeInvalid,
                  "unsupported JSON flat Tantivy version {}",
                  explicit_version);
    }
}

struct RuntimeParams {
    std::string nested_path;
};

RuntimeParams
ParseRuntimeParams(const Config& params) {
    if (!params.is_object()) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat load parameters must be an object");
    }
    ValidateRowDomain(params);
    if (ParseDataType(params, "field_type") != DataType::JSON ||
        ParseDataType(params, "value_type") != DataType::JSON) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat loader requires JSON field_type and value_type");
    }
    if (ParseString(params, kJsonCastTypeParam) != "JSON") {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat loader requires JSON json_cast_type");
    }
    for (const auto key : {std::string_view("element_type"),
                           std::string_view("array_element_type")}) {
        if (params.contains(key) &&
            ParseDataType(params, key) != DataType::NONE) {
            ThrowInfo(
                DataTypeInvalid, "JSON flat loader does not accept {}", key);
        }
    }
    const auto field_id = ParseInteger(params, FIELD_ID, 0, true);
    if (field_id < 0) {
        ThrowInfo(DataTypeInvalid, "JSON flat field_id must be non-negative");
    }
    ValidateTantivyVersion(params);
    return RuntimeParams{.nested_path = ParseJsonPath(params)};
}

struct EffectiveLoadOptions {
    bool mmap{false};
    std::string directory_parent;
};

EffectiveLoadOptions
ResolveLoadOptions(const storage::LoadOptions& opts) {
    EffectiveLoadOptions result;
    result.mmap = opts.enable_mmap;
    if (opts.params.is_object() && opts.params.contains(ENABLE_MMAP)) {
        result.mmap =
            result.mmap || ParseBool(opts.params.at(ENABLE_MMAP), ENABLE_MMAP);
    }
    result.directory_parent = opts.mmap_dir_path;
    if (result.directory_parent.empty() && opts.params.is_object() &&
        opts.params.contains(MMAP_FILE_PATH)) {
        result.directory_parent = ParseString(opts.params, MMAP_FILE_PATH);
        if (result.directory_parent.find('\0') != std::string::npos) {
            ThrowInfo(DataTypeInvalid,
                      "JSON flat mmap directory contains an embedded NUL");
        }
    } else {
        AssertInfo(result.directory_parent.find('\0') == std::string::npos,
                   "JSON flat load options contain an invalid staging path");
    }
    return result;
}

template <typename T>
T
GetRequiredMeta(storage::FileSource& source, std::string_view key) {
    const auto value = source.GetMeta(key);
    if (!value.has_value()) {
        ThrowInfo(DataFormatBroken, "JSON flat V3 metadata {} is missing", key);
    }
    try {
        return value->get<T>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataFormatBroken,
                  "invalid JSON flat V3 metadata {}: {}",
                  key,
                  error.what());
    }
}

void
ValidateEntryName(std::string_view name) {
    const std::filesystem::path path(name);
    if (name.empty() || name.find('\0') != std::string_view::npos ||
        path.filename().string() != name || name == "." || name == "..") {
        ThrowInfo(
            DataFormatBroken, "invalid JSON flat index entry name {}", name);
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
            GetRequiredMeta<std::vector<std::string>>(source, kFileNamesMeta);
        result.has_null = GetRequiredMeta<bool>(source, kHasNullMeta);
        std::set<std::string> expected;
        for (const auto& name : result.engine_files) {
            ValidateEntryName(name);
            if (name == kNullOffsetsEntry || name == INDEX_TYPE ||
                !expected.insert(name).second) {
                ThrowInfo(DataFormatBroken,
                          "invalid or duplicate JSON flat engine entry {}",
                          name);
            }
            if (!source.HasEntry(name)) {
                ThrowInfo(DataFormatBroken,
                          "JSON flat engine entry {} is missing",
                          name);
            }
        }
        if (source.HasEntry(kNullOffsetsEntry) != result.has_null) {
            ThrowInfo(DataFormatBroken,
                      "JSON flat V3 has_null metadata disagrees with sidecar");
        }
        if (result.has_null) {
            expected.emplace(kNullOffsetsEntry);
        }
        std::set<std::string> actual;
        for (const auto& name : source.EntryNames()) {
            ValidateEntryName(name);
            if (!actual.insert(name).second) {
                ThrowInfo(DataFormatBroken,
                          "duplicate JSON flat artifact entry {}",
                          name);
            }
        }
        if (actual != expected) {
            ThrowInfo(DataFormatBroken,
                      "JSON flat V3 entries disagree with file_names "
                      "metadata");
        }
    } else {
        result.has_null = source.HasEntry(kNullOffsetsEntry);
        std::set<std::string> unique;
        for (const auto& name : source.EntryNames()) {
            ValidateEntryName(name);
            if (name == kNullOffsetsEntry || name == INDEX_TYPE ||
                name == INDEX_FILE_SLICE_META) {
                continue;
            }
            if (!unique.insert(name).second) {
                ThrowInfo(DataFormatBroken,
                          "duplicate JSON flat engine entry {}",
                          name);
            }
            result.engine_files.push_back(name);
        }
    }
    if (result.engine_files.empty()) {
        ThrowInfo(DataFormatBroken, "JSON flat artifact has no engine files");
    }
    return result;
}

class LocalEntryGuard {
 public:
    explicit LocalEntryGuard(std::string path) : path_(std::move(path)) {
    }

    ~LocalEntryGuard() {
        Remove(/*report_error=*/false);
    }

    LocalEntryGuard(const LocalEntryGuard&) = delete;
    LocalEntryGuard&
    operator=(const LocalEntryGuard&) = delete;

    const std::string&
    Path() const {
        return path_;
    }

    void
    RemoveChecked() {
        Remove(/*report_error=*/true);
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
                      "failed to remove JSON flat staging file {}: {}",
                      path,
                      std::strerror(errno));
        }
    }

    std::string path_;
};

class FileDescriptorGuard {
 public:
    explicit FileDescriptorGuard(int fd) : fd_(fd) {
    }

    ~FileDescriptorGuard() {
        if (fd_ != -1) {
            ::close(fd_);
        }
    }

    FileDescriptorGuard(const FileDescriptorGuard&) = delete;
    FileDescriptorGuard&
    operator=(const FileDescriptorGuard&) = delete;

    int
    Get() const {
        return fd_;
    }

    void
    CloseChecked(const std::string& path) {
        const auto fd = std::exchange(fd_, -1);
        if (::close(fd) != 0) {
            ThrowInfo(FileReadFailed,
                      "failed to close JSON flat staging file {}: {}",
                      path,
                      std::strerror(errno));
        }
    }

 private:
    int fd_{-1};
};

size_t
FileSize(const std::string& path) {
    std::error_code error;
    const auto size = std::filesystem::file_size(path, error);
    if (error || size > std::numeric_limits<size_t>::max()) {
        ThrowInfo(FileReadFailed,
                  "failed to determine JSON flat entry size for {}: {}",
                  path,
                  error.message());
    }
    return static_cast<size_t>(size);
}

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
                      "failed to read JSON flat staging file {}: {}",
                      path,
                      std::strerror(errno));
        }
        if (read_size == 0) {
            ThrowInfo(FileReadFailed,
                      "unexpected EOF while reading JSON flat staging file {}",
                      path);
        }
        cursor += read_size;
        size -= static_cast<size_t>(read_size);
    }
}

std::shared_ptr<const std::vector<size_t>>
ReadNullOffsets(storage::FileSource& source,
                bool has_null,
                const std::string& staging_parent,
                size_t count) {
    if (!has_null) {
        return std::make_shared<const std::vector<size_t>>();
    }

    std::optional<size_t> expected_bytes;
    if (source.Gen() == storage::Generation::V3) {
        const auto declared = source.EntrySize(kNullOffsetsEntry);
        if (declared < 0 || static_cast<uint64_t>(declared) >
                                std::numeric_limits<size_t>::max()) {
            ThrowInfo(DataFormatBroken,
                      "invalid JSON flat null-offset byte size {}",
                      declared);
        }
        expected_bytes = static_cast<size_t>(declared);
    }

    auto staging = JsonFlatIndexDirectory::Create(staging_parent);
    auto path = (std::filesystem::path(staging->Path()) /
                 std::string(kNullOffsetsEntry))
                    .string();
    LocalEntryGuard local(std::move(path));
    source.ReadEntryToLocalFile(kNullOffsetsEntry, local.Path());
    const auto bytes = FileSize(local.Path());
    if (expected_bytes.has_value() && bytes != *expected_bytes) {
        ThrowInfo(DataFormatBroken,
                  "JSON flat null-offset size changed from {} to {}",
                  *expected_bytes,
                  bytes);
    }

    if (bytes == 0 || bytes % sizeof(size_t) != 0 ||
        bytes / sizeof(size_t) > count) {
        ThrowInfo(DataFormatBroken,
                  "invalid JSON flat null-offset byte size {} for count {}",
                  bytes,
                  count);
    }

    auto result = std::make_shared<std::vector<size_t>>(bytes / sizeof(size_t));
    const auto fd = ::open(local.Path().c_str(), O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        ThrowInfo(FileOpenFailed,
                  "failed to open JSON flat staging file {}: {}",
                  local.Path(),
                  std::strerror(errno));
    }
    FileDescriptorGuard descriptor(fd);
    ReadAll(descriptor.Get(), result->data(), bytes, local.Path());
    descriptor.CloseChecked(local.Path());
    local.RemoveChecked();
    return result;
}

size_t
MaterializedBytes(const std::vector<std::string>& paths) {
    size_t total = 0;
    for (const auto& path : paths) {
        std::error_code error;
        const auto bytes = std::filesystem::file_size(path, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to inspect JSON flat entry {}: {}",
                      path,
                      error.message());
        }
        if (bytes > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken,
                      "JSON flat artifact byte size overflows size_t");
        }
        total += static_cast<size_t>(bytes);
    }
    return total;
}

size_t
RamPayloadBytes(milvus::tantivy::TantivyIndexWrapper& engine) {
    const auto bytes = engine.index_size_bytes();
    if (bytes > std::numeric_limits<size_t>::max()) {
        ThrowInfo(DataFormatBroken,
                  "RAM JSON flat payload exceeds size_t domain");
    }
    return static_cast<size_t>(bytes);
}

struct JsonFlatLoadState {
    // Shared state releases its engine before the final publication directory.
    std::shared_ptr<JsonFlatIndexDirectory> directory;
    std::vector<std::string> engine_files;
    std::shared_ptr<const JsonFlatIndexReaderState> state;
};

JsonFlatLoadState
LoadState(storage::FileSource& source, const storage::LoadOptions& opts) {
    const auto params = ParseRuntimeParams(opts.params);
    const auto effective = ResolveLoadOptions(opts);
    const auto entries = ReadPersistedEntries(source);

    auto directory = JsonFlatIndexDirectory::Create(effective.directory_parent);
    const auto paths =
        source.ReadEntriesToLocalDir(entries.engine_files, directory->Path());
    if (paths.size() != entries.engine_files.size()) {
        ThrowInfo(DataFormatBroken,
                  "JSON flat source materialized {} of {} engine entries",
                  paths.size(),
                  entries.engine_files.size());
    }
    for (size_t i = 0; i < paths.size(); ++i) {
        if (std::filesystem::path(paths[i]).filename().string() !=
            entries.engine_files[i]) {
            ThrowInfo(DataFormatBroken,
                      "JSON flat source materialized entry {} as {}",
                      entries.engine_files[i],
                      paths[i]);
        }
    }
    const auto mapped_bytes =
        effective.mmap ? MaterializedBytes(paths) : size_t{0};
    if (!tantivy_index_exist(directory->Path().c_str())) {
        ThrowInfo(DataFormatBroken,
                  "materialized JSON flat artifact is not a Tantivy index");
    }

    // Keep the directory and engine local until state construction succeeds.
    // RAM engines copy their directory and the resulting state does not retain
    // this publication owner; mmap engines receive the owner explicitly.
    auto engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
        directory->Path().c_str(), effective.mmap, SetBitsetSealed);
    const auto count = static_cast<size_t>(engine->count());
    auto null_offsets = ReadNullOffsets(
        source, entries.has_null, effective.directory_parent, count);
    const auto engine_bytes =
        effective.mmap ? mapped_bytes : RamPayloadBytes(*engine);
    auto state =
        JsonFlatIndexReaderState::Create(effective.mmap ? directory : nullptr,
                                         engine,
                                         params.nested_path,
                                         std::move(null_offsets),
                                         effective.mmap,
                                         engine_bytes,
                                         directory->PathHeapBytes());

    JsonFlatLoadState result;
    result.directory = std::move(directory);
    result.engine_files = entries.engine_files;
    result.state = std::move(state);
    return result;
}

}  // namespace

std::string
JsonFlatIndexLoader::Family() const {
    return families::kJsonFlat;
}

ReaderCaps
JsonFlatIndexLoader::DeriveCaps(const Config& index_meta) const {
    static_cast<void>(ParseRuntimeParams(index_meta));
    return ReaderCaps{.json_paths = true, .exact = true};
}

std::unique_ptr<IndexReaderBase>
JsonFlatIndexLoader::OpenIndex(storage::FileSource& source,
                               const storage::LoadOptions& opts) {
    auto loaded = LoadState(source, opts);
    return std::make_unique<JsonFlatIndexReader>(std::move(loaded.state));
}

RehydratedIndex
JsonFlatIndexLoader::OpenForRewrite(storage::FileSource& source,
                                    const storage::LoadOptions& opts) {
    auto loaded = LoadState(source, opts);
    RehydratedIndex result;
    result.artifact = std::make_unique<JsonFlatIndexArtifact>(
        loaded.directory, loaded.engine_files, loaded.state);
    result.reader =
        std::make_unique<JsonFlatIndexReader>(std::move(loaded.state));
    return result;
}

namespace {

const bool kJsonFlatLoaderRegistered = [] {
    LoaderRegistry::Instance().Register(
        families::kJsonFlat, std::make_shared<JsonFlatIndexLoader>());
    return true;
}();

}  // namespace

}  // namespace milvus::index
