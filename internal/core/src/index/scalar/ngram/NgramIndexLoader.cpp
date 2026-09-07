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

#include "index/scalar/ngram/NgramIndexLoader.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cctype>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Slice.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/json/JsonProjectedIndexLoad.h"
#include "index/scalar/ngram/NgramIndexArtifact.h"
#include "index/scalar/ngram/NgramIndexReader.h"
#include "nlohmann/json.hpp"
#include "tantivy-wrapper.h"

namespace milvus::index {
namespace {

constexpr std::string_view kNullOffsetsEntry = "index_null_offset";
constexpr std::string_view kAvgRowSizeEntry = "ngram_avg_row_size";
constexpr std::string_view kFileNamesMeta = "file_names";
constexpr std::string_view kHasNullMeta = "has_null";
constexpr size_t kDefaultAvgRowSize = 5000;

bool
IsStringType(DataType type) {
    return type == DataType::STRING || type == DataType::VARCHAR ||
           type == DataType::TEXT;
}

std::string
Upper(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](char c) {
        return static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    });
    return value;
}

DataType
ParseDataType(const Config& params, std::string_view key, DataType fallback) {
    if (!params.is_object() || !params.contains(key)) {
        return fallback;
    }
    const auto& encoded = params.at(key);
    if (encoded.is_number_unsigned()) {
        try {
            const auto value = encoded.get<uint64_t>();
            if (value >
                static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
                ThrowInfo(DataTypeInvalid,
                          "NGRAM data type in {} is out of range",
                          key);
            }
            return static_cast<DataType>(static_cast<int32_t>(value));
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid NGRAM data type in {}: {}",
                      key,
                      error.what());
        }
    }
    if (encoded.is_number_integer()) {
        try {
            const auto value = encoded.get<int64_t>();
            if (value < std::numeric_limits<int32_t>::min() ||
                value > std::numeric_limits<int32_t>::max()) {
                ThrowInfo(DataTypeInvalid,
                          "NGRAM data type in {} is out of range",
                          key);
            }
            return static_cast<DataType>(static_cast<int32_t>(value));
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid NGRAM data type in {}: {}",
                      key,
                      error.what());
        }
    }
    if (!encoded.is_string()) {
        ThrowInfo(
            DataTypeInvalid, "NGRAM parameter {} must be a data type", key);
    }
    const auto text = Upper(encoded.get<std::string>());
    static const std::map<std::string, DataType> kNames = {
        {"STRING", DataType::STRING},
        {"VARCHAR", DataType::VARCHAR},
        {"TEXT", DataType::TEXT},
        {"JSON", DataType::JSON},
    };
    if (const auto it = kNames.find(text); it != kNames.end()) {
        return it->second;
    }
    int64_t numeric = 0;
    const auto* begin = text.data();
    const auto* end = begin + text.size();
    const auto parsed = std::from_chars(begin, end, numeric);
    if (parsed.ec == std::errc{} && parsed.ptr == end &&
        numeric >= std::numeric_limits<int32_t>::min() &&
        numeric <= std::numeric_limits<int32_t>::max()) {
        return static_cast<DataType>(static_cast<int32_t>(numeric));
    }
    ThrowInfo(DataTypeInvalid,
              "unsupported NGRAM data type {} for parameter {}",
              text,
              key);
}

uint64_t
ParseUnsigned(const Config& params,
              std::string_view key,
              uint64_t fallback,
              bool required) {
    if (!params.is_object() || !params.contains(key)) {
        if (required) {
            ThrowInfo(DataTypeInvalid, "NGRAM requires parameter {}", key);
        }
        return fallback;
    }
    const auto& encoded = params.at(key);
    if (encoded.is_number_unsigned()) {
        try {
            return encoded.get<uint64_t>();
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid NGRAM parameter {}: {}",
                      key,
                      error.what());
        }
    }
    if (encoded.is_number_integer()) {
        try {
            const auto value = encoded.get<int64_t>();
            if (value < 0) {
                ThrowInfo(DataTypeInvalid,
                          "NGRAM parameter {} must be non-negative",
                          key);
            }
            return static_cast<uint64_t>(value);
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid NGRAM parameter {}: {}",
                      key,
                      error.what());
        }
    }
    if (encoded.is_string()) {
        try {
            const auto text = encoded.get<std::string>();
            uint64_t value = 0;
            const auto* begin = text.data();
            const auto* end = begin + text.size();
            const auto parsed = std::from_chars(begin, end, value);
            if (parsed.ec == std::errc{} && parsed.ptr == end) {
                return value;
            }
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid NGRAM parameter {}: {}",
                      key,
                      error.what());
        }
    }
    ThrowInfo(DataTypeInvalid, "NGRAM parameter {} must be an integer", key);
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
                  "invalid NGRAM parameter {}: {}",
                  key,
                  error.what());
    }
}

bool
ParseBoolValue(const nlohmann::json& encoded, std::string_view key) {
    if (encoded.is_boolean()) {
        return encoded.get<bool>();
    }
    if (encoded.is_string()) {
        const auto text = Upper(encoded.get<std::string>());
        if (text == "TRUE") {
            return true;
        }
        if (text == "FALSE") {
            return false;
        }
    }
    ThrowInfo(DataTypeInvalid, "NGRAM parameter {} must be boolean", key);
}

bool
ReadBool(const Config& params, std::string_view key, bool fallback) {
    if (!params.is_object() || !params.contains(key)) {
        return fallback;
    }
    return ParseBoolValue(params.at(key), key);
}

void
ValidateRowDomain(const Config& params) {
    std::optional<bool> nested;
    std::string_view first_key;
    for (const auto key : {std::string_view("nested"),
                           std::string_view("is_nested"),
                           std::string_view("is_nested_index")}) {
        if (!params.is_object() || !params.contains(key)) {
            continue;
        }
        const auto value = ParseBoolValue(params.at(key), key);
        if (nested.has_value() && *nested != value) {
            ThrowInfo(DataTypeInvalid,
                      "NGRAM nested parameters {} and {} disagree",
                      first_key,
                      key);
        }
        if (!nested.has_value()) {
            nested = value;
            first_key = key;
        }
    }
    if (nested.value_or(false)) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM supports only the row coordinate domain");
    }
}

std::string
ParseJsonPath(const Config& params) {
    const bool has_json_path = params.is_object() && params.contains(JSON_PATH);
    const bool has_nested_path =
        params.is_object() && params.contains("nested_path");
    const auto json_path = ParseString(params, JSON_PATH);
    const auto nested_path = ParseString(params, "nested_path");
    if (has_json_path && has_nested_path && json_path != nested_path) {
        ThrowInfo(DataTypeInvalid, "NGRAM json_path and nested_path disagree");
    }
    return has_json_path ? json_path : nested_path;
}

struct RuntimeParams {
    DataType value_type{DataType::VARCHAR};
    uintptr_t min_gram{0};
    uintptr_t max_gram{0};
};

RuntimeParams
ParseRuntimeParams(const Config& params) {
    if (!params.is_object()) {
        ThrowInfo(DataTypeInvalid, "NGRAM load parameters must be an object");
    }
    ValidateRowDomain(params);

    const auto path = ParseJsonPath(params);
    RuntimeParams result;
    const auto field_type =
        ParseDataType(params,
                      "field_type",
                      path.empty() ? DataType::VARCHAR : DataType::JSON);
    const auto configured_value_type = ParseDataType(
        params,
        "value_type",
        field_type == DataType::JSON ? DataType::VARCHAR : field_type);
    if (!IsStringType(configured_value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM value_type must be STRING, VARCHAR, or TEXT");
    }
    if (field_type == DataType::JSON) {
        if (path.empty()) {
            ThrowInfo(DataTypeInvalid, "JSON NGRAM requires json_path");
        }
        if (Upper(ParseString(params, JSON_CAST_TYPE)) != "VARCHAR") {
            ThrowInfo(DataTypeInvalid,
                      "JSON NGRAM requires VARCHAR json_cast_type");
        }
        result.value_type = DataType::VARCHAR;
    } else {
        if (!IsStringType(field_type)) {
            ThrowInfo(
                DataTypeInvalid,
                "NGRAM field_type must be STRING, VARCHAR, TEXT, or JSON");
        }
        if (!path.empty()) {
            ThrowInfo(DataTypeInvalid,
                      "scalar NGRAM must not carry a JSON path");
        }
        result.value_type = configured_value_type;
    }

    const auto min_gram = ParseUnsigned(params, MIN_GRAM, 0, true);
    const auto max_gram = ParseUnsigned(params, MAX_GRAM, 0, true);
    if (min_gram == 0 || max_gram == 0 || min_gram > max_gram ||
        min_gram > std::numeric_limits<uintptr_t>::max() ||
        max_gram > std::numeric_limits<uintptr_t>::max()) {
        ThrowInfo(DataTypeInvalid,
                  "invalid NGRAM range min_gram={} max_gram={}",
                  min_gram,
                  max_gram);
    }
    result.min_gram = static_cast<uintptr_t>(min_gram);
    result.max_gram = static_cast<uintptr_t>(max_gram);

    const auto version = ParseUnsigned(params, TANTIVY_INDEX_VERSION, 0, false);
    if (version != 0 && version != TANTIVY_INDEX_LATEST_VERSION) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM supports only Tantivy index version {}",
                  TANTIVY_INDEX_LATEST_VERSION);
    }
    return result;
}

struct EffectiveLoadOptions {
    bool mmap{false};
    std::string directory_parent;
};

EffectiveLoadOptions
ResolveLoadOptions(const storage::LoadOptions& opts) {
    EffectiveLoadOptions result;
    result.mmap = opts.enable_mmap || ReadBool(opts.params, ENABLE_MMAP, false);
    result.directory_parent = opts.mmap_dir_path;
    if (result.directory_parent.empty() && opts.params.is_object() &&
        opts.params.contains(MMAP_FILE_PATH)) {
        result.directory_parent = ParseString(opts.params, MMAP_FILE_PATH);
    }
    if (result.directory_parent.find('\0') != std::string::npos) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM staging directory must not contain NUL");
    }
    return result;
}

template <typename T>
T
GetRequiredMeta(storage::FileSource& source, std::string_view key) {
    const auto value = source.GetMeta(key);
    if (!value.has_value()) {
        ThrowInfo(DataFormatBroken, "NGRAM V3 metadata {} is missing", key);
    }
    try {
        return value->get<T>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataFormatBroken,
                  "invalid NGRAM V3 metadata {}: {}",
                  key,
                  error.what());
    }
}

void
ValidateEntryName(std::string_view name) {
    const std::filesystem::path path(name);
    if (name.empty() || name.find('\0') != std::string_view::npos ||
        path.filename().string() != name || name == "." || name == "..") {
        ThrowInfo(DataFormatBroken, "invalid NGRAM index entry name {}", name);
    }
}

struct PersistedEntries {
    std::vector<std::string> engine_files;
    bool has_null{false};
    bool has_avg{false};
};

PersistedEntries
ReadPersistedEntries(storage::FileSource& source) {
    PersistedEntries result;
    if (source.Gen() == storage::Generation::V3) {
        result.engine_files =
            GetRequiredMeta<std::vector<std::string>>(source, kFileNamesMeta);
        result.has_null = GetRequiredMeta<bool>(source, kHasNullMeta);
        result.has_avg = source.HasEntry(kAvgRowSizeEntry);
        if (!result.has_avg) {
            ThrowInfo(DataFormatBroken,
                      "NGRAM V3 average-row-size entry is missing");
        }
        std::set<std::string> unique;
        for (const auto& name : result.engine_files) {
            ValidateEntryName(name);
            if (name == kNullOffsetsEntry || name == kAvgRowSizeEntry ||
                name == INDEX_TYPE ||
                name == INDEX_NON_EXIST_OFFSET_FILE_NAME ||
                !unique.insert(name).second) {
                ThrowInfo(DataFormatBroken,
                          "invalid or duplicate NGRAM engine entry {}",
                          name);
            }
            if (!source.HasEntry(name)) {
                ThrowInfo(
                    DataFormatBroken, "NGRAM engine entry {} is missing", name);
            }
        }
        if (source.HasEntry(kNullOffsetsEntry) != result.has_null) {
            ThrowInfo(DataFormatBroken,
                      "NGRAM V3 has_null metadata disagrees with sidecar");
        }
    } else {
        result.has_null = source.HasEntry(kNullOffsetsEntry);
        result.has_avg = source.HasEntry(kAvgRowSizeEntry);
        std::set<std::string> unique;
        for (const auto& name : source.EntryNames()) {
            ValidateEntryName(name);
            if (name == kNullOffsetsEntry || name == kAvgRowSizeEntry ||
                name == INDEX_TYPE ||
                name == INDEX_NON_EXIST_OFFSET_FILE_NAME ||
                name == INDEX_FILE_SLICE_META) {
                continue;
            }
            if (!unique.insert(name).second) {
                ThrowInfo(
                    DataFormatBroken, "duplicate NGRAM engine entry {}", name);
            }
            result.engine_files.push_back(name);
        }
    }
    if (result.engine_files.empty()) {
        ThrowInfo(DataFormatBroken, "NGRAM artifact has no engine files");
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
                      "failed to remove NGRAM staging file {}: {}",
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
                      "failed to close NGRAM staging file {}: {}",
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
                  "failed to determine NGRAM entry size for {}: {}",
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
                      "failed to read NGRAM staging file {}: {}",
                      path,
                      std::strerror(errno));
        }
        if (read_size == 0) {
            ThrowInfo(FileReadFailed,
                      "unexpected EOF while reading NGRAM staging file {}",
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
                      "invalid NGRAM null-offset byte size {}",
                      declared);
        }
        expected_bytes = static_cast<size_t>(declared);
    }

    // The sidecar uses a separate owned child so it can never be enumerated
    // as a Tantivy file by the publishable engine directory.
    auto staging = NgramIndexDirectory::Create(staging_parent);
    auto path =
        (std::filesystem::path(staging->Path()) / kNullOffsetsEntry).string();
    LocalEntryGuard local(std::move(path));
    source.ReadEntryToLocalFile(kNullOffsetsEntry, local.Path());
    const auto bytes = FileSize(local.Path());
    if (expected_bytes.has_value() && bytes != *expected_bytes) {
        ThrowInfo(DataFormatBroken,
                  "NGRAM null-offset size changed from {} to {}",
                  *expected_bytes,
                  bytes);
    }
    if (bytes == 0 || bytes % sizeof(size_t) != 0) {
        ThrowInfo(
            DataFormatBroken, "invalid NGRAM null-offset byte size {}", bytes);
    }
    const auto offset_count = bytes / sizeof(size_t);
    if (offset_count > count) {
        ThrowInfo(DataFormatBroken,
                  "NGRAM null-offset count {} exceeds row count {}",
                  offset_count,
                  count);
    }

    std::vector<size_t> result(offset_count);
    const auto fd = ::open(local.Path().c_str(), O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        ThrowInfo(FileOpenFailed,
                  "failed to open NGRAM staging file {}: {}",
                  local.Path(),
                  std::strerror(errno));
    }
    FileDescriptorGuard descriptor(fd);
    ReadAll(descriptor.Get(), result.data(), bytes, local.Path());
    descriptor.CloseChecked(local.Path());
    local.RemoveChecked();
    return std::make_shared<const std::vector<size_t>>(std::move(result));
}

size_t
ReadAvgRowSize(storage::FileSource& source, bool has_avg) {
    if (!has_avg) {
        return kDefaultAvgRowSize;
    }
    const auto bytes = source.ReadEntry(kAvgRowSizeEntry);
    if (bytes.size() != sizeof(size_t)) {
        ThrowInfo(DataFormatBroken,
                  "invalid NGRAM average-row-size byte size {}",
                  bytes.size());
    }
    size_t result = 0;
    std::memcpy(&result, bytes.data(), sizeof(result));
    return result;
}

size_t
MaterializedBytes(const std::vector<std::string>& paths,
                  bool ram_payload_only) {
    size_t total = 0;
    for (const auto& path : paths) {
        // Tantivy's in-RAM open copies every regular file except *.lock.
        if (ram_payload_only && std::string_view(path).ends_with(".lock")) {
            continue;
        }
        std::error_code error;
        const auto bytes = std::filesystem::file_size(path, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to inspect NGRAM entry {}: {}",
                      path,
                      error.message());
        }
        if (bytes > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken,
                      "NGRAM artifact byte size overflows size_t");
        }
        total += static_cast<size_t>(bytes);
    }
    return total;
}

struct NgramLoadState {
    // Declaration order makes the engine release before its backing directory.
    std::shared_ptr<NgramIndexDirectory> directory;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine;
    std::shared_ptr<const std::vector<size_t>> null_offsets;
    RuntimeParams params;
    size_t avg_row_size{0};
    bool mmap{false};
    size_t engine_bytes{0};
};

NgramLoadState
LoadState(storage::FileSource& source,
          const storage::LoadOptions& opts,
          RuntimeParams params) {
    const auto effective = ResolveLoadOptions(opts);
    const auto entries = ReadPersistedEntries(source);
    const auto avg_row_size = ReadAvgRowSize(source, entries.has_avg);

    // Keep local owners intact until the state has acquired its own shared
    // references. On every exception the engine is released before directory.
    auto directory = NgramIndexDirectory::Create(effective.directory_parent);
    const auto paths =
        source.ReadEntriesToLocalDir(entries.engine_files, directory->Path());
    if (paths.size() != entries.engine_files.size()) {
        ThrowInfo(DataFormatBroken,
                  "NGRAM source materialized {} of {} engine entries",
                  paths.size(),
                  entries.engine_files.size());
    }
    for (size_t i = 0; i < paths.size(); ++i) {
        if (std::filesystem::path(paths[i]).filename().string() !=
            entries.engine_files[i]) {
            ThrowInfo(DataFormatBroken,
                      "NGRAM source materialized entry {} as {}",
                      entries.engine_files[i],
                      paths[i]);
        }
    }
    const auto engine_bytes = MaterializedBytes(paths, !effective.mmap);
    if (!tantivy_index_exist(directory->Path().c_str())) {
        ThrowInfo(DataFormatBroken,
                  "materialized NGRAM artifact is not a Tantivy index");
    }
    auto engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
        directory->Path().c_str(), effective.mmap, SetBitsetSealed);
    auto null_offsets = ReadNullOffsets(source,
                                        entries.has_null,
                                        effective.directory_parent,
                                        static_cast<size_t>(engine->count()));

    NgramLoadState result;
    result.directory = directory;
    result.engine = engine;
    result.null_offsets = std::move(null_offsets);
    result.params = std::move(params);
    result.avg_row_size = avg_row_size;
    result.mmap = effective.mmap;
    result.engine_bytes = engine_bytes;
    return result;
}

std::unique_ptr<IndexReaderBase>
MakeReader(const NgramLoadState& state) {
    AssertInfo(state.directory != nullptr,
               "NGRAM load state requires a directory owner");
    AssertInfo(state.engine != nullptr, "NGRAM load state requires an engine");
    AssertInfo(state.null_offsets != nullptr,
               "NGRAM load state requires immutable null offsets");
    return std::make_unique<NgramIndexReader>(
        state.mmap ? state.directory : nullptr,
        state.engine,
        state.null_offsets,
        state.params.value_type,
        state.params.min_gram,
        state.params.max_gram,
        state.avg_row_size,
        state.mmap,
        state.engine_bytes);
}

}  // namespace

std::string
NgramIndexLoader::Family() const {
    return families::kNgram;
}

ReaderCaps
NgramIndexLoader::DeriveCaps(const Config& index_meta) const {
    static_cast<void>(ParseRuntimeParams(index_meta));
    return DeriveJsonProjectedCaps(
        families::kNgram,
        index_meta,
        ReaderCaps{.ngram_candidates = true, .exact = false});
}

std::unique_ptr<IndexReaderBase>
NgramIndexLoader::OpenIndex(storage::FileSource& source,
                            const storage::LoadOptions& opts) {
    auto projection = PrepareJsonProjectedOpen(families::kNgram, source, opts);
    auto state = LoadState(source, opts, ParseRuntimeParams(opts.params));
    auto reader = MakeReader(state);
    return FinishJsonProjectedOpen(
        std::move(projection), source, std::move(reader));
}

RehydratedIndex
NgramIndexLoader::OpenForRewrite(storage::FileSource& source,
                                 const storage::LoadOptions& opts) {
    auto projection = PrepareJsonProjectedOpen(families::kNgram, source, opts);
    auto state = LoadState(source, opts, ParseRuntimeParams(opts.params));

    // Keep state intact until both recipients exist. If either allocation
    // throws, its engine still releases before the final directory owner.
    RehydratedIndex inner;
    inner.artifact =
        std::make_unique<NgramIndexArtifact>(state.directory,
                                             state.null_offsets,
                                             state.params.value_type,
                                             state.params.min_gram,
                                             state.params.max_gram,
                                             state.avg_row_size);
    inner.reader = MakeReader(state);
    return FinishJsonProjectedRewrite(
        std::move(projection), source, std::move(inner));
}

namespace {

const bool kNgramLoaderRegistered = [] {
    LoaderRegistry::Instance().Register(families::kNgram,
                                        std::make_shared<NgramIndexLoader>());
    return true;
}();

}  // namespace

}  // namespace milvus::index
