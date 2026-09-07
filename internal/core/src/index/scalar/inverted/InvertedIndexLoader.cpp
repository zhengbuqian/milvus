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

#include "index/scalar/inverted/InvertedIndexLoader.h"

#include <algorithm>
#include <cerrno>
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
#include "index/scalar/inverted/InvertedIndexArtifact.h"
#include "index/scalar/inverted/InvertedIndexReader.h"
#include "index/scalar/json/JsonProjectedIndexLoad.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

constexpr std::string_view kNullOffsetsEntry = "index_null_offset";
constexpr std::string_view kFileNamesMeta = "file_names";
constexpr std::string_view kHasNullMeta = "has_null";

bool
IsStringType(DataType type) {
    return type == DataType::STRING || type == DataType::VARCHAR ||
           type == DataType::TEXT;
}

bool
IsSupportedType(DataType type) {
    return type == DataType::BOOL || type == DataType::INT8 ||
           type == DataType::INT16 || type == DataType::INT32 ||
           type == DataType::INT64 || type == DataType::TIMESTAMPTZ ||
           type == DataType::FLOAT || type == DataType::DOUBLE ||
           IsStringType(type);
}

bool
CompatibleType(DataType first, DataType second) {
    return first == second || (IsStringType(first) && IsStringType(second)) ||
           ((first == DataType::INT64 || first == DataType::TIMESTAMPTZ) &&
            (second == DataType::INT64 || second == DataType::TIMESTAMPTZ));
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
                          "inverted data type in {} is out of range",
                          key);
            }
            return static_cast<DataType>(static_cast<int32_t>(value));
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid inverted data type in {}: {}",
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
                          "inverted data type in {} is out of range",
                          key);
            }
            return static_cast<DataType>(static_cast<int32_t>(value));
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid inverted data type in {}: {}",
                      key,
                      error.what());
        }
    }
    if (!encoded.is_string()) {
        ThrowInfo(
            DataTypeInvalid, "inverted parameter {} must be a data type", key);
    }
    const auto text = encoded.get<std::string>();
    static const std::map<std::string, DataType> kNames = {
        {"BOOL", DataType::BOOL},
        {"INT8", DataType::INT8},
        {"INT16", DataType::INT16},
        {"INT32", DataType::INT32},
        {"INT64", DataType::INT64},
        {"FLOAT", DataType::FLOAT},
        {"DOUBLE", DataType::DOUBLE},
        {"STRING", DataType::STRING},
        {"VARCHAR", DataType::VARCHAR},
        {"TEXT", DataType::TEXT},
        {"TIMESTAMPTZ", DataType::TIMESTAMPTZ},
        {"JSON", DataType::JSON},
        {"ARRAY", DataType::ARRAY},
    };
    if (const auto it = kNames.find(text); it != kNames.end()) {
        return it->second;
    }
    try {
        size_t parsed = 0;
        const auto numeric = std::stoi(text, &parsed);
        if (parsed == text.size()) {
            return static_cast<DataType>(numeric);
        }
    } catch (const std::invalid_argument&) {
    } catch (const std::out_of_range&) {
    }
    ThrowInfo(DataTypeInvalid,
              "unsupported inverted data type {} for parameter {}",
              text,
              key);
}

bool
ReadBool(const Config& params, std::string_view key, bool fallback) {
    if (!params.is_object() || !params.contains(key)) {
        return fallback;
    }
    const auto& encoded = params.at(key);
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
    ThrowInfo(DataTypeInvalid, "inverted parameter {} must be boolean", key);
}

bool
ReadRequiredNested(const Config& params) {
    std::optional<bool> nested;
    std::string_view first_key;
    for (const auto key : {std::string_view("nested"),
                           std::string_view("is_nested"),
                           std::string_view("is_nested_index")}) {
        if (!params.is_object() || !params.contains(key)) {
            continue;
        }
        const auto value = ReadBool(params, key, false);
        if (nested.has_value() && *nested != value) {
            ThrowInfo(DataTypeInvalid,
                      "inverted nested parameters {} and {} disagree",
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
                  "inverted loader requires an explicit normalized nested "
                  "parameter");
    }
    return *nested;
}

struct RuntimeParams {
    DataType field_type{DataType::NONE};
    DataType value_type{DataType::NONE};
    bool nested{false};
};

RuntimeParams
ParseRuntimeParams(const Config& params) {
    RuntimeParams result;
    result.field_type = ParseDataType(params, "field_type", DataType::NONE);
    const auto array_element_type =
        ParseDataType(params, "array_element_type", DataType::NONE);
    const auto legacy_element_type =
        ParseDataType(params, "element_type", DataType::NONE);
    if (array_element_type != DataType::NONE &&
        legacy_element_type != DataType::NONE &&
        !CompatibleType(array_element_type, legacy_element_type)) {
        ThrowInfo(DataTypeInvalid,
                  "inverted array_element_type {} conflicts with element_type "
                  "{}",
                  static_cast<int>(array_element_type),
                  static_cast<int>(legacy_element_type));
    }
    const auto element_type = array_element_type != DataType::NONE
                                  ? array_element_type
                                  : legacy_element_type;
    const auto configured = ParseDataType(params, "value_type", DataType::NONE);
    result.nested = ReadRequiredNested(params);

    if (result.field_type == DataType::ARRAY) {
        if (configured != DataType::NONE && configured != DataType::ARRAY &&
            element_type != DataType::NONE &&
            !CompatibleType(configured, element_type)) {
            ThrowInfo(DataTypeInvalid,
                      "inverted ARRAY value_type {} conflicts with element "
                      "type {}",
                      static_cast<int>(configured),
                      static_cast<int>(element_type));
        }
        result.value_type =
            element_type != DataType::NONE
                ? element_type
                : (configured != DataType::ARRAY ? configured : DataType::NONE);
    } else if (result.field_type == DataType::JSON) {
        if (result.nested) {
            ThrowInfo(DataTypeInvalid,
                      "nested inverted input requires ARRAY field_type");
        }
        result.value_type = configured;
    } else {
        if (result.nested) {
            ThrowInfo(DataTypeInvalid,
                      "nested inverted input requires ARRAY field_type");
        }
        if (element_type != DataType::NONE) {
            ThrowInfo(DataTypeInvalid,
                      "scalar inverted field conflicts with element type {}",
                      static_cast<int>(element_type));
        }
        result.value_type =
            configured != DataType::NONE ? configured : result.field_type;
        if (result.field_type != DataType::NONE &&
            !CompatibleType(result.field_type, result.value_type)) {
            ThrowInfo(DataTypeInvalid,
                      "inverted field type {} conflicts with value type {}",
                      static_cast<int>(result.field_type),
                      static_cast<int>(result.value_type));
        }
    }
    if (!IsSupportedType(result.value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "unsupported inverted value type {}",
                  static_cast<int>(result.value_type));
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
        try {
            result.directory_parent =
                opts.params.at(MMAP_FILE_PATH).get<std::string>();
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "inverted mmap path must be a string: {}",
                      error.what());
        }
    }
    AssertInfo(result.directory_parent.find('\0') == std::string::npos,
               "inverted load options contain an invalid staging path");
    return result;
}

template <typename T>
T
GetRequiredMeta(storage::FileSource& source, std::string_view key) {
    const auto value = source.GetMeta(key);
    if (!value.has_value()) {
        ThrowInfo(DataFormatBroken, "inverted V3 metadata {} is missing", key);
    }
    try {
        return value->get<T>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataFormatBroken,
                  "invalid inverted V3 metadata {}: {}",
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
            DataFormatBroken, "invalid inverted index entry name {}", name);
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
        std::set<std::string> unique;
        for (const auto& name : result.engine_files) {
            ValidateEntryName(name);
            if (name == kNullOffsetsEntry || name == INDEX_TYPE ||
                name == INDEX_NON_EXIST_OFFSET_FILE_NAME ||
                !unique.insert(name).second) {
                ThrowInfo(DataFormatBroken,
                          "invalid or duplicate inverted engine entry {}",
                          name);
            }
            if (!source.HasEntry(name)) {
                ThrowInfo(DataFormatBroken,
                          "inverted engine entry {} is missing",
                          name);
            }
        }
        if (source.HasEntry(kNullOffsetsEntry) != result.has_null) {
            ThrowInfo(DataFormatBroken,
                      "inverted V3 has_null metadata disagrees with sidecar");
        }
    } else {
        result.has_null = source.HasEntry(kNullOffsetsEntry);
        for (const auto& name : source.EntryNames()) {
            ValidateEntryName(name);
            if (name != kNullOffsetsEntry && name != INDEX_TYPE &&
                name != INDEX_NON_EXIST_OFFSET_FILE_NAME &&
                name != INDEX_FILE_SLICE_META) {
                result.engine_files.push_back(name);
            }
        }
    }
    if (result.engine_files.empty()) {
        ThrowInfo(DataFormatBroken, "inverted artifact has no engine files");
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
                      "failed to remove inverted staging file {}: {}",
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
                      "failed to close inverted staging file {}: {}",
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
                  "failed to determine inverted entry size for {}: {}",
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
                      "failed to read inverted staging file {}: {}",
                      path,
                      std::strerror(errno));
        }
        if (read_size == 0) {
            ThrowInfo(FileReadFailed,
                      "unexpected EOF while reading inverted staging file {}",
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
                size_t count,
                bool nested) {
    if (!has_null) {
        return std::make_shared<const std::vector<size_t>>();
    }

    std::optional<size_t> expected_bytes;
    if (source.Gen() == storage::Generation::V3) {
        const auto declared = source.EntrySize(kNullOffsetsEntry);
        if (declared < 0 || static_cast<uint64_t>(declared) >
                                std::numeric_limits<size_t>::max()) {
            ThrowInfo(DataFormatBroken,
                      "invalid inverted null-offset byte size {}",
                      declared);
        }
        expected_bytes = static_cast<size_t>(declared);
    }

    // The sidecar uses a separate owned child so it can never be enumerated
    // as a Tantivy file by the publishable engine directory.
    auto staging = InvertedIndexDirectory::Create(staging_parent);
    auto path =
        (std::filesystem::path(staging->Path()) / kNullOffsetsEntry).string();
    LocalEntryGuard local(std::move(path));
    source.ReadEntryToLocalFile(kNullOffsetsEntry, local.Path());
    const auto bytes = FileSize(local.Path());
    if (expected_bytes.has_value() && bytes != *expected_bytes) {
        ThrowInfo(DataFormatBroken,
                  "inverted null-offset size changed from {} to {}",
                  *expected_bytes,
                  bytes);
    }
    if (bytes == 0 || bytes % sizeof(size_t) != 0) {
        ThrowInfo(DataFormatBroken,
                  "invalid inverted null-offset byte size {}",
                  bytes);
    }
    const auto offset_count = bytes / sizeof(size_t);
    if (!nested && offset_count > count) {
        ThrowInfo(DataFormatBroken,
                  "inverted null-offset count {} exceeds row count {}",
                  offset_count,
                  count);
    }

    std::vector<size_t> result(offset_count);
    const auto fd = ::open(local.Path().c_str(), O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        ThrowInfo(FileOpenFailed,
                  "failed to open inverted staging file {}: {}",
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
MaterializedBytes(const std::vector<std::string>& paths,
                  bool ram_payload_only) {
    size_t total = 0;
    for (const auto& path : paths) {
        // Tantivy's MmapDirectory::convert_to_ram_directory copies every
        // regular file except *.lock. Keep heap payload accounting aligned
        // with that exact ownership boundary; mmap readers retain every staged
        // file and therefore do not skip locks.
        if (ram_payload_only && std::string_view(path).ends_with(".lock")) {
            continue;
        }
        std::error_code error;
        const auto bytes = std::filesystem::file_size(path, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to inspect inverted entry {}: {}",
                      path,
                      error.message());
        }
        if (bytes > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken,
                      "inverted artifact byte size overflows size_t");
        }
        total += static_cast<size_t>(bytes);
    }
    return total;
}

struct InvertedLoadState {
    // Declaration order makes the engine release before its backing directory.
    std::shared_ptr<InvertedIndexDirectory> directory;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine;
    std::shared_ptr<const std::vector<size_t>> null_offsets;
    RuntimeParams params;
    bool mmap{false};
    size_t engine_bytes{0};
};

InvertedLoadState
LoadState(storage::FileSource& source,
          const storage::LoadOptions& opts,
          RuntimeParams params) {
    const auto effective = ResolveLoadOptions(opts);
    const auto entries = ReadPersistedEntries(source);

    // Keep local owners intact until the state has acquired its own shared
    // references. On every exception the engine is released before directory.
    auto directory = InvertedIndexDirectory::Create(effective.directory_parent);
    const auto paths =
        source.ReadEntriesToLocalDir(entries.engine_files, directory->Path());
    if (paths.size() != entries.engine_files.size()) {
        ThrowInfo(DataFormatBroken,
                  "inverted source materialized {} of {} engine entries",
                  paths.size(),
                  entries.engine_files.size());
    }
    for (size_t i = 0; i < paths.size(); ++i) {
        if (std::filesystem::path(paths[i]).filename().string() !=
            entries.engine_files[i]) {
            ThrowInfo(DataFormatBroken,
                      "inverted source materialized entry {} as {}",
                      entries.engine_files[i],
                      paths[i]);
        }
    }
    const auto engine_bytes = MaterializedBytes(paths, !effective.mmap);
    if (!tantivy_index_exist(directory->Path().c_str())) {
        ThrowInfo(DataFormatBroken,
                  "materialized inverted artifact is not a Tantivy index");
    }
    auto engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
        directory->Path().c_str(), effective.mmap, SetBitsetSealed);
    auto null_offsets = ReadNullOffsets(source,
                                        entries.has_null,
                                        effective.directory_parent,
                                        static_cast<size_t>(engine->count()),
                                        params.nested);

    InvertedLoadState result;
    result.directory = directory;
    result.engine = engine;
    result.null_offsets = std::move(null_offsets);
    result.params = std::move(params);
    result.mmap = effective.mmap;
    result.engine_bytes = engine_bytes;
    return result;
}

std::unique_ptr<IndexReaderBase>
MakeReader(const InvertedLoadState& state) {
    AssertInfo(state.directory != nullptr,
               "inverted load state requires a directory owner");
    AssertInfo(state.engine != nullptr,
               "inverted load state requires an engine");
    AssertInfo(state.null_offsets != nullptr,
               "inverted load state requires immutable null offsets");
    const auto engine_path_bytes = state.directory->PathHeapBytes();
    const auto make = [&]<typename T>() -> std::unique_ptr<IndexReaderBase> {
        return std::make_unique<InvertedIndexReader<T>>(
            state.mmap ? state.directory : nullptr,
            state.engine,
            state.null_offsets,
            state.params.value_type,
            state.params.nested,
            state.mmap,
            state.engine_bytes,
            engine_path_bytes);
    };
    switch (state.params.value_type) {
        case DataType::BOOL:
            return make.template operator()<bool>();
        case DataType::INT8:
            return make.template operator()<int8_t>();
        case DataType::INT16:
            return make.template operator()<int16_t>();
        case DataType::INT32:
            return make.template operator()<int32_t>();
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return make.template operator()<int64_t>();
        case DataType::FLOAT:
            return make.template operator()<float>();
        case DataType::DOUBLE:
            return make.template operator()<double>();
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
            return make.template operator()<std::string_view>();
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported inverted value type {}",
                      static_cast<int>(state.params.value_type));
    }
}

}  // namespace

std::string
InvertedIndexLoader::Family() const {
    return families::kInverted;
}

ReaderCaps
InvertedIndexLoader::DeriveCaps(const Config& index_meta) const {
    const auto params = ParseRuntimeParams(index_meta);
    return DeriveJsonProjectedCaps(
        families::kInverted,
        index_meta,
        ReaderCaps{
            .predicate = true,
            .pattern_match = IsStringType(params.value_type),
            .nested = params.nested,
            .exact = !params.nested,
        });
}

std::unique_ptr<IndexReaderBase>
InvertedIndexLoader::OpenIndex(storage::FileSource& source,
                               const storage::LoadOptions& opts) {
    auto projection =
        PrepareJsonProjectedOpen(families::kInverted, source, opts);
    auto state = LoadState(source, opts, ParseRuntimeParams(opts.params));
    auto reader = MakeReader(state);
    return FinishJsonProjectedOpen(
        std::move(projection), source, std::move(reader));
}

RehydratedIndex
InvertedIndexLoader::OpenForRewrite(storage::FileSource& source,
                                    const storage::LoadOptions& opts) {
    auto projection =
        PrepareJsonProjectedOpen(families::kInverted, source, opts);
    auto state = LoadState(source, opts, ParseRuntimeParams(opts.params));

    // Keep state intact until both recipients exist. If either allocation
    // throws, its engine still releases before the final directory owner.
    RehydratedIndex inner;
    inner.artifact =
        std::make_unique<InvertedIndexArtifact>(state.directory,
                                                state.null_offsets,
                                                state.params.value_type,
                                                state.params.nested);
    inner.reader = MakeReader(state);
    return FinishJsonProjectedRewrite(
        std::move(projection), source, std::move(inner));
}

namespace {

const bool kInvertedLoaderRegistered = [] {
    LoaderRegistry::Instance().Register(
        families::kInverted, std::make_shared<InvertedIndexLoader>());
    return true;
}();

}  // namespace

}  // namespace milvus::index
