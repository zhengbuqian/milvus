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

#include "index/scalar/text/TextIndexLoader.h"

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

#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/text/TextIndexArtifact.h"
#include "index/scalar/text/TextIndexReader.h"
#include "nlohmann/json.hpp"
#include "tantivy-wrapper.h"

namespace milvus::index {
namespace {

constexpr std::string_view kNullOffsetsEntry = "index_null_offset";
constexpr std::string_view kFileNamesMeta = "file_names";
constexpr std::string_view kHasNullMeta = "has_null";

bool
IsTextValueType(DataType type) {
    return type == DataType::STRING || type == DataType::VARCHAR ||
           type == DataType::TEXT;
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
                          "text data type in {} is out of range",
                          key);
            }
            return static_cast<DataType>(static_cast<int32_t>(value));
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid text data type in {}: {}",
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
                          "text data type in {} is out of range",
                          key);
            }
            return static_cast<DataType>(static_cast<int32_t>(value));
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid text data type in {}: {}",
                      key,
                      error.what());
        }
    }
    if (!encoded.is_string()) {
        ThrowInfo(
            DataTypeInvalid, "text parameter {} must be a data type", key);
    }
    const auto name = encoded.get<std::string>();
    static const std::map<std::string, DataType> kNames = {
        {"STRING", DataType::STRING},
        {"VARCHAR", DataType::VARCHAR},
        {"TEXT", DataType::TEXT},
    };
    if (const auto it = kNames.find(name); it != kNames.end()) {
        return it->second;
    }
    try {
        size_t parsed = 0;
        const auto numeric = std::stoi(name, &parsed);
        if (parsed == name.size()) {
            return static_cast<DataType>(numeric);
        }
    } catch (const std::invalid_argument&) {
    } catch (const std::out_of_range&) {
    }
    ThrowInfo(DataTypeInvalid,
              "unsupported text data type {} for parameter {}",
              name,
              key);
}

bool
ParseBoolValue(const nlohmann::json& encoded, std::string_view key) {
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
    ThrowInfo(DataTypeInvalid, "text parameter {} must be boolean", key);
}

bool
ReadRequiredRowDomain(const Config& params) {
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
                      "text nested parameters {} and {} disagree",
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
                  "text loader requires an explicit normalized nested "
                  "parameter");
    }
    if (*nested) {
        ThrowInfo(DataTypeInvalid,
                  "text indexes support only the row coordinate domain");
    }
    return false;
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
                  "invalid text parameter {}: {}",
                  key,
                  error.what());
    }
}

struct RuntimeParams {
    DataType value_type{DataType::NONE};
    std::string analyzer_name;
    std::string analyzer_params;
    std::string analyzer_extra_info;
};

RuntimeParams
ParseRuntimeParams(const Config& params) {
    if (!params.is_object()) {
        ThrowInfo(DataTypeInvalid, "text load parameters must be an object");
    }
    static_cast<void>(ReadRequiredRowDomain(params));
    const auto field_type = ParseDataType(params, "field_type", DataType::NONE);
    const auto value_type = ParseDataType(params, "value_type", DataType::NONE);
    if (field_type != DataType::NONE && value_type != DataType::NONE &&
        field_type != value_type &&
        !(IsTextValueType(field_type) && IsTextValueType(value_type))) {
        ThrowInfo(DataTypeInvalid,
                  "text field_type {} disagrees with value_type {}",
                  static_cast<int>(field_type),
                  static_cast<int>(value_type));
    }
    RuntimeParams result;
    result.value_type = field_type != DataType::NONE ? field_type : value_type;
    if (!IsTextValueType(result.value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "text loader requires STRING, VARCHAR, or TEXT value_type");
    }
    result.analyzer_name =
        ParseString(params, "analyzer_name", "milvus_tokenizer");
    result.analyzer_params = ParseString(params, "analyzer_params", "{}");
    result.analyzer_extra_info = ParseString(params, "analyzer_extra_info");
    if (result.analyzer_name.empty()) {
        ThrowInfo(DataTypeInvalid, "text loader requires an analyzer name");
    }
    return result;
}

struct EffectiveLoadOptions {
    bool file_backed{false};
    std::string directory_parent;
};

EffectiveLoadOptions
ResolveLoadOptions(const storage::LoadOptions& opts) {
    EffectiveLoadOptions result;
    result.file_backed = opts.enable_mmap;
    if (opts.params.is_object() && opts.params.contains(ENABLE_MMAP)) {
        result.file_backed =
            result.file_backed ||
            ParseBoolValue(opts.params.at(ENABLE_MMAP), ENABLE_MMAP);
    }
    result.directory_parent = opts.mmap_dir_path;
    if (result.directory_parent.empty() && opts.params.is_object() &&
        opts.params.contains(MMAP_FILE_PATH)) {
        result.directory_parent = ParseString(opts.params, MMAP_FILE_PATH);
    }
    AssertInfo(result.directory_parent.find('\0') == std::string::npos,
               "text load options contain an invalid staging path");
    return result;
}

template <typename T>
T
GetRequiredMeta(storage::FileSource& source, std::string_view key) {
    const auto value = source.GetMeta(key);
    if (!value.has_value()) {
        ThrowInfo(DataFormatBroken, "text V3 metadata {} is missing", key);
    }
    try {
        return value->get<T>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataFormatBroken,
                  "invalid text V3 metadata {}: {}",
                  key,
                  error.what());
    }
}

void
ValidateEntryName(std::string_view name) {
    const std::filesystem::path path(name);
    if (name.empty() || name.find('\0') != std::string_view::npos ||
        path.filename().string() != name || name == "." || name == "..") {
        ThrowInfo(DataFormatBroken, "invalid text index entry name {}", name);
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
            if (name == kNullOffsetsEntry || !unique.insert(name).second) {
                ThrowInfo(DataFormatBroken,
                          "invalid or duplicate text engine entry {}",
                          name);
            }
            if (!source.HasEntry(name)) {
                ThrowInfo(
                    DataFormatBroken, "text engine entry {} is missing", name);
            }
        }
        if (source.HasEntry(kNullOffsetsEntry) != result.has_null) {
            ThrowInfo(DataFormatBroken,
                      "text V3 has_null metadata disagrees with sidecar");
        }
        auto expected = unique;
        if (result.has_null) {
            expected.emplace(kNullOffsetsEntry);
        }
        std::set<std::string> actual;
        for (const auto& name : source.EntryNames()) {
            ValidateEntryName(name);
            if (!actual.insert(name).second) {
                ThrowInfo(
                    DataFormatBroken, "duplicate text artifact entry {}", name);
            }
        }
        if (actual != expected) {
            ThrowInfo(DataFormatBroken,
                      "text V3 entries disagree with file_names metadata");
        }
    } else {
        result.has_null = source.HasEntry(kNullOffsetsEntry);
        std::set<std::string> unique;
        for (const auto& name : source.EntryNames()) {
            ValidateEntryName(name);
            if (name == kNullOffsetsEntry) {
                continue;
            }
            if (!unique.insert(name).second) {
                ThrowInfo(
                    DataFormatBroken, "duplicate text engine entry {}", name);
            }
            result.engine_files.push_back(name);
        }
    }
    if (result.engine_files.empty()) {
        ThrowInfo(DataFormatBroken, "text artifact has no engine files");
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
                      "failed to remove text NULL staging file {}: {}",
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
                      "failed to close text NULL staging file {}: {}",
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
                      "failed to read text NULL staging file {}: {}",
                      path,
                      std::strerror(errno));
        }
        if (read_size == 0) {
            ThrowInfo(FileReadFailed,
                      "unexpected EOF while reading text NULL staging file {}",
                      path);
        }
        cursor += read_size;
        size -= static_cast<size_t>(read_size);
    }
}

void
ValidateNullOffsets(const std::vector<size_t>& offsets, size_t count) {
    size_t previous = 0;
    bool first = true;
    for (const auto offset : offsets) {
        if ((!first && offset <= previous) || offset >= count) {
            ThrowInfo(DataFormatBroken,
                      "invalid text null offset {} for count {}",
                      offset,
                      count);
        }
        previous = offset;
        first = false;
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

    // Keep the sidecar outside the Tantivy directory so it can never enter the
    // engine inventory enumerated by TextIndexArtifact::Serialize.
    auto directory = TextIndexDirectory::Create(staging_parent, "null");
    auto path =
        (std::filesystem::path(directory->Path()) / kNullOffsetsEntry).string();
    LocalEntryGuard local(std::move(path));
    source.ReadEntryToLocalFile(kNullOffsetsEntry, local.Path());

    std::error_code error;
    const auto observed = std::filesystem::file_size(local.Path(), error);
    if (error || observed > std::numeric_limits<size_t>::max()) {
        ThrowInfo(FileReadFailed,
                  "failed to determine text NULL sidecar size {}: {}",
                  local.Path(),
                  error.message());
    }
    const auto bytes = static_cast<size_t>(observed);
    if (bytes == 0 || bytes % sizeof(size_t) != 0) {
        ThrowInfo(
            DataFormatBroken, "invalid text null-offset byte size {}", bytes);
    }
    const auto offset_count = bytes / sizeof(size_t);
    if (offset_count > count) {
        ThrowInfo(DataFormatBroken,
                  "text null-offset count {} exceeds row count {}",
                  offset_count,
                  count);
    }

    auto result = std::make_shared<std::vector<size_t>>(offset_count);
    const auto fd = ::open(local.Path().c_str(), O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        ThrowInfo(FileOpenFailed,
                  "failed to open text NULL staging file {}: {}",
                  local.Path(),
                  std::strerror(errno));
    }
    FileDescriptorGuard descriptor(fd);
    ReadAll(descriptor.Get(), result->data(), bytes, local.Path());
    descriptor.CloseChecked(local.Path());
    ValidateNullOffsets(*result, count);
    local.RemoveChecked();
    return result;
}

size_t
RamPayloadBytes(milvus::tantivy::TantivyIndexWrapper& engine) {
    const auto bytes = engine.index_size_bytes();
    if (bytes > std::numeric_limits<size_t>::max()) {
        ThrowInfo(DataFormatBroken,
                  "RAM text index payload exceeds size_t domain");
    }
    return static_cast<size_t>(bytes);
}

struct TextLoadState {
    // Declaration order makes the engine release before the final directory
    // owner on every success and exception path.
    std::shared_ptr<TextIndexDirectory> directory;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine;
    std::shared_ptr<const std::vector<size_t>> null_offsets;
    RuntimeParams params;
    int64_t count{0};
    bool reader_file_backed{false};
    size_t payload_bytes{0};
};

TextLoadState
LoadState(storage::FileSource& source,
          const storage::LoadOptions& opts,
          RuntimeParams params) {
    const auto effective = ResolveLoadOptions(opts);
    const auto entries = ReadPersistedEntries(source);

    // Keep these local owners intact until TextLoadState has acquired all
    // shared references. The engine is declared after its backing directory
    // and therefore releases first if any later operation throws.
    auto directory =
        TextIndexDirectory::Create(effective.directory_parent, "loaded");
    const auto paths =
        source.ReadEntriesToLocalDir(entries.engine_files, directory->Path());
    if (paths.size() != entries.engine_files.size()) {
        ThrowInfo(DataFormatBroken,
                  "text source materialized {} of {} engine entries",
                  paths.size(),
                  entries.engine_files.size());
    }
    for (size_t i = 0; i < paths.size(); ++i) {
        if (std::filesystem::path(paths[i]).filename().string() !=
            entries.engine_files[i]) {
            ThrowInfo(DataFormatBroken,
                      "text source materialized entry {} as {}",
                      entries.engine_files[i],
                      paths[i]);
        }
    }
    if (!tantivy_index_exist(directory->Path().c_str())) {
        ThrowInfo(DataFormatBroken,
                  "materialized text artifact is not a Tantivy index");
    }

    auto engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
        directory->Path().c_str(), effective.file_backed, SetBitsetSealed);
    engine->set_analyzer_extra_info(params.analyzer_extra_info);
    engine->register_tokenizer(params.analyzer_name.c_str(),
                               params.analyzer_params.c_str());
    const auto count = static_cast<int64_t>(engine->count());
    auto null_offsets = ReadNullOffsets(source,
                                        entries.has_null,
                                        effective.directory_parent,
                                        static_cast<size_t>(count));
    const auto payload_bytes = effective.file_backed ? directory->ByteSize()
                                                     : RamPayloadBytes(*engine);

    TextLoadState result;
    result.directory = directory;
    result.engine = engine;
    result.null_offsets = std::move(null_offsets);
    result.params = std::move(params);
    result.count = count;
    result.reader_file_backed = effective.file_backed;
    result.payload_bytes = payload_bytes;
    return result;
}

std::unique_ptr<IndexReaderBase>
MakeReader(const TextLoadState& state) {
    AssertInfo(state.directory != nullptr,
               "text load state requires a publication directory");
    AssertInfo(state.engine != nullptr,
               "text load state requires a reader engine");
    AssertInfo(state.null_offsets != nullptr,
               "text load state requires validated null offsets");
    return std::make_unique<TextIndexReader>(
        state.reader_file_backed ? state.directory : nullptr,
        state.engine,
        state.count,
        state.params.value_type,
        state.reader_file_backed,
        state.payload_bytes);
}

}  // namespace

std::string
TextIndexLoader::Family() const {
    return families::kText;
}

ReaderCaps
TextIndexLoader::DeriveCaps(const Config& index_meta) const {
    static_cast<void>(ParseRuntimeParams(index_meta));
    return ReaderCaps{.text_match = true};
}

std::unique_ptr<IndexReaderBase>
TextIndexLoader::OpenIndex(storage::FileSource& source,
                           const storage::LoadOptions& opts) {
    auto state = LoadState(source, opts, ParseRuntimeParams(opts.params));
    return MakeReader(state);
}

RehydratedIndex
TextIndexLoader::OpenForRewrite(storage::FileSource& source,
                                const storage::LoadOptions& opts) {
    auto state = LoadState(source, opts, ParseRuntimeParams(opts.params));

    // Keep state intact until both recipients exist. Text is unlike the other
    // Tantivy families here: its artifact shares the already configured engine
    // so a fresh Reader handle never creates a second resident wrapper.
    RehydratedIndex result;
    result.artifact =
        std::make_unique<TextIndexArtifact>(state.directory,
                                            state.engine,
                                            state.null_offsets,
                                            state.count,
                                            state.params.value_type,
                                            state.reader_file_backed,
                                            state.payload_bytes);
    result.reader = MakeReader(state);
    return result;
}

namespace {

const bool kTextLoaderRegistered = [] {
    LoaderRegistry::Instance().Register(families::kText,
                                        std::make_shared<TextIndexLoader>());
    return true;
}();

}  // namespace

}  // namespace milvus::index
