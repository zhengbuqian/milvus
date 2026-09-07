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

#include "index/vector/VectorDiskLoader.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorDiskArtifact.h"
#include "index/vector/VectorDiskBuildFileManager.h"
#include "index/vector/VectorDiskLoadFileManager.h"
#include "index/vector/VectorDiskLocalFiles.h"
#include "index/vector/VectorDiskReader.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "index/vector/VectorLoadResource.h"
#include "knowhere/binaryset.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/segcore_error_code.h"
#include "nlohmann/json.hpp"
#include "storage/DiskFileManagerImpl.h"

namespace milvus::index {
namespace {

constexpr std::string_view kEmptyEmbListOffsets = "empty_emb_list_offsets";
constexpr const char* kEnableDiskMmap = "enable_disk_mmap";
constexpr uint32_t kDefaultBeamwidth = 8;
constexpr uint32_t kMinDiskAnnBeamwidth = 1;
constexpr uint32_t kMaxDiskAnnBeamwidth = 128;

enum class ArtifactState {
    Normal,
    AllNull,
    EmptyEmbeddingList,
};

std::string
Upper(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](char ch) {
        return static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    });
    return value;
}

int64_t
ParseInt64(const nlohmann::json& value, std::string_view key) {
    if (value.is_number_unsigned()) {
        const auto parsed = value.get<uint64_t>();
        if (parsed <=
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return static_cast<int64_t>(parsed);
        }
    } else if (value.is_number_integer()) {
        return value.get<int64_t>();
    } else if (value.is_string()) {
        const auto text = value.get<std::string>();
        int64_t parsed = 0;
        const auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), parsed);
        if (error == std::errc() && end == text.data() + text.size()) {
            return parsed;
        }
    }
    ThrowInfo(
        UnexpectedError, "normalized vector parameter {} is not an int64", key);
}

std::optional<int64_t>
ReadInt64(const Config& params, std::string_view key) {
    if (!params.contains(key) || params.at(key).is_null()) {
        return std::nullopt;
    }
    return ParseInt64(params.at(key), key);
}

std::optional<int64_t>
ReadAliasedInt64(const Config& params,
                 std::initializer_list<std::string_view> keys,
                 std::string_view label) {
    std::optional<int64_t> result;
    std::string_view first_key;
    for (const auto key : keys) {
        const auto value = ReadInt64(params, key);
        if (!value.has_value()) {
            continue;
        }
        if (result.has_value() && *result != *value) {
            ThrowInfo(UnexpectedError,
                      "normalized vector {} parameters {} and {} disagree",
                      label,
                      first_key,
                      key);
        }
        if (!result.has_value()) {
            result = value;
            first_key = key;
        }
    }
    return result;
}

DataType
ParseDataType(const nlohmann::json& value, std::string_view key) {
    if (value.is_string()) {
        const auto text = value.get<std::string>();
        int64_t numeric = 0;
        const auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), numeric);
        if (error == std::errc() && end == text.data() + text.size()) {
            if (numeric < std::numeric_limits<int>::min() ||
                numeric > std::numeric_limits<int>::max()) {
                ThrowInfo(UnexpectedError,
                          "normalized vector data type {} is out of range",
                          key);
            }
            return static_cast<DataType>(static_cast<int>(numeric));
        }
        static const std::map<std::string, DataType> names = {
            {"NONE", DataType::NONE},
            {"VECTOR_FLOAT", DataType::VECTOR_FLOAT},
            {"VECTOR_BINARY", DataType::VECTOR_BINARY},
            {"VECTOR_FLOAT16", DataType::VECTOR_FLOAT16},
            {"VECTOR_BFLOAT16", DataType::VECTOR_BFLOAT16},
            {"VECTOR_SPARSE_U32_F32", DataType::VECTOR_SPARSE_U32_F32},
            {"VECTOR_INT8", DataType::VECTOR_INT8},
            {"VECTOR_ARRAY", DataType::VECTOR_ARRAY},
        };
        const auto found = names.find(Upper(text));
        if (found != names.end()) {
            return found->second;
        }
    } else if (value.is_number_unsigned()) {
        const auto numeric = value.get<uint64_t>();
        if (numeric <= static_cast<uint64_t>(std::numeric_limits<int>::max())) {
            return static_cast<DataType>(static_cast<int>(numeric));
        }
    } else if (value.is_number_integer()) {
        const auto numeric = value.get<int64_t>();
        if (numeric >= std::numeric_limits<int>::min() &&
            numeric <= std::numeric_limits<int>::max()) {
            return static_cast<DataType>(static_cast<int>(numeric));
        }
    }
    ThrowInfo(UnexpectedError,
              "normalized vector parameter {} is not a supported data type",
              key);
}

std::optional<DataType>
ReadDataType(const Config& params, std::string_view key) {
    if (!params.contains(key) || params.at(key).is_null()) {
        return std::nullopt;
    }
    return ParseDataType(params.at(key), key);
}

std::optional<DataType>
ReadAliasedDataType(const Config& params,
                    std::initializer_list<std::string_view> keys,
                    std::string_view label) {
    std::optional<DataType> result;
    std::string_view first_key;
    for (const auto key : keys) {
        const auto value = ReadDataType(params, key);
        if (!value.has_value()) {
            continue;
        }
        if (result.has_value() && *result != *value) {
            ThrowInfo(UnexpectedError,
                      "normalized vector {} parameters {} and {} disagree",
                      label,
                      first_key,
                      key);
        }
        if (!result.has_value()) {
            result = value;
            first_key = key;
        }
    }
    return result;
}

bool
ParseBool(const nlohmann::json& value, std::string_view key) {
    if (value.is_boolean()) {
        return value.get<bool>();
    }
    if (value.is_number_unsigned()) {
        const auto parsed = value.get<uint64_t>();
        if (parsed <= 1) {
            return parsed != 0;
        }
    } else if (value.is_number_integer()) {
        const auto parsed = value.get<int64_t>();
        if (parsed == 0 || parsed == 1) {
            return parsed != 0;
        }
    } else if (value.is_string()) {
        const auto text = Upper(value.get<std::string>());
        if (text == "TRUE" || text == "1") {
            return true;
        }
        if (text == "FALSE" || text == "0") {
            return false;
        }
    }
    ThrowInfo(UnexpectedError,
              "normalized vector parameter {} is not a boolean",
              key);
}

std::optional<bool>
ReadBool(const Config& params, std::string_view key) {
    if (!params.contains(key) || params.at(key).is_null()) {
        return std::nullopt;
    }
    return ParseBool(params.at(key), key);
}

std::optional<bool>
ReadAliasedBool(const Config& params,
                std::initializer_list<std::string_view> keys,
                std::string_view label) {
    std::optional<bool> result;
    std::string_view first_key;
    for (const auto key : keys) {
        const auto value = ReadBool(params, key);
        if (!value.has_value()) {
            continue;
        }
        if (result.has_value() && *result != *value) {
            ThrowInfo(UnexpectedError,
                      "normalized vector {} parameters {} and {} disagree",
                      label,
                      first_key,
                      key);
        }
        if (!result.has_value()) {
            result = value;
            first_key = key;
        }
    }
    return result;
}

std::string
ReadRequiredString(const Config& params, std::string_view key) {
    if (!params.contains(key) || !params.at(key).is_string()) {
        ThrowInfo(UnexpectedError,
                  "normalized vector parameter {} must be a string",
                  key);
    }
    auto value = params.at(key).get<std::string>();
    if (value.empty()) {
        ThrowInfo(UnexpectedError,
                  "normalized vector parameter {} must not be empty",
                  key);
    }
    return value;
}

bool
IsPhysicalVectorType(DataType type) {
    switch (type) {
        case DataType::VECTOR_FLOAT:
        case DataType::VECTOR_BINARY:
        case DataType::VECTOR_FLOAT16:
        case DataType::VECTOR_BFLOAT16:
        case DataType::VECTOR_SPARSE_U32_F32:
        case DataType::VECTOR_INT8:
            return true;
        default:
            return false;
    }
}

uint32_t
ParseUint32(const nlohmann::json& value, std::string_view key) {
    const auto parsed = ParseInt64(value, key);
    if (parsed < 0 ||
        static_cast<uint64_t>(parsed) >
            static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
        ThrowInfo(UnexpectedError,
                  "normalized vector parameter {} is outside uint32",
                  key);
    }
    return static_cast<uint32_t>(parsed);
}

int32_t
ParsePositiveInt32Config(const nlohmann::json& value, std::string_view key) {
    int64_t parsed = 0;
    bool valid = false;
    if (value.is_number_unsigned()) {
        const auto number = value.get<uint64_t>();
        if (number <=
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            parsed = static_cast<int64_t>(number);
            valid = true;
        }
    } else if (value.is_number_integer()) {
        parsed = value.get<int64_t>();
        valid = true;
    } else if (value.is_string()) {
        const auto text = value.get<std::string>();
        const auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), parsed);
        valid = error == std::errc() && end == text.data() + text.size();
    }
    if (!valid || parsed <= 0 || parsed > std::numeric_limits<int32_t>::max()) {
        ThrowInfo(
            ConfigInvalid, "vector parameter {} must be a positive int32", key);
    }
    return static_cast<int32_t>(parsed);
}

struct RuntimeParams {
    DataType field_type{DataType::NONE};
    DataType physical_type{DataType::NONE};
    DataType elem_type{DataType::NONE};
    IndexType index_type;
    MetricType metric_type;
    IndexVersion version{0};
    std::optional<int64_t> expected_dim;
    std::optional<int64_t> num_rows;
    std::optional<bool> nullable;
    uint32_t beamwidth{kDefaultBeamwidth};
    std::optional<int32_t> load_threads;
    bool mmap_i2o{false};
    bool mmap_o2i{false};
    Config knowhere_config;
};

RuntimeParams
ParseRuntimeParams(const Config& params, bool parse_open_options) {
    if (!params.is_object()) {
        ThrowInfo(UnexpectedError,
                  "normalized vector load parameters must be an object");
    }

    RuntimeParams result;
    const auto field_type = ReadDataType(params, "field_type");
    if (!field_type.has_value()) {
        ThrowInfo(UnexpectedError,
                  "vector loader requires normalized field_type");
    }
    result.field_type = *field_type;
    const auto configured_value = ReadDataType(params, "value_type");
    const auto configured_element = ReadAliasedDataType(
        params, {"element_type", "array_element_type"}, "element type");

    if (result.field_type == DataType::VECTOR_ARRAY) {
        if (!configured_element.has_value() ||
            *configured_element == DataType::NONE) {
            ThrowInfo(UnexpectedError,
                      "VECTOR_ARRAY loader requires an element type");
        }
        result.elem_type = *configured_element;
        result.physical_type = configured_value.value_or(result.elem_type);
        if (result.physical_type != result.elem_type) {
            ThrowInfo(UnexpectedError,
                      "VECTOR_ARRAY value type {} conflicts with element type "
                      "{}",
                      result.physical_type,
                      result.elem_type);
        }
        if (result.elem_type == DataType::VECTOR_SPARSE_U32_F32) {
            ThrowInfo(Unsupported,
                      "sparse vectors are not supported as embedding-list "
                      "elements");
        }
    } else {
        if (!IsPhysicalVectorType(result.field_type)) {
            ThrowInfo(UnexpectedError,
                      "vector disk loader received non-vector field type {}",
                      result.field_type);
        }
        if (configured_element.has_value() &&
            *configured_element != DataType::NONE) {
            ThrowInfo(UnexpectedError,
                      "ordinary vector field has unexpected element type {}",
                      *configured_element);
        }
        result.physical_type = configured_value.value_or(result.field_type);
        if (result.physical_type != result.field_type) {
            ThrowInfo(UnexpectedError,
                      "vector field type {} conflicts with value type {}",
                      result.field_type,
                      result.physical_type);
        }
    }
    if (!IsPhysicalVectorType(result.physical_type)) {
        ThrowInfo(UnexpectedError,
                  "unsupported vector physical type {}",
                  result.physical_type);
    }

    const auto nested = ReadAliasedBool(
        params, {"nested", "is_nested", "is_nested_index"}, "nested");
    if (nested.value_or(false)) {
        ThrowInfo(Unsupported,
                  "vector indexes do not use scalar nested coordinates");
    }
    result.nullable = ReadBool(params, "nullable");
    result.index_type = ReadRequiredString(params, INDEX_TYPE);
    result.metric_type = ReadRequiredString(params, METRIC_TYPE);

    const auto version = ReadInt64(params, INDEX_ENGINE_VERSION);
    if (!version.has_value() ||
        *version < std::numeric_limits<IndexVersion>::min() ||
        *version > std::numeric_limits<IndexVersion>::max()) {
        ThrowInfo(UnexpectedError,
                  "vector loader requires a valid index_engine_version");
    }
    result.version = static_cast<IndexVersion>(*version);
    if (!VectorUsesDiskLoad(result.index_type, result.version)) {
        ThrowInfo(UnexpectedError,
                  "memory-load vector index {} was routed to vector_disk",
                  result.index_type);
    }

    result.expected_dim = ReadInt64(params, DIM_KEY);
    if (result.expected_dim.has_value() &&
        (*result.expected_dim < 0 ||
         (*result.expected_dim == 0 &&
          result.physical_type != DataType::VECTOR_SPARSE_U32_F32))) {
        ThrowInfo(UnexpectedError,
                  "normalized vector dimension {} is invalid for type {}",
                  *result.expected_dim,
                  result.physical_type);
    }
    result.num_rows =
        ReadAliasedInt64(params, {"num_rows", "index_num_rows"}, "row count");
    if (result.num_rows.has_value() && *result.num_rows < 0) {
        ThrowInfo(UnexpectedError,
                  "normalized vector row count must be non-negative");
    }

    if (parse_open_options &&
        result.index_type == knowhere::IndexEnum::INDEX_DISKANN) {
        if (params.contains(DISK_ANN_QUERY_BEAMWIDTH)) {
            result.beamwidth = ParseUint32(params.at(DISK_ANN_QUERY_BEAMWIDTH),
                                           DISK_ANN_QUERY_BEAMWIDTH);
        }
        if (result.beamwidth < kMinDiskAnnBeamwidth ||
            result.beamwidth > kMaxDiskAnnBeamwidth) {
            ThrowInfo(ConfigInvalid,
                      "DiskANN beamwidth {} is outside [{}, {}]",
                      result.beamwidth,
                      kMinDiskAnnBeamwidth,
                      kMaxDiskAnnBeamwidth);
        }
        if (!params.contains(DISK_ANN_LOAD_THREAD_NUM)) {
            ThrowInfo(ConfigInvalid,
                      "DiskANN load requires {}",
                      DISK_ANN_LOAD_THREAD_NUM);
        }
        result.load_threads = ParsePositiveInt32Config(
            params.at(DISK_ANN_LOAD_THREAD_NUM), DISK_ANN_LOAD_THREAD_NUM);
    }
    result.mmap_i2o = ReadBool(params, ENABLE_MMAP_I2O_MAP).value_or(false);
    result.mmap_o2i = ReadBool(params, ENABLE_MMAP_O2I_MAP).value_or(false);
    result.knowhere_config = params;
    return result;
}

struct EntryPlan {
    ArtifactState state{ArtifactState::Normal};
    std::vector<std::string> engine_names;
    bool has_validity{false};
    bool has_empty_offsets{false};
};

EntryPlan
PlanEntries(storage::FileSource& source, const RuntimeParams& params) {
    EntryPlan plan;
    std::unordered_set<std::string> unique;
    for (const auto& name : source.EntryNames()) {
        if (name.empty() || !unique.insert(name).second) {
            ThrowInfo(DataFormatBroken,
                      "disk vector artifact has an empty or duplicate entry "
                      "name");
        }
        if (name == VALID_DATA_KEY) {
            plan.has_validity = true;
        } else if (name == VALID_DATA_COUNT_KEY) {
            ThrowInfo(DataFormatBroken,
                      "disk vector artifact contains memory-format validity");
        } else if (name == kEmptyEmbListOffsets) {
            plan.has_empty_offsets = true;
        } else {
            plan.engine_names.push_back(name);
        }
    }

    if (plan.has_empty_offsets) {
        if (params.elem_type == DataType::NONE || !plan.engine_names.empty()) {
            ThrowInfo(DataFormatBroken,
                      "empty embedding-list artifact contains incompatible "
                      "engine entries");
        }
        plan.state = ArtifactState::EmptyEmbeddingList;
    } else if (plan.engine_names.empty()) {
        if (!plan.has_validity) {
            ThrowInfo(DataFormatBroken,
                      "disk vector artifact has no loadable state");
        }
        plan.state = ArtifactState::AllNull;
    }
    return plan;
}

VectorValidData
DecodeValidityBytes(const std::vector<uint8_t>& bytes,
                    const RuntimeParams& params,
                    const std::string& local_prefix) {
    VectorValidData valid;
    if (bytes.size() < sizeof(uint64_t)) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector disk valid_data file is too small");
    }
    uint64_t wire_count = 0;
    std::memcpy(&wire_count, bytes.data(), sizeof(wire_count));
    const auto count = FromValidDataCount(wire_count);
    const auto bitmap_size = GetValidDataBitmapSize(count);
    if (bytes.size() < sizeof(uint64_t) + bitmap_size) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector disk valid_data bitmap is truncated");
    }
    const auto valid_count =
        CountValidDataBitmap(count, bytes.data() + sizeof(uint64_t));
    OffsetMappingBuildOptions options;
    options.enable_mmap_i2o_map = params.mmap_i2o;
    options.enable_mmap_o2i_map = params.mmap_o2i;
    if (NeedOffsetMappingMmap(options, count, valid_count)) {
        options.mmap_dir_path = GetOffsetMappingMmapDir(local_prefix);
    }
    BuildValidDataFromBitmap(
        valid, count, bytes.data() + sizeof(uint64_t), options);
    return valid;
}

VectorValidData
DecodeValidity(storage::FileSource& source,
               const EntryPlan& plan,
               const RuntimeParams& params,
               const std::string& local_prefix) {
    if (!plan.has_validity) {
        return {};
    }
    return DecodeValidityBytes(
        source.ReadEntry(VALID_DATA_KEY), params, local_prefix);
}

struct EmptyEmbeddingListState {
    int64_t dim{0};
    std::vector<size_t> offsets;
};

EmptyEmbeddingListState
DecodeEmptyEmbeddingListBytes(const std::vector<uint8_t>& bytes) {
    constexpr size_t header_size = sizeof(int64_t) + sizeof(uint64_t);
    if (bytes.size() < header_size) {
        ThrowInfo(DataFormatBroken,
                  "empty embedding-list offset entry is too small");
    }
    EmptyEmbeddingListState result;
    auto* cursor = bytes.data();
    std::memcpy(&result.dim, cursor, sizeof(result.dim));
    cursor += sizeof(result.dim);
    uint64_t wire_count = 0;
    std::memcpy(&wire_count, cursor, sizeof(wire_count));
    cursor += sizeof(wire_count);
    const auto count = FromValidDataCount(wire_count);
    if (count == 0 ||
        count > (std::numeric_limits<size_t>::max() - header_size) /
                    sizeof(size_t)) {
        ThrowInfo(DataFormatBroken,
                  "empty embedding-list offset count is invalid");
    }
    const auto required = header_size + count * sizeof(size_t);
    if (bytes.size() < required) {
        ThrowInfo(DataFormatBroken,
                  "empty embedding-list offset entry is truncated");
    }
    result.offsets.resize(count);
    std::memcpy(result.offsets.data(), cursor, count * sizeof(size_t));
    if (result.dim <= 0 || result.offsets.front() != 0 ||
        result.offsets.back() != 0 ||
        !std::is_sorted(result.offsets.begin(), result.offsets.end())) {
        ThrowInfo(DataFormatBroken, "empty embedding-list state is invalid");
    }
    return result;
}

EmptyEmbeddingListState
DecodeEmptyEmbeddingList(storage::FileSource& source) {
    return DecodeEmptyEmbeddingListBytes(
        source.ReadEntry(kEmptyEmbListOffsets));
}

void
PrepareCommonLoadConfig(Config& config,
                        const RuntimeParams& params,
                        const std::string& prefix,
                        bool stream_backend,
                        bool enable_mmap) {
    config.erase(MMAP_FILE_PATH);
    config.erase(kEnableDiskMmap);
    config[ENABLE_MMAP] = enable_mmap;
    if (stream_backend &&
        params.index_type == knowhere::IndexEnum::INDEX_DISKANN) {
        config[kEnableDiskMmap] = enable_mmap;
    }
    config[DISK_ANN_PREFIX_PATH] = prefix;
    if (params.index_type != knowhere::IndexEnum::INDEX_DISKANN) {
        return;
    }
    config[DISK_ANN_PREPARE_WARM_UP] = false;
    config[DISK_ANN_PREPARE_USE_BFS_CACHE] = false;
    AssertInfo(params.load_threads.has_value(),
               "validated DiskANN load thread count is missing");
    config[DISK_ANN_THREADS_NUM] = *params.load_threads;
}

void
PrepareLoadConfig(Config& config,
                  const RuntimeParams& params,
                  const storage::FileSource& source,
                  const std::string& prefix,
                  bool stream_backend,
                  bool enable_mmap) {
    PrepareCommonLoadConfig(
        config, params, prefix, stream_backend, enable_mmap);
    config["index_files"] = source.RemotePaths();
}

void
PrepareOwnedLoadConfig(Config& config,
                       const RuntimeParams& params,
                       const std::string& prefix,
                       bool stream_backend,
                       bool enable_mmap) {
    PrepareCommonLoadConfig(
        config, params, prefix, stream_backend, enable_mmap);
    config.erase("index_files");
}

void
EnsureMmapDirectory(const std::string& path) {
    std::error_code error;
    std::filesystem::create_directories(path, error);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to create disk vector mmap directory {}: {}",
                  path,
                  error.message());
    }
}

void
ValidateDimension(const RuntimeParams& params,
                  int64_t loaded_dim,
                  bool persisted) {
    const bool valid = loaded_dim > 0 ||
                       (loaded_dim == 0 && params.physical_type ==
                                               DataType::VECTOR_SPARSE_U32_F32);
    if (!valid) {
        ThrowInfo(persisted ? DataFormatBroken : UnexpectedError,
                  "loaded disk vector dimension {} is invalid for type {}",
                  loaded_dim,
                  params.physical_type);
    }
    if (params.expected_dim.has_value() && *params.expected_dim != loaded_dim) {
        ThrowInfo(UnexpectedError,
                  "runtime vector dimension {} disagrees with loaded "
                  "dimension {}",
                  *params.expected_dim,
                  loaded_dim);
    }
}

void
ValidateShape(const RuntimeParams& params,
              const EntryPlan& plan,
              const KnowhereEngine& engine,
              const VectorValidData& valid) {
    if (params.nullable.has_value()) {
        const bool zero_rows = params.num_rows.value_or(-1) == 0;
        if ((!*params.nullable && valid.Enabled()) ||
            (*params.nullable && !valid.Enabled() && !zero_rows)) {
            ThrowInfo(UnexpectedError,
                      "runtime nullable metadata disagrees with disk vector "
                      "validity");
        }
    }
    if (plan.state == ArtifactState::AllNull &&
        (!valid.Enabled() || valid.ValidCount() != 0)) {
        ThrowInfo(DataFormatBroken,
                  "validity-only disk vector artifact contains valid rows");
    }
    if (plan.state == ArtifactState::EmptyEmbeddingList) {
        const auto& offsets = engine.EmptyEmbListOffsets();
        if (valid.Enabled()) {
            if (valid.ValidCount() < 0 ||
                static_cast<uint64_t>(valid.ValidCount()) + 1 !=
                    offsets.size()) {
                ThrowInfo(DataFormatBroken,
                          "empty embedding-list offsets disagree with valid "
                          "parent rows");
            }
        } else if (params.num_rows.has_value() &&
                   static_cast<uint64_t>(*params.num_rows) + 1 !=
                       offsets.size()) {
            ThrowInfo(UnexpectedError,
                      "runtime row count disagrees with empty embedding-list "
                      "offsets");
        }
    }
    if (plan.state == ArtifactState::Normal &&
        params.elem_type == DataType::NONE && valid.Enabled() &&
        engine.RawCount() != valid.ValidCount()) {
        ThrowInfo(DataFormatBroken,
                  "loaded disk vector count {} disagrees with valid row count "
                  "{}",
                  engine.RawCount(),
                  valid.ValidCount());
    }
    if (params.num_rows.has_value()) {
        if (valid.Enabled() && valid.TotalCount() != *params.num_rows) {
            ThrowInfo(UnexpectedError,
                      "runtime row count {} disagrees with nullable vector "
                      "row count {}",
                      *params.num_rows,
                      valid.TotalCount());
        }
        if (!valid.Enabled() && params.elem_type == DataType::NONE &&
            plan.state == ArtifactState::Normal &&
            engine.RawCount() != *params.num_rows) {
            ThrowInfo(UnexpectedError,
                      "runtime row count {} disagrees with loaded disk vector "
                      "count {}",
                      *params.num_rows,
                      engine.RawCount());
        }
    }
}

[[noreturn]] void
ThrowDeserializeError(knowhere::Status status) {
    ThrowInfo(knowhere::ToSegcoreErrorCode(status),
              "failed to deserialize disk vector index: status {} ({})",
              static_cast<int>(status),
              knowhere::Status2String(status));
}

std::string
OwnedEntryPath(const VectorDiskLocalFiles& owner, std::string_view name) {
    const auto entry = std::string(name);
    const auto path = std::filesystem::path(entry);
    if (entry.empty() || entry.find('\0') != std::string::npos ||
        entry == "." || entry == ".." || path.filename().string() != entry) {
        ThrowInfo(DataFormatBroken,
                  "disk vector artifact entry is not a safe basename: {}",
                  entry);
    }
    return (std::filesystem::path(owner.Directory()) / path).string();
}

void
WriteOwnedEntry(const std::string& path, const std::vector<uint8_t>& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if (!output.good()) {
        ThrowInfo(
            FileOpenFailed, "failed to open local disk vector entry {}", path);
    }
    constexpr size_t kWriteChunk = 1024 * 1024;
    size_t offset = 0;
    while (offset < bytes.size()) {
        const auto chunk = std::min(kWriteChunk, bytes.size() - offset);
        output.write(reinterpret_cast<const char*>(bytes.data() + offset),
                     static_cast<std::streamsize>(chunk));
        if (!output.good()) {
            ThrowInfo(FileWriteFailed,
                      "failed to write local disk vector entry {}",
                      path);
        }
        offset += chunk;
    }
    output.flush();
    if (!output.good()) {
        ThrowInfo(FileWriteFailed,
                  "failed to flush local disk vector entry {}",
                  path);
    }
    output.close();
    if (output.fail()) {
        ThrowInfo(FileWriteFailed,
                  "failed to close local disk vector entry {}",
                  path);
    }
}

std::vector<uint8_t>
MaterializeSidecar(storage::FileSource& source,
                   std::string_view name,
                   const std::shared_ptr<VectorDiskLocalFiles>& owner,
                   const std::shared_ptr<VectorDiskBuildFileManager>& manager) {
    auto bytes = source.ReadEntry(name);
    const auto path = OwnedEntryPath(*owner, name);
    WriteOwnedEntry(path, bytes);
    manager->RegisterOwnedFile(path, VectorDiskFileTransport::LegacySliced);
    return bytes;
}

void
MaterializeEngineEntries(
    storage::FileSource& source,
    const std::vector<std::string>& names,
    bool raw_unsliced,
    const std::shared_ptr<VectorDiskLocalFiles>& owner,
    const std::shared_ptr<VectorDiskBuildFileManager>& manager) {
    const auto transport = raw_unsliced ? VectorDiskFileTransport::RawUnsliced
                                        : VectorDiskFileTransport::LegacySliced;
    for (const auto& name : names) {
        const auto path = OwnedEntryPath(*owner, name);
        if (raw_unsliced) {
            source.ReadRawEntryToLocalFile(name, path);
        } else {
            source.ReadEntryToLocalFile(name, path);
        }
        manager->RegisterOwnedFile(path, transport);
    }
}

template <typename T>
std::shared_ptr<IndexReaderBase>
OpenTyped(storage::FileSource& source,
          const RuntimeParams& params,
          const EntryPlan& plan,
          const storage::LoadOptions& opts) {
    auto make_engine = [&](std::shared_ptr<milvus::FileManager> manager) {
        auto pack = knowhere::Pack(std::move(manager));
        return KnowhereEngine(params.physical_type,
                              params.elem_type,
                              params.index_type,
                              params.metric_type,
                              params.version,
                              pack,
                              true);
    };

    // Preserve the established DiskFileManager behavior for non-stream
    // backends. A stream-capable node is cheap to probe before Deserialize;
    // only that path is rebuilt with the inventory-checking manager.
    auto disk_manager =
        std::make_shared<storage::DiskFileManagerImpl>(source.Context());
    // Declared before the engine so failed construction/deserialization always
    // destroys the node before its stream manager and local generation.
    std::shared_ptr<VectorDiskLoadFileManager> stream_manager;
    auto engine = make_engine(
        std::static_pointer_cast<milvus::FileManager>(disk_manager));
    const bool stream_backend = plan.state == ArtifactState::Normal &&
                                engine.Raw().LoadIndexWithStream();
    if (stream_backend) {
        stream_manager = std::make_shared<VectorDiskLoadFileManager>(
            source.Context(), source.RemotePaths(), plan.engine_names);
        engine = make_engine(
            std::static_pointer_cast<milvus::FileManager>(stream_manager));
        AssertInfo(engine.Raw().LoadIndexWithStream(),
                   "disk vector stream capability changed while opening");
    }
    std::shared_ptr<const void> opaque_owner =
        stream_backend ? std::static_pointer_cast<const void>(stream_manager)
                       : std::static_pointer_cast<const void>(disk_manager);
    const auto prefix = stream_backend
                            ? stream_manager->LocalIndexPrefix()
                            : disk_manager->GetLocalIndexObjectPrefix();
    auto valid = DecodeValidity(source, plan, params, prefix);

    if (plan.state == ArtifactState::EmptyEmbeddingList) {
        auto empty = DecodeEmptyEmbeddingList(source);
        engine.SetDim(empty.dim);
        engine.SetEmptyEmbListOffsets(std::move(empty.offsets));
        ValidateDimension(params, engine.Dim(), true);
    } else if (plan.state == ArtifactState::AllNull) {
        if (!params.expected_dim.has_value()) {
            ThrowInfo(UnexpectedError,
                      "validity-only disk vector artifact requires runtime "
                      "dim");
        }
        engine.SetDim(*params.expected_dim);
        ValidateDimension(params, engine.Dim(), false);
    } else {
        const bool enable_mmap = opts.enable_mmap && engine.MmapSupported();
        if (!stream_backend) {
            const auto paths =
                source.ReadEntriesToLocalDir(plan.engine_names, prefix);
            if (paths.size() != plan.engine_names.size()) {
                ThrowInfo(
                    FileReadFailed,
                    "disk vector source returned an incomplete local file "
                    "set");
            }
            for (size_t i = 0; i < paths.size(); ++i) {
                if (std::filesystem::path(paths[i]).filename().string() !=
                    std::filesystem::path(plan.engine_names[i])
                        .filename()
                        .string()) {
                    ThrowInfo(DataFormatBroken,
                              "disk vector entry {} materialized as unexpected "
                              "file {}",
                              plan.engine_names[i],
                              paths[i]);
                }
            }
        } else if (enable_mmap) {
            EnsureMmapDirectory(prefix);
        }

        auto config = params.knowhere_config;
        PrepareLoadConfig(
            config, params, source, prefix, stream_backend, enable_mmap);
        const auto status =
            engine.Raw().Deserialize(knowhere::BinarySet{}, config);
        if (stream_backend) {
            stream_manager->RethrowFirstFailure();
        }
        // The knowhere deserialize API has no OpContext entrance. FileSource
        // observes its captured cancellation/priority while materializing
        // sidecars/non-stream files. Stream backends retain their storage input
        // through the opaque reader owner; neither source nor opts.op_ctx is
        // retained by the reader.
        if (status != knowhere::Status::success) {
            ThrowDeserializeError(status);
        }
        engine.SetDim(engine.Raw().Dim());
        ValidateDimension(params, engine.Dim(), true);
    }

    ValidateShape(params, plan, engine, valid);
    return std::make_shared<VectorDiskReader<T>>(std::move(engine),
                                                 std::move(valid),
                                                 params.beamwidth,
                                                 std::move(opaque_owner));
}

template <typename T>
RehydratedIndex
OpenTypedForRewrite(storage::FileSource& source,
                    const RuntimeParams& params,
                    const EntryPlan& plan,
                    const storage::LoadOptions& opts) {
    // The rewrite generation is independent of the operation-bound source.
    // Keep local owners alive in this frame until both the artifact and reader
    // have acquired immutable handles.
    auto local_files = VectorDiskLocalFiles::Create(opts.mmap_dir_path);
    auto file_manager =
        std::make_shared<VectorDiskBuildFileManager>(local_files);
    auto manager = std::static_pointer_cast<milvus::FileManager>(file_manager);
    auto pack = knowhere::Pack(std::move(manager));
    KnowhereEngine engine(params.physical_type,
                          params.elem_type,
                          params.index_type,
                          params.metric_type,
                          params.version,
                          pack,
                          true);

    // Capability probing performs no Deserialize. The same final engine is
    // populated below exactly once after its complete local generation exists.
    const bool stream_backend = plan.state == ArtifactState::Normal &&
                                engine.Raw().LoadIndexWithStream();
    MaterializeEngineEntries(
        source, plan.engine_names, stream_backend, local_files, file_manager);

    VectorValidData valid;
    if (plan.has_validity) {
        auto bytes = MaterializeSidecar(
            source, VALID_DATA_KEY, local_files, file_manager);
        valid = DecodeValidityBytes(bytes, params, file_manager->IndexPrefix());
    }

    std::optional<EmptyEmbeddingListState> empty_state;
    if (plan.has_empty_offsets) {
        auto bytes = MaterializeSidecar(
            source, kEmptyEmbListOffsets, local_files, file_manager);
        empty_state.emplace(DecodeEmptyEmbeddingListBytes(bytes));
    }

    // Freeze the exact source-derived publication inventory before the engine
    // opens it. Backend-created cache/temporary files are not artifact entries.
    auto files = file_manager->Files();
    AssertInfo(!files.empty(),
               "rewritten disk vector artifact has no completed files");

    if (plan.state == ArtifactState::EmptyEmbeddingList) {
        AssertInfo(empty_state.has_value(),
                   "planned empty embedding-list state is missing");
        engine.SetDim(empty_state->dim);
        engine.SetEmptyEmbListOffsets(
            std::make_shared<const std::vector<size_t>>(
                std::move(empty_state->offsets)));
        ValidateDimension(params, engine.Dim(), true);
    } else if (plan.state == ArtifactState::AllNull) {
        if (!params.expected_dim.has_value()) {
            ThrowInfo(UnexpectedError,
                      "validity-only disk vector artifact requires runtime "
                      "dim");
        }
        engine.SetDim(*params.expected_dim);
        ValidateDimension(params, engine.Dim(), false);
    } else {
        const bool enable_mmap = opts.enable_mmap && engine.MmapSupported();
        auto config = params.knowhere_config;
        PrepareOwnedLoadConfig(config,
                               params,
                               file_manager->IndexPrefix(),
                               stream_backend,
                               enable_mmap);
        const auto status =
            engine.Raw().Deserialize(knowhere::BinarySet{}, config);
        file_manager->RethrowFirstFailure();
        if (status != knowhere::Status::success) {
            ThrowDeserializeError(status);
        }
        engine.SetDim(engine.Raw().Dim());
        ValidateDimension(params, engine.Dim(), true);
    }

    ValidateShape(params, plan, engine, valid);

    auto artifact = std::make_unique<VectorDiskArtifact<T>>(engine,
                                                            valid,
                                                            params.beamwidth,
                                                            local_files,
                                                            file_manager,
                                                            std::move(files));
    auto loaded = artifact->OpenReader();
    auto reader = std::dynamic_pointer_cast<IndexReaderBase>(loaded);
    AssertInfo(reader != nullptr,
               "disk vector artifact opened an unexpected reader type");
    return {std::move(artifact), std::move(reader)};
}

std::shared_ptr<IndexReaderBase>
DispatchOpen(storage::FileSource& source,
             const RuntimeParams& params,
             const EntryPlan& plan,
             const storage::LoadOptions& opts) {
    switch (params.physical_type) {
        case DataType::VECTOR_FLOAT:
            return OpenTyped<float>(source, params, plan, opts);
        case DataType::VECTOR_BINARY:
            return OpenTyped<bin1>(source, params, plan, opts);
        case DataType::VECTOR_FLOAT16:
            return OpenTyped<float16>(source, params, plan, opts);
        case DataType::VECTOR_BFLOAT16:
            return OpenTyped<bfloat16>(source, params, plan, opts);
        case DataType::VECTOR_INT8:
            return OpenTyped<int8>(source, params, plan, opts);
        case DataType::VECTOR_SPARSE_U32_F32:
            return OpenTyped<sparse_u32_f32>(source, params, plan, opts);
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported vector physical type {} after validation",
                      params.physical_type);
    }
}

RehydratedIndex
DispatchRewrite(storage::FileSource& source,
                const RuntimeParams& params,
                const EntryPlan& plan,
                const storage::LoadOptions& opts) {
    switch (params.physical_type) {
        case DataType::VECTOR_FLOAT:
            return OpenTypedForRewrite<float>(source, params, plan, opts);
        case DataType::VECTOR_BINARY:
            return OpenTypedForRewrite<bin1>(source, params, plan, opts);
        case DataType::VECTOR_FLOAT16:
            return OpenTypedForRewrite<float16>(source, params, plan, opts);
        case DataType::VECTOR_BFLOAT16:
            return OpenTypedForRewrite<bfloat16>(source, params, plan, opts);
        case DataType::VECTOR_INT8:
            return OpenTypedForRewrite<int8>(source, params, plan, opts);
        case DataType::VECTOR_SPARSE_U32_F32:
            return OpenTypedForRewrite<sparse_u32_f32>(
                source, params, plan, opts);
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported vector physical type {} after validation",
                      params.physical_type);
    }
}

}  // namespace

std::string
VectorDiskLoader::Family() const {
    return families::kVectorDisk;
}

ReaderCaps
VectorDiskLoader::DeriveCaps(const Config& index_meta) const {
    (void)ParseRuntimeParams(index_meta, false);
    // ReaderCaps is scalar-shaped. Vector family/type metadata is sufficient
    // for current pre-pin selection; no speculative vector cap fields live here.
    return {};
}

std::shared_ptr<IndexReaderBase>
VectorDiskLoader::OpenIndex(storage::FileSource& source,
                            const storage::LoadOptions& opts) {
    const auto params = ParseRuntimeParams(opts.params, true);
    if (source.Gen() != storage::Generation::V1V2) {
        ThrowInfo(Unsupported,
                  "disk vector indexes have no V3 persisted format");
    }
    const auto plan = PlanEntries(source, params);
    return DispatchOpen(source, params, plan, opts);
}

RehydratedIndex
VectorDiskLoader::OpenForRewrite(storage::FileSource& source,
                                 const storage::LoadOptions& opts) {
    const auto params = ParseRuntimeParams(opts.params, true);
    if (source.Gen() != storage::Generation::V1V2) {
        ThrowInfo(Unsupported,
                  "disk vector indexes have no V3 persisted format");
    }
    if (opts.mmap_dir_path.empty() ||
        opts.mmap_dir_path.find('\0') != std::string::npos) {
        AssertInfo(false,
                   "disk vector rewrite requires a valid staging parent");
    }
    const auto plan = PlanEntries(source, params);
    return DispatchRewrite(source, params, plan, opts);
}

}  // namespace milvus::index
