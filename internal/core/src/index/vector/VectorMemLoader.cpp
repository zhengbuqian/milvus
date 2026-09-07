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

#include "index/vector/VectorMemLoader.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <initializer_list>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "index/vector/VectorLoadResource.h"
#include "index/vector/VectorMemArtifact.h"
#include "index/vector/VectorMemLocalFiles.h"
#include "index/vector/VectorMemReader.h"
#include "knowhere/binaryset.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/segcore_error_code.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

constexpr std::string_view kEmptyEmbListOffsets = "empty_emb_list_offsets";

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
    bool mmap_i2o{false};
    bool mmap_o2i{false};
    Config knowhere_config;
};

RuntimeParams
ParseRuntimeParams(const Config& params) {
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
                      "vector memory loader received non-vector field type {}",
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
                  "vector indexes do not use the scalar nested coordinate "
                  "mode");
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
    if (VectorUsesDiskLoad(result.index_type, result.version)) {
        ThrowInfo(UnexpectedError,
                  "disk-load vector index {} was routed to vector_mem",
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
    result.mmap_i2o = ReadBool(params, ENABLE_MMAP_I2O_MAP).value_or(false);
    result.mmap_o2i = ReadBool(params, ENABLE_MMAP_O2I_MAP).value_or(false);
    result.knowhere_config = params;
    return result;
}

struct EntryPlan {
    ArtifactState state{ArtifactState::Normal};
    std::vector<std::string> all_names;
    std::vector<std::string> engine_names;
    bool has_validity{false};
    bool has_emb_meta{false};
    bool has_emb_raw{false};
};

EntryPlan
PlanEntries(storage::FileSource& source, const RuntimeParams& params) {
    EntryPlan plan;
    plan.all_names = source.EntryNames();
    std::unordered_set<std::string> unique;
    unique.reserve(plan.all_names.size());
    bool has_valid_count = false;
    bool has_valid_data = false;
    bool has_empty_offsets = false;
    for (const auto& name : plan.all_names) {
        if (name.empty() || !unique.insert(name).second) {
            ThrowInfo(DataFormatBroken,
                      "vector artifact has an empty or duplicate entry name");
        }
        if (name == VALID_DATA_COUNT_KEY) {
            has_valid_count = true;
        } else if (name == VALID_DATA_KEY) {
            has_valid_data = true;
        } else if (name == kEmptyEmbListOffsets) {
            has_empty_offsets = true;
        } else if (name == knowhere::meta::EMB_LIST_META) {
            plan.has_emb_meta = true;
        } else if (name == knowhere::meta::EMB_LIST_RAW_INDEX) {
            plan.has_emb_raw = true;
        } else {
            plan.engine_names.push_back(name);
        }
    }
    if (has_valid_count != has_valid_data) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector valid_data entries are incomplete");
    }
    plan.has_validity = has_valid_count;

    if (has_empty_offsets) {
        if (params.elem_type == DataType::NONE || !plan.engine_names.empty() ||
            plan.has_emb_meta || plan.has_emb_raw) {
            ThrowInfo(DataFormatBroken,
                      "empty embedding-list artifact contains incompatible "
                      "engine entries");
        }
        plan.state = ArtifactState::EmptyEmbeddingList;
        return plan;
    }

    if (plan.engine_names.empty()) {
        if (!plan.has_validity || plan.has_emb_meta || plan.has_emb_raw) {
            ThrowInfo(DataFormatBroken,
                      "vector artifact has no loadable engine state");
        }
        plan.state = ArtifactState::AllNull;
        return plan;
    }

    if (params.elem_type == DataType::NONE) {
        if (plan.has_emb_meta || plan.has_emb_raw) {
            ThrowInfo(DataFormatBroken,
                      "ordinary vector artifact contains embedding-list "
                      "sidecars");
        }
    } else if (!plan.has_emb_meta) {
        ThrowInfo(DataFormatBroken,
                  "embedding-list vector artifact has no EMB_LIST_META entry");
    }
    return plan;
}

void
AppendReadEntry(storage::FileSource& source,
                const std::string& name,
                knowhere::BinarySet& entries) {
    auto bytes = source.ReadEntry(name);
    if (bytes.size() >
        static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        ThrowInfo(DataFormatBroken,
                  "vector artifact entry {} exceeds BinarySet size",
                  name);
    }
    const auto size = static_cast<int64_t>(bytes.size());
    auto owner = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
    std::shared_ptr<uint8_t[]> data(owner, owner->data());
    entries.Append(name, std::move(data), size);
}

knowhere::BinarySet
ReadEntries(storage::FileSource& source,
            const std::vector<std::string>& names) {
    knowhere::BinarySet entries;
    for (const auto& name : names) {
        AppendReadEntry(source, name, entries);
    }
    return entries;
}

struct EmptyEmbeddingListState {
    int64_t dim{0};
    std::vector<size_t> offsets;
};

EmptyEmbeddingListState
DecodeEmptyEmbeddingList(const knowhere::BinarySet& entries) {
    const auto entry = entries.GetByName(std::string(kEmptyEmbListOffsets));
    constexpr size_t header_size = sizeof(int64_t) + sizeof(uint64_t);
    if (entry == nullptr || entry->size < 0 ||
        static_cast<uint64_t>(entry->size) < header_size ||
        entry->data == nullptr) {
        ThrowInfo(DataFormatBroken,
                  "empty embedding-list offset entry is invalid");
    }
    const auto* cursor = entry->data.get();
    EmptyEmbeddingListState result;
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
    if (static_cast<uint64_t>(entry->size) < required) {
        ThrowInfo(DataFormatBroken,
                  "empty embedding-list offset entry is truncated");
    }
    result.offsets.resize(count);
    std::memcpy(result.offsets.data(), cursor, count * sizeof(size_t));
    if (result.offsets.front() != 0 || result.offsets.back() != 0 ||
        !std::is_sorted(result.offsets.begin(), result.offsets.end())) {
        ThrowInfo(DataFormatBroken,
                  "empty embedding-list offsets are not an all-zero prefix "
                  "sum");
    }
    if (result.dim <= 0) {
        ThrowInfo(DataFormatBroken,
                  "empty embedding-list dimension {} is invalid",
                  result.dim);
    }
    return result;
}

void
SetWarmup(Config& config, storage::WarmupPolicy warmup) {
    switch (warmup) {
        case storage::WarmupPolicy::Disable:
            config[WARMUP] = "disable";
            return;
        case storage::WarmupPolicy::Sync:
            config[WARMUP] = "sync";
            return;
        case storage::WarmupPolicy::Async:
            config[WARMUP] = "async";
            return;
    }
    ThrowInfo(UnexpectedError, "unknown vector warmup policy");
}

[[noreturn]] void
ThrowDeserializeError(knowhere::Status status) {
    ThrowInfo(knowhere::ToSegcoreErrorCode(status),
              "failed to deserialize vector index: status {} ({})",
              static_cast<int>(status),
              knowhere::Status2String(status));
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
                  "loaded vector dimension {} is invalid for type {}",
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
                      "runtime nullable metadata disagrees with vector "
                      "validity entries");
        }
    }
    if (plan.state == ArtifactState::AllNull) {
        if (!valid.Enabled() || valid.ValidCount() != 0) {
            ThrowInfo(DataFormatBroken,
                      "validity-only vector artifact contains valid rows");
        }
    }
    if (plan.state == ArtifactState::EmptyEmbeddingList) {
        const auto& offsets = engine.EmptyEmbListOffsets();
        if (valid.Enabled()) {
            if (valid.ValidCount() < 0 ||
                static_cast<uint64_t>(valid.ValidCount()) + 1 !=
                    offsets.size()) {
                ThrowInfo(DataFormatBroken,
                          "empty embedding-list offset count disagrees with "
                          "nullable valid row count");
            }
        } else if (params.num_rows.has_value() &&
                   static_cast<uint64_t>(*params.num_rows) + 1 !=
                       offsets.size()) {
            ThrowInfo(UnexpectedError,
                      "runtime vector row count disagrees with empty "
                      "embedding-list offsets");
        }
    }
    if (plan.state == ArtifactState::Normal &&
        params.elem_type == DataType::NONE && valid.Enabled() &&
        engine.RawCount() != valid.ValidCount()) {
        ThrowInfo(DataFormatBroken,
                  "loaded vector count {} disagrees with nullable valid row "
                  "count {}",
                  engine.RawCount(),
                  valid.ValidCount());
    }
    if (params.num_rows.has_value()) {
        if (valid.Enabled() && valid.TotalCount() != *params.num_rows) {
            ThrowInfo(UnexpectedError,
                      "runtime vector row count {} disagrees with nullable "
                      "row count {}",
                      *params.num_rows,
                      valid.TotalCount());
        }
        if (!valid.Enabled() && params.elem_type == DataType::NONE &&
            plan.state == ArtifactState::Normal &&
            engine.RawCount() != *params.num_rows) {
            ThrowInfo(UnexpectedError,
                      "runtime vector row count {} disagrees with loaded "
                      "count {}",
                      *params.num_rows,
                      engine.RawCount());
        }
    }
}

VectorValidData
LoadValidity(storage::FileSource& source,
             const EntryPlan& plan,
             const storage::LoadOptions& opts,
             const RuntimeParams& params,
             knowhere::BinarySet* existing = nullptr) {
    VectorValidData valid;
    if (!plan.has_validity) {
        return valid;
    }
    knowhere::BinarySet local;
    auto& entries = existing == nullptr ? local : *existing;
    if (!entries.Contains(VALID_DATA_COUNT_KEY)) {
        AppendReadEntry(source, VALID_DATA_COUNT_KEY, entries);
        AppendReadEntry(source, VALID_DATA_KEY, entries);
    }
    OffsetMappingBuildOptions options;
    options.enable_mmap_i2o_map = params.mmap_i2o;
    options.enable_mmap_o2i_map = params.mmap_o2i;
    options.mmap_dir_path = opts.mmap_dir_path;
    LoadValidDataFromBinarySet(entries, valid, options);
    return valid;
}

struct OpenedMemState {
    explicit OpenedMemState(const RuntimeParams& params)
        : engine(params.physical_type,
                 params.elem_type,
                 params.index_type,
                 params.metric_type,
                 params.version) {
    }

    // The owner precedes the engine so all knowhere mappings are destroyed
    // before the loader-created directory on every success/failure path.
    std::shared_ptr<VectorMemLocalFiles> local_files;
    KnowhereEngine engine;
    VectorValidData valid;
};

void
PopulateState(OpenedMemState& state,
              storage::FileSource& source,
              const storage::LoadOptions& opts,
              const RuntimeParams& params,
              const EntryPlan& plan) {
    auto config = params.knowhere_config;
    config.erase(MMAP_FILE_PATH);
    config.erase(EMB_LIST_META_PATH);
    config.erase(EMB_LIST_RAW_INDEX_PATH);
    SetWarmup(config, opts.warmup);

    if (plan.state == ArtifactState::EmptyEmbeddingList) {
        auto entries = ReadEntries(source, plan.all_names);
        auto empty = DecodeEmptyEmbeddingList(entries);
        state.engine.SetDim(empty.dim);
        state.engine.SetEmptyEmbListOffsets(std::move(empty.offsets));
        state.valid = LoadValidity(source, plan, opts, params, &entries);
        ValidateDimension(params, state.engine.Dim(), true);
    } else if (plan.state == ArtifactState::AllNull) {
        if (!params.expected_dim.has_value()) {
            ThrowInfo(UnexpectedError,
                      "validity-only vector artifact requires runtime dim");
        }
        state.engine.SetDim(*params.expected_dim);
        state.valid = LoadValidity(source, plan, opts, params);
        ValidateDimension(params, state.engine.Dim(), false);
    } else {
        const bool mmap = opts.enable_mmap && state.engine.MmapSupported();
        if (mmap) {
            state.local_files = VectorMemLocalFiles::Create(opts.mmap_dir_path);
            const auto& directory = state.local_files->Directory();
            const auto main_path =
                (std::filesystem::path(directory) / "index").string();
            source.ReadEntriesToLocalFile(plan.engine_names, main_path);
            config[ENABLE_MMAP] = true;
            if (params.elem_type != DataType::NONE) {
                const auto meta_path =
                    (std::filesystem::path(directory) / EMB_LIST_META_FILE_NAME)
                        .string();
                source.ReadEntryToLocalFile(knowhere::meta::EMB_LIST_META,
                                            meta_path);
                config[EMB_LIST_META_PATH] = meta_path;
                if (plan.has_emb_raw) {
                    const auto raw_path = (std::filesystem::path(directory) /
                                           EMB_LIST_RAW_INDEX_FILE_NAME)
                                              .string();
                    source.ReadEntryToLocalFile(
                        knowhere::meta::EMB_LIST_RAW_INDEX, raw_path);
                    config[EMB_LIST_RAW_INDEX_PATH] = raw_path;
                }
            }
            const auto status =
                state.engine.Raw().DeserializeFromFile(main_path, config);
            // The knowhere deserialize API has no OpContext entrance. Remote
            // reads and local materialization observe the context captured by
            // FileSource; the borrowed opts.op_ctx is never retained here.
            if (status != knowhere::Status::success) {
                ThrowDeserializeError(status);
            }
            state.valid = LoadValidity(source, plan, opts, params);
        } else {
            config[ENABLE_MMAP] = false;
            auto entries = ReadEntries(source, plan.all_names);
            const auto status = state.engine.Raw().Deserialize(entries, config);
            if (status != knowhere::Status::success) {
                ThrowDeserializeError(status);
            }
            state.valid = LoadValidity(source, plan, opts, params, &entries);
            entries.clear();
        }
        state.engine.SetDim(state.engine.Raw().Dim());
        ValidateDimension(params, state.engine.Dim(), true);
    }

    ValidateShape(params, plan, state.engine, state.valid);
}

template <typename T>
std::shared_ptr<IndexReaderBase>
MakeReader(OpenedMemState& state) {
    // Pass an lvalue owner so state continues to pin the mmap generation if
    // make_shared or reader construction throws after moving the engine.
    return std::make_shared<VectorMemReader<T>>(
        std::move(state.engine), std::move(state.valid), state.local_files);
}

template <typename T>
RehydratedIndex
MakeRewrite(OpenedMemState& state) {
    auto artifact = std::make_unique<VectorMemArtifact<T>>(
        state.engine, state.valid, state.local_files);
    auto loaded = artifact->OpenReader();
    auto reader = std::dynamic_pointer_cast<IndexReaderBase>(loaded);
    AssertInfo(reader != nullptr,
               "vector_mem artifact opened an incompatible reader");
    return {.artifact = std::move(artifact), .reader = std::move(reader)};
}

std::shared_ptr<IndexReaderBase>
DispatchReader(DataType physical_type, OpenedMemState& state) {
    switch (physical_type) {
        case DataType::VECTOR_FLOAT:
            return MakeReader<float>(state);
        case DataType::VECTOR_BINARY:
            return MakeReader<bin1>(state);
        case DataType::VECTOR_FLOAT16:
            return MakeReader<float16>(state);
        case DataType::VECTOR_BFLOAT16:
            return MakeReader<bfloat16>(state);
        case DataType::VECTOR_INT8:
            return MakeReader<int8>(state);
        case DataType::VECTOR_SPARSE_U32_F32:
            return MakeReader<sparse_u32_f32>(state);
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported vector physical type {} after validation",
                      physical_type);
    }
}

RehydratedIndex
DispatchRewrite(DataType physical_type, OpenedMemState& state) {
    switch (physical_type) {
        case DataType::VECTOR_FLOAT:
            return MakeRewrite<float>(state);
        case DataType::VECTOR_BINARY:
            return MakeRewrite<bin1>(state);
        case DataType::VECTOR_FLOAT16:
            return MakeRewrite<float16>(state);
        case DataType::VECTOR_BFLOAT16:
            return MakeRewrite<bfloat16>(state);
        case DataType::VECTOR_INT8:
            return MakeRewrite<int8>(state);
        case DataType::VECTOR_SPARSE_U32_F32:
            return MakeRewrite<sparse_u32_f32>(state);
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported vector physical type {} after validation",
                      physical_type);
    }
}

}  // namespace

std::string
VectorMemLoader::Family() const {
    return families::kVectorMem;
}

ReaderCaps
VectorMemLoader::DeriveCaps(const Config& index_meta) const {
    (void)ParseRuntimeParams(index_meta);
    // ReaderCaps is scalar-shaped. Family/type metadata identifies this as a
    // vector reader; no vector capability expansion is implied here.
    return {};
}

std::shared_ptr<IndexReaderBase>
VectorMemLoader::OpenIndex(storage::FileSource& source,
                           const storage::LoadOptions& opts) {
    const auto params = ParseRuntimeParams(opts.params);
    if (source.Gen() != storage::Generation::V1V2) {
        ThrowInfo(Unsupported,
                  "in-memory vector indexes have no V3 persisted format");
    }
    const auto plan = PlanEntries(source, params);
    if (plan.has_validity && (params.mmap_i2o || params.mmap_o2i) &&
        opts.mmap_dir_path.empty()) {
        ThrowInfo(UnexpectedError,
                  "nullable vector mmap mapping requires a staging parent");
    }
    OpenedMemState state(params);
    PopulateState(state, source, opts, params, plan);
    return DispatchReader(params.physical_type, state);
}

RehydratedIndex
VectorMemLoader::OpenForRewrite(storage::FileSource& source,
                                const storage::LoadOptions& opts) {
    const auto params = ParseRuntimeParams(opts.params);
    if (source.Gen() != storage::Generation::V1V2) {
        ThrowInfo(Unsupported,
                  "in-memory vector indexes have no V3 persisted format");
    }
    const auto plan = PlanEntries(source, params);
    if (plan.has_validity && (params.mmap_i2o || params.mmap_o2i) &&
        opts.mmap_dir_path.empty()) {
        ThrowInfo(UnexpectedError,
                  "nullable vector mmap mapping requires a staging parent");
    }
    OpenedMemState state(params);
    PopulateState(state, source, opts, params, plan);
    return DispatchRewrite(params.physical_type, state);
}

}  // namespace milvus::index
