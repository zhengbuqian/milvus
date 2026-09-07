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

#include "index/scalar/text/TextIndexBuilder.h"

#include <limits>
#include <map>
#include <optional>
#include <string>
#include <utility>

#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/text/TextIndexArtifact.h"
#include "nlohmann/json.hpp"
#include "tantivy-wrapper.h"

namespace milvus::index {
namespace {

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
ParseBool(const Config& params, std::string_view key) {
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
    ThrowInfo(DataTypeInvalid, "text parameter {} must be boolean", key);
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
        const auto value = ParseBool(params, key);
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
                  "text builder requires an explicit normalized nested "
                  "parameter");
    }
    if (*nested) {
        ThrowInfo(DataTypeInvalid,
                  "text indexes support only the row coordinate domain");
    }
}

int64_t
ParseIntegral(const Config& params,
              std::string_view key,
              int64_t fallback,
              bool required) {
    if (!params.is_object() || !params.contains(key)) {
        if (required) {
            ThrowInfo(
                DataTypeInvalid, "text builder requires parameter {}", key);
        }
        return fallback;
    }
    const auto& encoded = params.at(key);
    if (encoded.is_number_unsigned()) {
        try {
            const auto value = encoded.get<uint64_t>();
            if (value <=
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                return static_cast<int64_t>(value);
            }
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid text parameter {}: {}",
                      key,
                      error.what());
        }
        ThrowInfo(DataTypeInvalid, "text parameter {} is out of range", key);
    }
    if (encoded.is_number_integer()) {
        try {
            return encoded.get<int64_t>();
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid text parameter {}: {}",
                      key,
                      error.what());
        }
    }
    if (encoded.is_string()) {
        try {
            const auto text = encoded.get<std::string>();
            size_t parsed = 0;
            const auto value = std::stoll(text, &parsed);
            if (parsed == text.size()) {
                return value;
            }
        } catch (const std::invalid_argument&) {
        } catch (const std::out_of_range&) {
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid text parameter {}: {}",
                      key,
                      error.what());
        }
    }
    ThrowInfo(DataTypeInvalid, "text parameter {} must be an integer", key);
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

TextIndexBuildParams
ParseBuildParams(const Config& params) {
    if (!params.is_object()) {
        ThrowInfo(DataTypeInvalid, "text builder parameters must be an object");
    }
    ValidateRowDomain(params);

    TextIndexBuildParams result;
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
    result.value_type = field_type != DataType::NONE ? field_type : value_type;
    if (!IsTextValueType(result.value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "text builder requires STRING, VARCHAR, or TEXT value_type");
    }

    result.unique_id = ParseString(params, "unique_id");
    result.field_name = ParseString(params, "field_name");
    if (result.field_name.empty() && params.contains(FIELD_ID)) {
        const auto field_id = ParseIntegral(params, FIELD_ID, 0, true);
        if (field_id < 0) {
            ThrowInfo(DataTypeInvalid, "text field_id must be non-negative");
        }
        result.field_name = std::to_string(field_id);
    }
    if (result.field_name.empty()) {
        // The sealed RAM/in-place path historically uses unique_id as the
        // Tantivy schema field name and legitimately has no FIELD_ID param.
        result.field_name = result.unique_id;
    }
    if (result.unique_id.empty()) {
        result.unique_id = result.field_name;
    }

    result.analyzer_name =
        ParseString(params, "analyzer_name", "milvus_tokenizer");
    result.analyzer_params = ParseString(params, "analyzer_params", "{}");
    result.analyzer_extra_info = ParseString(params, "analyzer_extra_info");
    result.local_dir = ParseString(params, "local_dir");

    const auto scalar_version =
        ParseIntegral(params, SCALAR_INDEX_ENGINE_VERSION, 2, false);
    const auto configured_tantivy =
        ParseIntegral(params, TANTIVY_INDEX_VERSION, 0, false);
    if (scalar_version < 0 || configured_tantivy < 0 ||
        configured_tantivy > std::numeric_limits<uint32_t>::max()) {
        ThrowInfo(DataTypeInvalid,
                  "invalid text engine versions scalar={} tantivy={}",
                  scalar_version,
                  configured_tantivy);
    }
    result.tantivy_index_version =
        configured_tantivy != 0 ? static_cast<uint32_t>(configured_tantivy)
        : scalar_version <= 1   ? TANTIVY_INDEX_MINIMUM_VERSION
                                : TANTIVY_INDEX_LATEST_VERSION;
    return result;
}

void
NormalizeAndValidate(TextIndexBuildParams& params) {
    if (params.field_name.empty()) {
        params.field_name = params.unique_id;
    }
    if (params.unique_id.empty()) {
        params.unique_id = params.field_name;
    }
    if (params.tantivy_index_version == 0) {
        params.tantivy_index_version = TANTIVY_INDEX_LATEST_VERSION;
    }
    if (params.field_name.empty()) {
        ThrowInfo(DataTypeInvalid,
                  "text builder requires a Tantivy field name or unique_id");
    }
    if (params.unique_id.empty()) {
        ThrowInfo(DataTypeInvalid, "text builder requires a unique_id");
    }
    if (params.analyzer_name.empty()) {
        ThrowInfo(DataTypeInvalid, "text builder requires an analyzer name");
    }
    if (!IsTextValueType(params.value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "text builder requires STRING, VARCHAR, or TEXT value type");
    }
    if (params.tantivy_index_version != TANTIVY_INDEX_MINIMUM_VERSION &&
        params.tantivy_index_version != TANTIVY_INDEX_LATEST_VERSION) {
        ThrowInfo(DataTypeInvalid,
                  "unsupported Tantivy text index version {}",
                  params.tantivy_index_version);
    }
    // The existing wrapper can only create a reader directly from a V7 writer.
    // Baseline sealed RAM builds always select latest/V7; V5 is a disk-only
    // compatibility build and is reopened from its files after finish.
    if (params.local_dir.empty() &&
        params.tantivy_index_version != TANTIVY_INDEX_LATEST_VERSION) {
        ThrowInfo(DataTypeInvalid,
                  "Tantivy version 5 sealed RAM text readers are unsupported");
    }
}

void
CheckAppend(size_t current, size_t added) {
    constexpr size_t kMaxCount = std::numeric_limits<uint32_t>::max();
    if (added > std::numeric_limits<size_t>::max() - current ||
        current > kMaxCount || added > kMaxCount - current) {
        ThrowInfo(DataTypeInvalid,
                  "text document count {} + {} exceeds uint32 count domain",
                  current,
                  added);
    }
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

}  // namespace

TextIndexBuilder::TextIndexBuilder(TextIndexBuildParams params)
    : params_(std::move(params)) {
    NormalizeAndValidate(params_);
    if (!params_.local_dir.empty()) {
        directory_ =
            TextIndexDirectory::Create(params_.local_dir, params_.unique_id);
    }
    const auto path =
        directory_ == nullptr ? std::string{} : directory_->Path();
    engine_ = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
        params_.field_name.c_str(),
        directory_ == nullptr,
        path.c_str(),
        params_.tantivy_index_version,
        params_.analyzer_name.c_str(),
        params_.analyzer_params.c_str(),
        params_.analyzer_extra_info.c_str(),
        milvus::tantivy::DEFAULT_NUM_THREADS,
        milvus::tantivy::DEFAULT_OVERALL_MEMORY_BUDGET_IN_BYTES,
        false);
}

TextIndexBuilder::~TextIndexBuilder() = default;

BuilderInputSpec
TextIndexBuilder::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Streaming,
                            .needs_second_pass = false};
}

void
TextIndexBuilder::Add(size_t n,
                      const std::string_view* values,
                      const bool* valid) {
    AssertInfo(!sealed_, "text builder cannot Add after Seal");
    AssertInfo(!failed_, "text builder cannot Add after a failed append");
    AssertInfo(engine_ != nullptr, "text builder has no writer");
    AssertInfo(n == 0 || values != nullptr,
               "text builder received null values with non-zero count");
    CheckAppend(count_, n);

    if (valid != nullptr) {
        size_t null_count = 0;
        for (size_t i = 0; i < n; ++i) {
            null_count += valid[i] ? 0 : 1;
        }
        if (null_count >
            std::numeric_limits<size_t>::max() - null_offsets_.size()) {
            ThrowInfo(DataTypeInvalid, "text null-offset count overflows");
        }
        null_offsets_.reserve(null_offsets_.size() + null_count);
    }

    static const std::string empty;
    std::string scratch;
    try {
        for (size_t i = 0; i < n; ++i) {
            const auto offset = count_;
            if (valid != nullptr && !valid[i]) {
                // Empty-array documents preserve every row coordinate,
                // including trailing and all-null input, exactly as baseline
                // AddNullSealed.
                engine_->add_array_data(
                    &empty, 0, static_cast<int64_t>(offset));
                null_offsets_.push_back(offset);
            } else {
                if (values[i].empty()) {
                    scratch.clear();
                } else {
                    scratch.assign(values[i].data(), values[i].size());
                }
                engine_->add_data(&scratch, 1, static_cast<int64_t>(offset));
            }
            ++count_;
        }
    } catch (...) {
        failed_ = true;
        throw;
    }
}

storage::ArtifactPtr
TextIndexBuilder::Seal() && {
    AssertInfo(!sealed_, "text builder cannot Seal more than once");
    AssertInfo(!failed_, "text builder cannot Seal after a failed append");
    sealed_ = true;
    auto params = std::move(params_);
    auto directory = std::move(directory_);
    auto engine = std::exchange(engine_, nullptr);
    auto null_offsets = std::move(null_offsets_);
    const auto expected_count = std::exchange(count_, 0);
    AssertInfo(engine != nullptr, "text builder has no writer to seal");

    if (params.tantivy_index_version == TANTIVY_INDEX_LATEST_VERSION) {
        // V7 readers own an Arc<Index>. Create one before finish consumes the
        // writer, then expose the final commit/merge through reload below. For
        // a disk writer, the cloned Index keeps its mmap directory backing.
        engine->create_reader(SetBitsetSealed);
    }
    engine->finish();
    if (params.tantivy_index_version == TANTIVY_INDEX_MINIMUM_VERSION) {
        // V5 cannot create a reader from a writer. RAM was rejected at the
        // parameter boundary, so reopen its finished disk files explicitly as
        // file-backed mappings.
        AssertInfo(directory != nullptr,
                   "Tantivy V5 text build requires a disk directory");
        engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
            directory->Path().c_str(), true, SetBitsetSealed);
    }
    engine->reload();
    engine->set_analyzer_extra_info(params.analyzer_extra_info);
    engine->register_tokenizer(params.analyzer_name.c_str(),
                               params.analyzer_params.c_str());

    const auto count = static_cast<size_t>(engine->count());
    AssertInfo(count == expected_count,
               "text builder fed {} rows but Tantivy exposes {}",
               expected_count,
               count);
    const bool file_backed = directory != nullptr;
    const auto payload_bytes =
        file_backed ? directory->ByteSize() : RamPayloadBytes(*engine);
    return std::make_unique<TextIndexArtifact>(std::move(directory),
                                               std::move(engine),
                                               std::move(null_offsets),
                                               static_cast<int64_t>(count),
                                               params.value_type,
                                               file_backed,
                                               payload_bytes);
}

namespace {

// Self-registration (§11.2 rule 4). See index/Registry.cpp for the
// static-registration hazard note.
const bool kTextBuilderRegistered = [] {
    BuilderRegistry<std::string_view>::Instance().Register(
        families::kText, [](const BuildParams& params) {
            return std::make_unique<TextIndexBuilder>(ParseBuildParams(params));
        });
    return true;
}();

}  // namespace

}  // namespace milvus::index
