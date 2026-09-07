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

#include "index/scalar/ngram/NgramIndexBuilder.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <limits>
#include <map>
#include <optional>
#include <utility>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/scalar/ngram/NgramIndexArtifact.h"
#include "tantivy-wrapper.h"

namespace milvus::index {
namespace {

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
ParseBool(const Config& params, std::string_view key, bool fallback) {
    if (!params.is_object() || !params.contains(key)) {
        return fallback;
    }
    const auto& encoded = params.at(key);
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
        const auto value = ParseBool(params, key, false);
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

void
ValidateExplicitEngineVersion(const Config& params) {
    const auto version = ParseUnsigned(params, TANTIVY_INDEX_VERSION, 0, false);
    // The ngram writer binding has always created Tantivy v7 directly and has
    // no v5 constructor. Reject an explicit different request rather than
    // silently producing a newer engine format.
    if (version != 0 && version != TANTIVY_INDEX_LATEST_VERSION) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM supports only Tantivy index version {}",
                  TANTIVY_INDEX_LATEST_VERSION);
    }
}

NgramBuildParams
ParseBuildParams(const Config& params, bool json_projection) {
    if (!params.is_object()) {
        ThrowInfo(DataTypeInvalid, "NGRAM build parameters must be an object");
    }
    ValidateRowDomain(params);
    ValidateExplicitEngineVersion(params);

    NgramBuildParams result;
    const auto field_id = ParseUnsigned(params, FIELD_ID, 0, true);
    if (field_id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        ThrowInfo(DataTypeInvalid, "NGRAM field_id is out of range");
    }
    result.field_name = std::to_string(field_id);
    const auto field_type =
        ParseDataType(params, "field_type", DataType::VARCHAR);
    result.value_type = ParseDataType(
        params,
        "value_type",
        field_type == DataType::JSON ? DataType::VARCHAR : field_type);
    if (!IsStringType(result.value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM value_type must be STRING, VARCHAR, or TEXT");
    }
    const auto nested_path = ParseJsonPath(params);
    if (field_type == DataType::JSON) {
        if (nested_path.empty()) {
            ThrowInfo(DataTypeInvalid, "JSON NGRAM requires json_path");
        }
        const auto cast = Upper(ParseString(params, JSON_CAST_TYPE));
        if (cast != "VARCHAR") {
            ThrowInfo(DataTypeInvalid,
                      "JSON NGRAM requires VARCHAR json_cast_type");
        }
        if (!json_projection) {
            ThrowInfo(UnexpectedError,
                      "JSON NGRAM requires the tri-state projection builder");
        }
    } else if (json_projection) {
        ThrowInfo(DataTypeInvalid,
                  "tri-state NGRAM projection requires a JSON field");
    } else if (!IsStringType(field_type)) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM field_type must be STRING, VARCHAR, TEXT, or JSON");
    } else if (!nested_path.empty()) {
        ThrowInfo(DataTypeInvalid, "scalar NGRAM must not carry a JSON path");
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
    result.local_dir = ParseString(params, "local_dir");
    return result;
}

void
CheckAppend(size_t current, size_t count) {
    constexpr size_t kMaxDocs = std::numeric_limits<uint32_t>::max();
    if (count > std::numeric_limits<size_t>::max() - current ||
        current > kMaxDocs || count > kMaxDocs - current) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM document count {} + {} exceeds uint32 domain",
                  current,
                  count);
    }
}

void
AssignString(std::string& target, std::string_view source) {
    if (source.empty()) {
        target.clear();
    } else {
        target.assign(source.data(), source.size());
    }
}

NgramBuildParams
ValidateBuildParams(NgramBuildParams params) {
    AssertInfo(!params.field_name.empty(),
               "NGRAM builder requires a field identity");
    AssertInfo(IsStringType(params.value_type),
               "NGRAM builder requires a string value type");
    AssertInfo(params.min_gram > 0 && params.min_gram <= params.max_gram,
               "NGRAM builder has invalid gram range {}..{}",
               params.min_gram,
               params.max_gram);
    return params;
}

}  // namespace

class NgramBuilderCore {
 public:
    explicit NgramBuilderCore(NgramBuildParams params)
        : params_(ValidateBuildParams(std::move(params))),
          directory_(NgramIndexDirectory::Create(params_.local_dir)),
          engine_(std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
              params_.field_name.c_str(),
              directory_->Path().c_str(),
              params_.min_gram,
              params_.max_gram)) {
    }

    BuilderInputSpec
    InputSpec() const {
        return BuilderInputSpec{.form = BuilderInputSpec::Streaming,
                                .needs_second_pass = false};
    }

    void
    Add(size_t n, const std::string_view* values, const bool* valid) {
        try {
            AssertInfo(
                n == 0 || values != nullptr,
                "NGRAM builder received null values with non-zero count");
            AddRows(
                n,
                [valid](size_t i) {
                    return valid != nullptr && !valid[i]
                               ? JsonProjectedStringState::FieldNull
                               : JsonProjectedStringState::Value;
                },
                [values](size_t i) { return values[i]; });
        } catch (...) {
            failed_ = true;
            throw;
        }
    }

    void
    Add(size_t n, const JsonProjectedString* values, const bool* valid) {
        try {
            AssertInfo(n == 0 || values != nullptr,
                       "JSON NGRAM builder received null values with non-zero "
                       "count");
            AssertInfo(valid == nullptr,
                       "JSON NGRAM tri-state input must not carry a separate "
                       "validity mask");
            AddRows(
                n,
                [values](size_t i) { return values[i].state; },
                [values](size_t i) { return values[i].value; });
        } catch (...) {
            failed_ = true;
            throw;
        }
    }

    storage::ArtifactPtr
    Seal() && {
        try {
            AssertInfo(!sealed_, "NGRAM builder cannot Seal more than once");
            AssertInfo(!failed_,
                       "NGRAM builder cannot Seal after a failed append");
            sealed_ = true;

            // On every failure path the writer must be destroyed before its
            // directory owner removes the files it may still reference.
            auto directory = std::move(directory_);
            auto engine = std::exchange(engine_, nullptr);
            auto null_offsets = std::move(null_offsets_);
            AssertInfo(engine != nullptr,
                       "NGRAM builder has no writer to seal");
            engine->finish();
            engine.reset();

            const auto avg_row_size =
                valid_rows_ == 0 ? 0 : total_bytes_ / valid_rows_;
            return std::make_unique<NgramIndexArtifact>(std::move(directory),
                                                        std::move(null_offsets),
                                                        params_.value_type,
                                                        params_.min_gram,
                                                        params_.max_gram,
                                                        avg_row_size);
        } catch (...) {
            failed_ = true;
            throw;
        }
    }

 private:
    template <typename StateAt, typename ValueAt>
    void
    AddRows(size_t n, StateAt state_at, ValueAt value_at) {
        try {
            AssertInfo(!sealed_, "NGRAM builder cannot Add after Seal");
            AssertInfo(!failed_,
                       "NGRAM builder cannot Add after a failed append");
            CheckAppend(count_, n);

            size_t added_nulls = 0;
            size_t added_bytes = 0;
            size_t added_rows = 0;
            for (size_t i = 0; i < n; ++i) {
                switch (state_at(i)) {
                    case JsonProjectedStringState::FieldNull:
                        ++added_nulls;
                        break;
                    case JsonProjectedStringState::NoValue:
                        break;
                    case JsonProjectedStringState::Value: {
                        const auto value = value_at(i);
                        if (value.size() >
                            std::numeric_limits<size_t>::max() - added_bytes) {
                            ThrowInfo(DataTypeInvalid,
                                      "NGRAM input byte count overflows");
                        }
                        added_bytes += value.size();
                        ++added_rows;
                        break;
                    }
                    default:
                        ThrowInfo(UnexpectedError,
                                  "JSON NGRAM input has an invalid projection "
                                  "state");
                }
            }
            if (added_nulls > null_offsets_.max_size() - null_offsets_.size()) {
                ThrowInfo(DataTypeInvalid,
                          "NGRAM null-offset count exceeds vector capacity");
            }
            if (added_bytes >
                    std::numeric_limits<size_t>::max() - total_bytes_ ||
                added_rows > std::numeric_limits<size_t>::max() - valid_rows_) {
                ThrowInfo(DataTypeInvalid,
                          "NGRAM row-size accounting overflows");
            }
            null_offsets_.reserve(null_offsets_.size() + added_nulls);

            std::string owned;
            for (size_t i = 0; i < n; ++i) {
                const auto offset = count_ + i;
                const auto state = state_at(i);
                if (state != JsonProjectedStringState::Value) {
                    if (state == JsonProjectedStringState::FieldNull) {
                        null_offsets_.push_back(offset);
                    }
                    const std::string* empty = nullptr;
                    engine_->add_array_data(
                        empty, 0, static_cast<int64_t>(offset));
                    continue;
                }
                AssignString(owned, value_at(i));
                engine_->add_data(&owned, 1, static_cast<int64_t>(offset));
            }
            count_ += n;
            total_bytes_ += added_bytes;
            valid_rows_ += added_rows;
        } catch (...) {
            failed_ = true;
            throw;
        }
    }

    NgramBuildParams params_;
    // The writer must be destroyed before its directory owner.
    std::shared_ptr<NgramIndexDirectory> directory_;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::vector<size_t> null_offsets_;
    size_t total_bytes_{0};
    size_t valid_rows_{0};
    size_t count_{0};
    bool sealed_{false};
    bool failed_{false};
};

NgramIndexBuilder::NgramIndexBuilder(NgramBuildParams params)
    : core_(std::make_unique<NgramBuilderCore>(std::move(params))) {
}

NgramIndexBuilder::~NgramIndexBuilder() = default;

BuilderInputSpec
NgramIndexBuilder::InputSpec() const {
    return core_->InputSpec();
}

void
NgramIndexBuilder::Add(size_t n,
                       const std::string_view* values,
                       const bool* valid) {
    core_->Add(n, values, valid);
}

storage::ArtifactPtr
NgramIndexBuilder::Seal() && {
    return std::move(*core_).Seal();
}

JsonNgramIndexBuilder::JsonNgramIndexBuilder(NgramBuildParams params)
    : core_(std::make_unique<NgramBuilderCore>(std::move(params))) {
}

JsonNgramIndexBuilder::~JsonNgramIndexBuilder() = default;

BuilderInputSpec
JsonNgramIndexBuilder::InputSpec() const {
    return core_->InputSpec();
}

void
JsonNgramIndexBuilder::Add(size_t n,
                           const JsonProjectedString* values,
                           const bool* valid) {
    core_->Add(n, values, valid);
}

storage::ArtifactPtr
JsonNgramIndexBuilder::Seal() && {
    return std::move(*core_).Seal();
}

namespace {

const bool kNgramBuilderRegistered = [] {
    BuilderRegistry<std::string_view>::Instance().Register(
        families::kNgram, [](const BuildParams& params) {
            return std::make_unique<NgramIndexBuilder>(
                ParseBuildParams(params, false));
        });
    BuilderRegistry<JsonProjectedString>::Instance().Register(
        families::kNgram, [](const BuildParams& params) {
            return std::make_unique<JsonNgramIndexBuilder>(
                ParseBuildParams(params, true));
        });
    return true;
}();

}  // namespace

}  // namespace milvus::index
