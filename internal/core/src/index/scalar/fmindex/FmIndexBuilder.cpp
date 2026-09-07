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

#include "index/scalar/fmindex/FmIndexBuilder.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <limits>
#include <optional>
#include <utility>

#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/fmindex/FMIndex.h"
#include "index/scalar/fmindex/FmIndexArtifact.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

constexpr uint32_t kDefaultSampleRate = 8;
constexpr uint32_t kMinSampleRate = 4;
constexpr uint32_t kMaxSampleRate = 256;
constexpr uint32_t kDefaultBlockBytes = 64;
constexpr uint32_t kMinBlockBytes = 8;
constexpr uint32_t kMaxBlockBytes = 128;

bool
IsStringType(DataType type) {
    return type == DataType::STRING || type == DataType::VARCHAR ||
           type == DataType::TEXT;
}

DataType
ParseDataTypeValue(const nlohmann::json& value, std::string_view key) {
    int64_t numeric = 0;
    if (value.is_number_unsigned()) {
        const auto encoded = value.get<uint64_t>();
        if (encoded > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
            ThrowInfo(DataTypeInvalid,
                      "FM-index parameter {} is not a valid data type",
                      key);
        }
        numeric = static_cast<int64_t>(encoded);
    } else if (value.is_number_integer()) {
        numeric = value.get<int64_t>();
    } else if (value.is_string()) {
        const auto text = value.get<std::string>();
        if (text == "NONE") {
            return DataType::NONE;
        }
        if (text == "STRING") {
            return DataType::STRING;
        }
        if (text == "VARCHAR") {
            return DataType::VARCHAR;
        }
        if (text == "TEXT") {
            return DataType::TEXT;
        }
        const auto* begin = text.data();
        const auto* end = begin + text.size();
        if (begin != end && *begin == '+') {
            ++begin;
        }
        const auto [parsed_end, error] = std::from_chars(begin, end, numeric);
        if (begin == end || error != std::errc() || parsed_end != end) {
            ThrowInfo(DataTypeInvalid,
                      "unsupported FM-index data type {} for parameter {}",
                      text,
                      key);
        }
    } else {
        ThrowInfo(
            DataTypeInvalid, "FM-index parameter {} must be a data type", key);
    }
    if (numeric < std::numeric_limits<int>::min() ||
        numeric > std::numeric_limits<int>::max()) {
        ThrowInfo(DataTypeInvalid,
                  "FM-index parameter {} is not a valid data type",
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
        const auto encoded = value.get<uint64_t>();
        if (encoded == 0 || encoded == 1) {
            return encoded != 0;
        }
    } else if (value.is_number_integer()) {
        const auto encoded = value.get<int64_t>();
        if (encoded == 0 || encoded == 1) {
            return encoded != 0;
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
    ThrowInfo(DataTypeInvalid, "FM-index parameter {} must be a boolean", key);
}

bool
ParseNested(const Config& params) {
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
                      "FM-index nested parameters {} and {} disagree",
                      first_key,
                      key);
        }
        if (!nested.has_value()) {
            nested = value;
            first_key = key;
        }
    }
    return nested.value_or(false);
}

DataType
ParseValueType(const Config& params) {
    const auto field_type = ReadDataType(params, "field_type");
    const auto value_type = ReadDataType(params, "value_type");
    for (const auto type : {field_type, value_type}) {
        if (type.has_value() && !IsStringType(*type)) {
            ThrowInfo(DataTypeInvalid,
                      "FM-index requires a string value type, got {}",
                      static_cast<int>(*type));
        }
    }
    for (const auto key : {std::string_view("array_element_type"),
                           std::string_view("element_type")}) {
        const auto type = ReadDataType(params, key);
        if (type.has_value() && *type != DataType::NONE) {
            ThrowInfo(DataTypeInvalid,
                      "FM-index parameter {} must be NONE, got {}",
                      key,
                      static_cast<int>(*type));
        }
    }
    if (ParseNested(params)) {
        ThrowInfo(DataTypeInvalid, "FM-index does not support nested input");
    }
    return value_type.value_or(field_type.value_or(DataType::VARCHAR));
}

uint32_t
ParseUnsignedParam(const Config& params,
                   std::string_view key,
                   uint32_t fallback) {
    if (!params.contains(key) || params.at(key).is_null()) {
        return fallback;
    }
    const auto& value = params.at(key);
    uint64_t encoded = 0;
    if (value.is_number_unsigned()) {
        encoded = value.get<uint64_t>();
    } else if (value.is_number_integer()) {
        const auto signed_value = value.get<int64_t>();
        if (signed_value < 0) {
            ThrowInfo(DataTypeInvalid,
                      "FM-index parameter {} must be non-negative",
                      key);
        }
        encoded = static_cast<uint64_t>(signed_value);
    } else if (value.is_string()) {
        const auto text = value.get<std::string>();
        const auto* begin = text.data();
        const auto* end = begin + text.size();
        if (begin != end && *begin == '+') {
            ++begin;
        }
        const auto [parsed_end, error] = std::from_chars(begin, end, encoded);
        if (begin == end || error != std::errc() || parsed_end != end) {
            ThrowInfo(DataTypeInvalid,
                      "FM-index parameter {} must be an unsigned integer",
                      key);
        }
    } else {
        ThrowInfo(DataTypeInvalid,
                  "FM-index parameter {} must be an unsigned integer",
                  key);
    }
    if (encoded > std::numeric_limits<uint32_t>::max()) {
        ThrowInfo(DataTypeInvalid,
                  "FM-index parameter {} exceeds uint32 capacity",
                  key);
    }
    return static_cast<uint32_t>(encoded);
}

std::string
ParseStringParam(const Config& params, std::string_view key) {
    if (!params.contains(key) || params.at(key).is_null()) {
        return {};
    }
    if (!params.at(key).is_string()) {
        ThrowInfo(
            DataTypeInvalid, "FM-index parameter {} must be a string", key);
    }
    return params.at(key).get<std::string>();
}

FmIndexBuildParams
ParseBuildParams(const BuildParams& params) {
    FmIndexBuildParams result;
    result.sa_sample_rate =
        ParseUnsignedParam(params, FM_SA_SAMPLE_RATE, kDefaultSampleRate);
    result.block_bytes =
        ParseUnsignedParam(params, FM_BLOCK_BYTES, kDefaultBlockBytes);
    if (result.sa_sample_rate < kMinSampleRate ||
        result.sa_sample_rate > kMaxSampleRate) {
        ThrowInfo(DataTypeInvalid,
                  "fm_sa_sample_rate must be in [{}, {}], got {}",
                  kMinSampleRate,
                  kMaxSampleRate,
                  result.sa_sample_rate);
    }
    if (result.block_bytes < kMinBlockBytes ||
        result.block_bytes > kMaxBlockBytes ||
        (result.block_bytes & (result.block_bytes - 1)) != 0) {
        ThrowInfo(DataTypeInvalid,
                  "fm_block_bytes must be a power of two in [{}, {}], got {}",
                  kMinBlockBytes,
                  kMaxBlockBytes,
                  result.block_bytes);
    }
    result.value_type = ParseValueType(params);
    if (params.contains("nullable") && !params.at("nullable").is_null()) {
        result.nullable = ParseBoolValue(params.at("nullable"), "nullable");
    }
    result.local_dir = ParseStringParam(params, "local_dir");
    return result;
}

}  // namespace

FmIndexBuilder::FmIndexBuilder(FmIndexBuildParams params)
    : params_(std::move(params)) {
    params_.sa_sample_rate = params_.sa_sample_rate == 0
                                 ? kDefaultSampleRate
                                 : params_.sa_sample_rate;
    params_.block_bytes =
        params_.block_bytes == 0 ? kDefaultBlockBytes : params_.block_bytes;
    if (params_.sa_sample_rate < kMinSampleRate ||
        params_.sa_sample_rate > kMaxSampleRate) {
        ThrowInfo(DataTypeInvalid,
                  "fm_sa_sample_rate must be in [{}, {}], got {}",
                  kMinSampleRate,
                  kMaxSampleRate,
                  params_.sa_sample_rate);
    }
    if (params_.block_bytes < kMinBlockBytes ||
        params_.block_bytes > kMaxBlockBytes ||
        (params_.block_bytes & (params_.block_bytes - 1)) != 0) {
        ThrowInfo(DataTypeInvalid,
                  "fm_block_bytes must be a power of two in [{}, {}], got {}",
                  kMinBlockBytes,
                  kMaxBlockBytes,
                  params_.block_bytes);
    }
    if (!IsStringType(params_.value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "FM-index requires a string value type, got {}",
                  static_cast<int>(params_.value_type));
    }
}

FmIndexBuilder::~FmIndexBuilder() = default;

BuilderInputSpec
FmIndexBuilder::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Contiguous,
                            .needs_second_pass = false};
}

void
FmIndexBuilder::Add(size_t n,
                    const std::string_view* values,
                    const bool* valid) {
    AssertInfo(!sealed_, "FM-index builder cannot Add after Seal");
    AssertInfo(!failed_, "FM-index builder cannot Add after a failed Add");
    AssertInfo(n == 0 || values != nullptr,
               "FM-index builder received null values with non-zero count");
    if (n > static_cast<size_t>(std::numeric_limits<int64_t>::max()) ||
        total_rows_ >
            std::numeric_limits<int64_t>::max() - static_cast<int64_t>(n)) {
        ThrowInfo(IndexBuildError,
                  "FM-index row count {} + {} exceeds int64 capacity",
                  total_rows_,
                  n);
    }

    uint64_t added_content_bytes = 0;
    for (size_t i = 0; i < n; ++i) {
        const bool row_valid = valid == nullptr || valid[i];
        if (!row_valid && !params_.nullable) {
            ThrowInfo(DataFormatBroken,
                      "non-nullable FM-index input contains a null row at {}",
                      total_rows_ + static_cast<int64_t>(i));
        }
        if (row_valid) {
            AssertInfo(values[i].empty() || values[i].data() != nullptr,
                       "FM-index row {} has a null buffer with non-zero size",
                       total_rows_ + static_cast<int64_t>(i));
            if (values[i].size() >
                static_cast<size_t>(std::numeric_limits<int64_t>::max()) -
                    added_content_bytes) {
                ThrowInfo(IndexBuildError,
                          "FM-index corpus exceeds int64 capacity");
            }
            added_content_bytes += values[i].size();
        }
    }
    const auto existing_internal_bytes = static_cast<uint64_t>(corpus_.size()) +
                                         static_cast<uint64_t>(total_rows_);
    if (added_content_bytes >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) - n ||
        existing_internal_bytes >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) - n -
                added_content_bytes) {
        ThrowInfo(IndexBuildError, "FM-index corpus exceeds int64 capacity");
    }

    const auto new_rows = total_rows_ + static_cast<int64_t>(n);
    try {
        null_bitmap_.resize(static_cast<size_t>(new_rows), false);
        for (size_t i = 0; i < n; ++i) {
            if (valid != nullptr && !valid[i]) {
                null_bitmap_.set(static_cast<size_t>(total_rows_) + i);
            } else if (!values[i].empty()) {
                const auto* begin =
                    reinterpret_cast<const uint8_t*>(values[i].data());
                corpus_.insert(corpus_.end(), begin, begin + values[i].size());
            }
            document_offsets_.push_back(corpus_.size());
        }
        total_rows_ = new_rows;
    } catch (const SegcoreError&) {
        failed_ = true;
        throw;
    } catch (const std::bad_alloc& error) {
        failed_ = true;
        ThrowInfo(MemAllocateFailed,
                  "failed to buffer FM-index input: {}",
                  error.what());
    } catch (const std::exception& error) {
        failed_ = true;
        ThrowInfo(IndexBuildError,
                  "failed to buffer FM-index input: {}",
                  error.what());
    }
}

storage::ArtifactPtr
FmIndexBuilder::Seal() && {
    AssertInfo(!sealed_, "FM-index builder cannot Seal more than once");
    AssertInfo(!failed_, "FM-index builder cannot Seal after a failed Add");
    sealed_ = true;

    std::vector<std::string_view> views;
    try {
        AssertInfo(
            document_offsets_.size() == static_cast<size_t>(total_rows_) + 1,
            "FM-index buffered row boundaries are inconsistent");
        views.reserve(static_cast<size_t>(total_rows_));
        const auto* data = corpus_.empty()
                               ? ""
                               : reinterpret_cast<const char*>(corpus_.data());
        for (int64_t row = 0; row < total_rows_; ++row) {
            const auto begin = document_offsets_[static_cast<size_t>(row)];
            const auto end = document_offsets_[static_cast<size_t>(row) + 1];
            views.emplace_back(data + begin, end - begin);
        }
        // Every view borrows only corpus_. Row boundaries are no longer needed
        // by the engine build and can be released before its peak allocation.
        std::vector<size_t>().swap(document_offsets_);
        fmindex::FMIndex engine;
        engine.Build(views,
                     params_.sa_sample_rate,
                     /*case_insensitive=*/false,
                     /*force_wide=*/false,
                     params_.block_bytes);
        const auto expected_internal_bytes =
            corpus_.size() + static_cast<size_t>(total_rows_);
        if (!engine.valid() ||
            engine.document_count() != static_cast<size_t>(total_rows_) ||
            engine.bwt_size() - 1 != expected_internal_bytes) {
            ThrowInfo(IndexBuildError,
                      "FM-index engine produced inconsistent row/text counts");
        }
        std::vector<uint8_t>().swap(corpus_);
        std::vector<std::string_view>().swap(views);
        return std::make_unique<FmIndexArtifact>(std::move(engine),
                                                 std::move(null_bitmap_),
                                                 total_rows_,
                                                 params_.value_type,
                                                 params_.nullable,
                                                 std::move(params_.local_dir));
    } catch (const SegcoreError&) {
        throw;
    } catch (const std::bad_alloc& error) {
        ThrowInfo(
            MemAllocateFailed, "failed to build FM-index: {}", error.what());
    } catch (const std::exception& error) {
        ThrowInfo(
            IndexBuildError, "failed to build FM-index: {}", error.what());
    }
}

namespace {

const bool kFmBuilderRegistered = [] {
    BuilderRegistry<std::string_view>::Instance().Register(
        families::kFmIndex, [](const BuildParams& params) {
            return std::make_unique<FmIndexBuilder>(ParseBuildParams(params));
        });
    return true;
}();

}  // namespace

}  // namespace milvus::index
