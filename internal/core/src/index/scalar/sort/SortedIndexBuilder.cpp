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

#include "index/scalar/sort/SortedIndexBuilder.h"

#include <algorithm>
#include <limits>
#include <map>
#include <optional>
#include <type_traits>
#include <utility>

#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/sort/SortedIndexArtifact.h"

namespace milvus::index {
namespace {

template <typename T>
constexpr DataType
CppDataType() {
    if constexpr (std::is_same_v<T, bool>) {
        return DataType::BOOL;
    } else if constexpr (std::is_same_v<T, int8_t>) {
        return DataType::INT8;
    } else if constexpr (std::is_same_v<T, int16_t>) {
        return DataType::INT16;
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return DataType::INT32;
    } else if constexpr (std::is_same_v<T, int64_t>) {
        return DataType::INT64;
    } else if constexpr (std::is_same_v<T, float>) {
        return DataType::FLOAT;
    } else if constexpr (std::is_same_v<T, double>) {
        return DataType::DOUBLE;
    } else {
        return DataType::VARCHAR;
    }
}

bool
IsStringType(DataType type) {
    return type == DataType::STRING || type == DataType::VARCHAR ||
           type == DataType::TEXT;
}

bool
CompatibleValueType(DataType actual, DataType expected) {
    return actual == expected ||
           (IsStringType(actual) && IsStringType(expected)) ||
           ((actual == DataType::INT64 || actual == DataType::TIMESTAMPTZ) &&
            (expected == DataType::INT64 || expected == DataType::TIMESTAMPTZ));
}

DataType
ParseDataType(const Config& params, std::string_view key, DataType fallback) {
    if (!params.contains(key)) {
        return fallback;
    }
    const auto& value = params.at(key);
    if (value.is_number_integer() || value.is_number_unsigned()) {
        return static_cast<DataType>(value.get<int>());
    }
    if (!value.is_string()) {
        ThrowInfo(
            DataTypeInvalid, "sorted parameter {} must be a data type", key);
    }
    const auto text = value.get<std::string>();
    try {
        size_t parsed = 0;
        const auto numeric = std::stoi(text, &parsed);
        if (parsed == text.size()) {
            return static_cast<DataType>(numeric);
        }
    } catch (const std::exception&) {
    }
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
    const auto it = kNames.find(text);
    if (it == kNames.end()) {
        ThrowInfo(DataTypeInvalid,
                  "unsupported sorted data type {} for parameter {}",
                  text,
                  key);
    }
    return it->second;
}

bool
ParseNested(const Config& params) {
    std::optional<bool> nested;
    for (const auto key : {"nested", "is_nested", "is_nested_index"}) {
        const auto value = GetValueFromConfig<bool>(params, key);
        if (!value.has_value()) {
            continue;
        }
        if (nested.has_value() && *nested != *value) {
            ThrowInfo(DataTypeInvalid, "conflicting sorted nested parameters");
        }
        nested = *value;
    }
    return nested.value_or(false);
}

SortedBuildParams
ParseBuildParams(const Config& params,
                 DataType default_type,
                 bool array_builder) {
    SortedBuildParams result;
    result.nested = ParseNested(params);
    result.field_type = ParseDataType(
        params, "field_type", array_builder ? DataType::ARRAY : default_type);
    const auto array_element_type =
        ParseDataType(params, "array_element_type", DataType::NONE);
    const auto legacy_element_type =
        ParseDataType(params, "element_type", DataType::NONE);
    if (array_element_type != DataType::NONE &&
        legacy_element_type != DataType::NONE &&
        !CompatibleValueType(array_element_type, legacy_element_type)) {
        ThrowInfo(DataTypeInvalid,
                  "sorted array_element_type {} conflicts with element_type "
                  "{}",
                  static_cast<int>(array_element_type),
                  static_cast<int>(legacy_element_type));
    }
    const auto element_type = array_element_type != DataType::NONE
                                  ? array_element_type
                                  : legacy_element_type;
    const auto configured = ParseDataType(params, "value_type", DataType::NONE);

    if (array_builder && result.field_type != DataType::ARRAY) {
        ThrowInfo(DataTypeInvalid,
                  "sorted ArrayView input requires ARRAY field_type, got {}",
                  static_cast<int>(result.field_type));
    }
    if (result.field_type == DataType::JSON) {
        if (result.nested) {
            ThrowInfo(DataTypeInvalid,
                      "nested sorted input requires ARRAY field_type");
        }
        if (element_type != DataType::NONE) {
            ThrowInfo(DataTypeInvalid,
                      "sorted JSON field conflicts with element type {}",
                      static_cast<int>(element_type));
        }
        if (configured == DataType::NONE || configured == DataType::ARRAY ||
            configured == DataType::JSON) {
            ThrowInfo(DataTypeInvalid,
                      "sorted JSON input requires a concrete cast "
                      "value_type");
        }
        result.value_type = configured;
    } else if (result.field_type == DataType::ARRAY) {
        if (array_builder && result.nested) {
            ThrowInfo(DataTypeInvalid,
                      "nested ARRAY sorted input must use typed flattened "
                      "elements");
        }
        if (!array_builder && !result.nested) {
            ThrowInfo(DataTypeInvalid,
                      "non-nested ARRAY sorted input must use ArrayView");
        }
        if (configured != DataType::NONE && configured != DataType::ARRAY &&
            element_type != DataType::NONE &&
            !CompatibleValueType(configured, element_type)) {
            ThrowInfo(DataTypeInvalid,
                      "sorted ARRAY value_type {} conflicts with element type "
                      "{}",
                      static_cast<int>(configured),
                      static_cast<int>(element_type));
        }
        result.value_type =
            element_type != DataType::NONE
                ? element_type
                : (configured != DataType::NONE && configured != DataType::ARRAY
                       ? configured
                       : default_type);
    } else {
        if (result.nested) {
            ThrowInfo(DataTypeInvalid,
                      "nested sorted input requires ARRAY field_type");
        }
        if (element_type != DataType::NONE) {
            ThrowInfo(DataTypeInvalid,
                      "sorted scalar field_type {} conflicts with element "
                      "type {}",
                      static_cast<int>(result.field_type),
                      static_cast<int>(element_type));
        }
        if (configured == DataType::ARRAY) {
            ThrowInfo(DataTypeInvalid,
                      "sorted scalar input cannot use ARRAY value_type");
        }
        if (configured != DataType::NONE &&
            !CompatibleValueType(configured, result.field_type)) {
            ThrowInfo(DataTypeInvalid,
                      "sorted value_type {} conflicts with field_type {}",
                      static_cast<int>(configured),
                      static_cast<int>(result.field_type));
        }
        result.value_type =
            configured != DataType::NONE ? configured : result.field_type;
    }
    if (result.value_type == DataType::NONE ||
        result.value_type == DataType::ARRAY) {
        ThrowInfo(DataTypeInvalid,
                  "sorted input requires a concrete value type");
    }
    return result;
}

template <typename T>
void
ValidateTypedBuildParams(SortedBuildParams& params) {
    const auto cpp_type = CppDataType<T>();
    if (params.field_type == DataType::NONE) {
        params.field_type = cpp_type;
    }
    if (params.field_type == DataType::JSON &&
        (params.value_type == DataType::NONE ||
         params.value_type == DataType::ARRAY ||
         params.value_type == DataType::JSON)) {
        ThrowInfo(DataTypeInvalid,
                  "sorted JSON input requires a concrete cast value_type");
    }
    if (params.value_type == DataType::NONE ||
        (params.value_type == DataType::ARRAY &&
         params.field_type == DataType::ARRAY && params.nested)) {
        params.value_type = cpp_type;
    }
    if (params.field_type == DataType::ARRAY) {
        if (!params.nested) {
            ThrowInfo(DataTypeInvalid,
                      "non-nested ARRAY sorted input must use ArrayView");
        }
    } else if (params.nested) {
        ThrowInfo(DataTypeInvalid,
                  "nested sorted input requires ARRAY field_type");
    } else if (params.field_type != DataType::JSON &&
               !CompatibleValueType(params.field_type, cpp_type)) {
        ThrowInfo(DataTypeInvalid,
                  "sorted typed input {} conflicts with field_type {}",
                  static_cast<int>(cpp_type),
                  static_cast<int>(params.field_type));
    }
    if (!CompatibleValueType(params.value_type, cpp_type)) {
        ThrowInfo(DataTypeInvalid,
                  "sorted typed input {} conflicts with value_type {}",
                  static_cast<int>(cpp_type),
                  static_cast<int>(params.value_type));
    }
}

void
CheckAppend(size_t current, size_t count) {
    constexpr auto kLimit =
        static_cast<size_t>(std::numeric_limits<int32_t>::max());
    if (current > kLimit || count > kLimit - current) {
        ThrowInfo(DataTypeInvalid,
                  "sorted index coordinate count {} + {} exceeds int32 "
                  "domain",
                  current,
                  count);
    }
}

TargetBitmap
MakeValidity(const std::vector<uint8_t>& validity) {
    TargetBitmap result(validity.size(), false);
    for (size_t i = 0; i < validity.size(); ++i) {
        if (validity[i] != 0) {
            result.set(i);
        }
    }
    return result;
}

template <typename T>
std::vector<int32_t>
MakeNumericOffsets(const std::vector<IndexStructure<T>>& data, size_t count) {
    std::vector<int32_t> result(count, -1);
    if (data.size() >
        static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        ThrowInfo(DataTypeInvalid,
                  "sorted index has too many value entries: {}",
                  data.size());
    }
    for (size_t i = 0; i < data.size(); ++i) {
        const auto row = data[i].idx_;
        AssertInfo(row >= 0 && static_cast<size_t>(row) < count,
                   "sorted row coordinate {} is outside [0, {})",
                   row,
                   count);
        result[static_cast<size_t>(row)] = static_cast<int32_t>(i);
    }
    return result;
}

std::vector<int32_t>
MakeStringOffsets(const std::vector<std::vector<uint32_t>>& postings,
                  size_t count) {
    if (postings.size() >
        static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        ThrowInfo(DataTypeInvalid,
                  "sorted string index has too many unique values: {}",
                  postings.size());
    }
    std::vector<int32_t> result(count, -1);
    for (size_t value = 0; value < postings.size(); ++value) {
        for (auto row : postings[value]) {
            AssertInfo(static_cast<size_t>(row) < count,
                       "sorted string row coordinate {} is outside [0, {})",
                       row,
                       count);
            result[row] = static_cast<int32_t>(value);
        }
    }
    return result;
}

template <typename T>
T
ArrayValue(const ArrayView& array, int index) {
    if constexpr (std::is_same_v<T, std::string_view>) {
        return array.get_data<std::string_view>(index);
    } else {
        return array.get_data<T>(index);
    }
}

}  // namespace

template <typename T>
SortedIndexBuilder<T>::SortedIndexBuilder(SortedBuildParams params)
    : params_(std::move(params)) {
    ValidateTypedBuildParams<T>(params_);
}

template <typename T>
SortedIndexBuilder<T>::~SortedIndexBuilder() = default;

template <typename T>
BuilderInputSpec
SortedIndexBuilder<T>::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Contiguous,
                            .needs_second_pass = false};
}

template <typename T>
void
SortedIndexBuilder<T>::Add(size_t n, const T* values, const bool* valid) {
    AssertInfo(!sealed_, "sorted builder cannot Add after Seal");
    AssertInfo(n == 0 || values != nullptr,
               "sorted builder received null values with non-zero count");
    CheckAppend(total_num_rows_, n);
    data_.reserve(data_.size() + n);
    validity_.reserve(validity_.size() + n);
    for (size_t i = 0; i < n; ++i) {
        const auto coordinate = total_num_rows_ + i;
        const bool is_valid = params_.nested || valid == nullptr || valid[i];
        validity_.push_back(is_valid ? 1 : 0);
        if (is_valid) {
            data_.emplace_back(values[i], static_cast<int32_t>(coordinate));
        }
    }
    total_num_rows_ += n;
}

template <typename T>
storage::ArtifactPtr
SortedIndexBuilder<T>::Seal() && {
    AssertInfo(!sealed_, "sorted builder cannot Seal more than once");
    if (total_num_rows_ == 0) {
        ThrowInfo(DataIsEmpty, "sorted index cannot build empty input");
    }
    sealed_ = true;
    std::sort(data_.begin(), data_.end());
    auto offsets = MakeNumericOffsets(data_, total_num_rows_);
    const bool value_lookup =
        params_.field_type != DataType::ARRAY || params_.nested;
    auto artifact =
        std::make_unique<SortedIndexArtifact<T>>(std::move(data_),
                                                 MakeValidity(validity_),
                                                 std::move(offsets),
                                                 total_num_rows_,
                                                 params_.value_type,
                                                 params_.nested,
                                                 value_lookup);
    std::vector<IndexStructure<T>>().swap(data_);
    std::vector<uint8_t>().swap(validity_);
    return artifact;
}

SortedStringIndexBuilder::SortedStringIndexBuilder(SortedBuildParams params)
    : params_(std::move(params)) {
    ValidateTypedBuildParams<std::string_view>(params_);
}

SortedStringIndexBuilder::~SortedStringIndexBuilder() = default;

BuilderInputSpec
SortedStringIndexBuilder::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Contiguous,
                            .needs_second_pass = false};
}

void
SortedStringIndexBuilder::Add(size_t n,
                              const std::string_view* values,
                              const bool* valid) {
    AssertInfo(!sealed_, "sorted string builder cannot Add after Seal");
    AssertInfo(n == 0 || values != nullptr,
               "sorted string builder received null values with non-zero "
               "count");
    CheckAppend(total_num_rows_, n);
    validity_.reserve(validity_.size() + n);
    for (size_t i = 0; i < n; ++i) {
        const auto coordinate = total_num_rows_ + i;
        const bool is_valid = params_.nested || valid == nullptr || valid[i];
        validity_.push_back(is_valid ? 1 : 0);
        if (is_valid) {
            postings_[std::string(values[i])].push_back(
                static_cast<uint32_t>(coordinate));
        }
    }
    total_num_rows_ += n;
}

storage::ArtifactPtr
SortedStringIndexBuilder::Seal() && {
    AssertInfo(!sealed_, "sorted string builder cannot Seal more than once");
    if (total_num_rows_ == 0) {
        ThrowInfo(DataIsEmpty, "sorted string index cannot build empty input");
    }
    sealed_ = true;
    std::vector<std::string> values;
    std::vector<std::vector<uint32_t>> postings;
    values.reserve(postings_.size());
    postings.reserve(postings_.size());
    for (auto& [value, rows] : postings_) {
        values.push_back(std::move(value));
        postings.push_back(std::move(rows));
    }
    auto offsets = MakeStringOffsets(postings, total_num_rows_);
    const bool value_lookup =
        params_.field_type != DataType::ARRAY || params_.nested;
    auto artifact =
        std::make_unique<SortedStringIndexArtifact>(std::move(values),
                                                    std::move(postings),
                                                    MakeValidity(validity_),
                                                    std::move(offsets),
                                                    total_num_rows_,
                                                    params_.value_type,
                                                    params_.nested,
                                                    value_lookup);
    std::map<std::string, std::vector<uint32_t>>().swap(postings_);
    std::vector<uint8_t>().swap(validity_);
    return artifact;
}

class SortedArrayIndexBuilder::Impl {
 public:
    virtual ~Impl() = default;

    virtual void
    AddRow(const ArrayView& value, int32_t coordinate) = 0;

    virtual storage::ArtifactPtr
    Seal(TargetBitmap validity,
         size_t count,
         const SortedBuildParams& params) = 0;
};

namespace {

template <typename T>
class NumericArrayBuilderImpl final : public SortedArrayIndexBuilder::Impl {
 public:
    void
    AddRow(const ArrayView& value, int32_t coordinate) override {
        data_.reserve(data_.size() + static_cast<size_t>(value.length()));
        for (int i = 0; i < value.length(); ++i) {
            data_.emplace_back(ArrayValue<T>(value, i), coordinate);
        }
    }

    storage::ArtifactPtr
    Seal(TargetBitmap validity,
         size_t count,
         const SortedBuildParams& params) override {
        std::sort(data_.begin(), data_.end());
        auto offsets = MakeNumericOffsets(data_, count);
        return std::make_unique<SortedIndexArtifact<T>>(std::move(data_),
                                                        std::move(validity),
                                                        std::move(offsets),
                                                        count,
                                                        params.value_type,
                                                        false,
                                                        false);
    }

 private:
    std::vector<IndexStructure<T>> data_;
};

class StringArrayBuilderImpl final : public SortedArrayIndexBuilder::Impl {
 public:
    void
    AddRow(const ArrayView& value, int32_t coordinate) override {
        for (int i = 0; i < value.length(); ++i) {
            postings_[std::string(ArrayValue<std::string_view>(value, i))]
                .push_back(static_cast<uint32_t>(coordinate));
        }
    }

    storage::ArtifactPtr
    Seal(TargetBitmap validity,
         size_t count,
         const SortedBuildParams& params) override {
        std::vector<std::string> values;
        std::vector<std::vector<uint32_t>> postings;
        values.reserve(postings_.size());
        postings.reserve(postings_.size());
        for (auto& [value, rows] : postings_) {
            values.push_back(std::move(value));
            postings.push_back(std::move(rows));
        }
        auto offsets = MakeStringOffsets(postings, count);
        return std::make_unique<SortedStringIndexArtifact>(std::move(values),
                                                           std::move(postings),
                                                           std::move(validity),
                                                           std::move(offsets),
                                                           count,
                                                           params.value_type,
                                                           false,
                                                           false);
    }

 private:
    std::map<std::string, std::vector<uint32_t>> postings_;
};

std::unique_ptr<SortedArrayIndexBuilder::Impl>
MakeArrayImpl(DataType element_type) {
    switch (element_type) {
        case DataType::BOOL:
            return std::make_unique<NumericArrayBuilderImpl<bool>>();
        case DataType::INT8:
            return std::make_unique<NumericArrayBuilderImpl<int8_t>>();
        case DataType::INT16:
            return std::make_unique<NumericArrayBuilderImpl<int16_t>>();
        case DataType::INT32:
            return std::make_unique<NumericArrayBuilderImpl<int32_t>>();
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return std::make_unique<NumericArrayBuilderImpl<int64_t>>();
        case DataType::FLOAT:
            return std::make_unique<NumericArrayBuilderImpl<float>>();
        case DataType::DOUBLE:
            return std::make_unique<NumericArrayBuilderImpl<double>>();
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
            return std::make_unique<StringArrayBuilderImpl>();
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported ARRAY element type {} for sorted index",
                      static_cast<int>(element_type));
    }
}

}  // namespace

SortedArrayIndexBuilder::SortedArrayIndexBuilder(SortedBuildParams params)
    : params_(std::move(params)) {
    if (params_.nested) {
        ThrowInfo(DataTypeInvalid,
                  "nested ARRAY sorted input must be flattened into typed "
                  "elements");
    }
    if (params_.field_type != DataType::NONE &&
        params_.field_type != DataType::ARRAY) {
        ThrowInfo(DataTypeInvalid,
                  "sorted ArrayView input requires ARRAY field_type, got {}",
                  static_cast<int>(params_.field_type));
    }
    if (params_.value_type == DataType::NONE ||
        params_.value_type == DataType::ARRAY) {
        ThrowInfo(DataTypeInvalid,
                  "sorted ARRAY builder requires a concrete element type");
    }
    params_.field_type = DataType::ARRAY;
    impl_ = MakeArrayImpl(params_.value_type);
}

SortedArrayIndexBuilder::~SortedArrayIndexBuilder() = default;

BuilderInputSpec
SortedArrayIndexBuilder::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Contiguous,
                            .needs_second_pass = false};
}

void
SortedArrayIndexBuilder::Add(size_t n,
                             const ArrayView* values,
                             const bool* valid) {
    AssertInfo(!sealed_, "sorted ARRAY builder cannot Add after Seal");
    AssertInfo(n == 0 || values != nullptr,
               "sorted ARRAY builder received null values with non-zero "
               "count");
    for (size_t i = 0; i < n; ++i) {
        if ((valid == nullptr || valid[i]) &&
            !CompatibleValueType(values[i].get_element_type(),
                                 params_.value_type)) {
            ThrowInfo(DataTypeInvalid,
                      "ARRAY element type {} does not match sorted type {}",
                      static_cast<int>(values[i].get_element_type()),
                      static_cast<int>(params_.value_type));
        }
    }
    CheckAppend(total_num_rows_, n);
    validity_.reserve(validity_.size() + n);
    for (size_t i = 0; i < n; ++i) {
        const auto coordinate = total_num_rows_ + i;
        const bool is_valid = valid == nullptr || valid[i];
        validity_.push_back(is_valid ? 1 : 0);
        if (!is_valid) {
            continue;
        }
        impl_->AddRow(values[i], static_cast<int32_t>(coordinate));
    }
    total_num_rows_ += n;
}

storage::ArtifactPtr
SortedArrayIndexBuilder::Seal() && {
    AssertInfo(!sealed_, "sorted ARRAY builder cannot Seal more than once");
    if (total_num_rows_ == 0) {
        ThrowInfo(DataIsEmpty, "sorted ARRAY index cannot build empty input");
    }
    sealed_ = true;
    auto artifact =
        impl_->Seal(MakeValidity(validity_), total_num_rows_, params_);
    impl_.reset();
    std::vector<uint8_t>().swap(validity_);
    return artifact;
}

namespace {

template <typename T>
bool
RegisterSortedBuilder() {
    BuilderRegistry<T>::Instance().Register(
        families::kSort, [](const BuildParams& params) {
            return std::make_unique<SortedIndexBuilder<T>>(
                ParseBuildParams(params, CppDataType<T>(), false));
        });
    return true;
}

const bool kRegistered =
    RegisterSortedBuilder<bool>() && RegisterSortedBuilder<int8_t>() &&
    RegisterSortedBuilder<int16_t>() && RegisterSortedBuilder<int32_t>() &&
    RegisterSortedBuilder<int64_t>() && RegisterSortedBuilder<float>() &&
    RegisterSortedBuilder<double>();

const bool kStringRegistered = [] {
    BuilderRegistry<std::string_view>::Instance().Register(
        families::kSort, [](const BuildParams& params) {
            return std::make_unique<SortedStringIndexBuilder>(
                ParseBuildParams(params, DataType::VARCHAR, false));
        });
    return true;
}();

const bool kArrayRegistered = [] {
    BuilderRegistry<ArrayView>::Instance().Register(
        families::kSort, [](const BuildParams& params) {
            auto parsed = ParseBuildParams(params, DataType::NONE, true);
            if (parsed.value_type == DataType::NONE ||
                parsed.value_type == DataType::ARRAY) {
                ThrowInfo(DataTypeInvalid,
                          "sorted ARRAY builder requires array_element_type");
            }
            return std::make_unique<SortedArrayIndexBuilder>(std::move(parsed));
        });
    return true;
}();

}  // namespace

#define INSTANTIATE_SORTED_BUILDER(T) template class SortedIndexBuilder<T>;
INSTANTIATE_SORTED_BUILDER(bool)
INSTANTIATE_SORTED_BUILDER(int8_t)
INSTANTIATE_SORTED_BUILDER(int16_t)
INSTANTIATE_SORTED_BUILDER(int32_t)
INSTANTIATE_SORTED_BUILDER(int64_t)
INSTANTIATE_SORTED_BUILDER(float)
INSTANTIATE_SORTED_BUILDER(double)
#undef INSTANTIATE_SORTED_BUILDER

}  // namespace milvus::index
