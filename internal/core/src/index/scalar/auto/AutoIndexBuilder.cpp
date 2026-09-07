// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "index/scalar/auto/AutoIndexBuilder.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <limits>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

#include "common/Array.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/auto/AutoIndexArtifact.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

bool
IsStringType(DataType type) {
    return type == DataType::STRING || type == DataType::VARCHAR ||
           type == DataType::TEXT;
}

int64_t
ParseInteger(const nlohmann::json& value, std::string_view key) {
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
        if (!text.empty() && error == std::errc{} &&
            end == text.data() + text.size()) {
            return parsed;
        }
    }
    ThrowInfo(DataTypeInvalid, "AUTO parameter {} must be an integer", key);
}

int64_t
RequiredInteger(const BuildParams& params, std::string_view key) {
    if (!params.contains(key) || params.at(key).is_null()) {
        ThrowInfo(DataTypeInvalid, "AUTO parameter {} is required", key);
    }
    return ParseInteger(params.at(key), key);
}

int64_t
OptionalInteger(const BuildParams& params,
                std::string_view key,
                int64_t default_value) {
    if (!params.contains(key) || params.at(key).is_null()) {
        return default_value;
    }
    return ParseInteger(params.at(key), key);
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
        auto text = value.get<std::string>();
        std::transform(
            text.begin(), text.end(), text.begin(), [](unsigned char c) {
                return static_cast<char>(std::tolower(c));
            });
        if (text == "true" || text == "1") {
            return true;
        }
        if (text == "false" || text == "0") {
            return false;
        }
    }
    ThrowInfo(DataTypeInvalid, "AUTO parameter {} must be boolean", key);
}

bool
ParseNested(const BuildParams& params) {
    std::optional<bool> result;
    std::string_view first_key;
    for (const auto key : {std::string_view("nested"),
                           std::string_view("is_nested"),
                           std::string_view("is_nested_index")}) {
        if (!params.contains(key) || params.at(key).is_null()) {
            continue;
        }
        const auto parsed = ParseBool(params.at(key), key);
        if (result.has_value() && *result != parsed) {
            ThrowInfo(DataTypeInvalid,
                      "AUTO nested parameters {} and {} disagree",
                      first_key,
                      key);
        }
        if (!result.has_value()) {
            result = parsed;
            first_key = key;
        }
    }
    return result.value_or(false);
}

DataType
ParseDataType(const BuildParams& params, std::string_view key, bool required) {
    if (!params.contains(key) || params.at(key).is_null()) {
        if (required) {
            ThrowInfo(DataTypeInvalid, "AUTO parameter {} is required", key);
        }
        return DataType::NONE;
    }
    const auto value = ParseInteger(params.at(key), key);
    if (value < std::numeric_limits<int32_t>::min() ||
        value > std::numeric_limits<int32_t>::max()) {
        ThrowInfo(DataTypeInvalid, "AUTO data type {} is out of range", key);
    }
    return static_cast<DataType>(static_cast<int32_t>(value));
}

DataType
ParseElementType(const BuildParams& params) {
    auto current = ParseDataType(params, "array_element_type", false);
    auto legacy = ParseDataType(params, "element_type", false);
    if (current == DataType::NONE) {
        current = legacy;
    } else if (legacy != DataType::NONE && current != legacy &&
               !(IsStringType(current) && IsStringType(legacy))) {
        ThrowInfo(DataTypeInvalid,
                  "AUTO array_element_type conflicts with element_type");
    }
    return current;
}

template <typename T>
bool
CompatibleValueType(DataType type) {
    if constexpr (std::is_same_v<T, bool>) {
        return type == DataType::BOOL;
    } else if constexpr (std::is_same_v<T, int8_t>) {
        return type == DataType::INT8;
    } else if constexpr (std::is_same_v<T, int16_t>) {
        return type == DataType::INT16;
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return type == DataType::INT32;
    } else if constexpr (std::is_same_v<T, int64_t>) {
        return type == DataType::INT64 || type == DataType::TIMESTAMPTZ;
    } else if constexpr (std::is_same_v<T, float>) {
        return type == DataType::FLOAT;
    } else if constexpr (std::is_same_v<T, double>) {
        return type == DataType::DOUBLE;
    } else if constexpr (std::is_same_v<T, std::string_view>) {
        return IsStringType(type);
    } else if constexpr (std::is_same_v<T, ArrayView>) {
        return type == DataType::ARRAY;
    }
    return false;
}

bool
CompatibleArrayElementType(DataType actual, DataType expected) {
    return actual == expected ||
           (IsStringType(actual) && IsStringType(expected)) ||
           (actual == DataType::INT32 &&
            (expected == DataType::INT8 || expected == DataType::INT16));
}

bool
IsSupportedArrayElement(DataType type) {
    return type == DataType::BOOL || type == DataType::INT8 ||
           type == DataType::INT16 || type == DataType::INT32 ||
           type == DataType::INT64 || type == DataType::FLOAT ||
           type == DataType::DOUBLE || IsStringType(type);
}

ScalarIndexType
SelectorForFamily(const IndexFamily& family) {
    if (family == families::kBitmap) {
        return ScalarIndexType::BITMAP;
    }
    if (family == families::kSort) {
        return ScalarIndexType::STLSORT;
    }
    if (family == families::kMarisa) {
        return ScalarIndexType::MARISA;
    }
    if (family == families::kInverted) {
        return ScalarIndexType::INVERTED;
    }
    ThrowInfo(UnexpectedError,
              "AUTO selected family {} has no legacy selector",
              family);
}

template <typename T>
struct OwnedProbeValue {
    using Type = T;

    static Type
    Copy(T value) {
        return value;
    }
};

template <>
struct OwnedProbeValue<std::string_view> {
    using Type = std::string;

    static Type
    Copy(std::string_view value) {
        return std::string(value);
    }
};

template <typename T>
class CardinalityProbe {
 public:
    explicit CardinalityProbe(DataType) {
    }

    void
    Add(size_t n, const T* values, const bool* valid, size_t limit) {
        for (size_t i = 0; i < n && values_.size() < limit; ++i) {
            if (valid == nullptr || valid[i]) {
                values_.insert(OwnedProbeValue<T>::Copy(values[i]));
            }
        }
    }

    size_t
    Size() const {
        return values_.size();
    }

 private:
    std::set<typename OwnedProbeValue<T>::Type> values_;
};

template <>
class CardinalityProbe<ArrayView> {
 public:
    using Sets = std::variant<std::set<bool>,
                              std::set<int8_t>,
                              std::set<int16_t>,
                              std::set<int32_t>,
                              std::set<int64_t>,
                              std::set<float>,
                              std::set<double>,
                              std::set<std::string>>;

    explicit CardinalityProbe(DataType element_type)
        : element_type_(element_type) {
        switch (element_type_) {
            case DataType::BOOL:
                values_.emplace<std::set<bool>>();
                break;
            case DataType::INT8:
                values_.emplace<std::set<int8_t>>();
                break;
            case DataType::INT16:
                values_.emplace<std::set<int16_t>>();
                break;
            case DataType::INT32:
                values_.emplace<std::set<int32_t>>();
                break;
            case DataType::INT64:
                values_.emplace<std::set<int64_t>>();
                break;
            case DataType::FLOAT:
                values_.emplace<std::set<float>>();
                break;
            case DataType::DOUBLE:
                values_.emplace<std::set<double>>();
                break;
            case DataType::STRING:
            case DataType::VARCHAR:
            case DataType::TEXT:
                values_.emplace<std::set<std::string>>();
                break;
            default:
                ThrowInfo(DataTypeInvalid,
                          "AUTO does not support ARRAY element type {}",
                          static_cast<int>(element_type_));
        }
    }

    void
    Add(size_t n, const ArrayView* values, const bool* valid, size_t limit) {
        for (size_t row = 0; row < n && Size() < limit; ++row) {
            if (valid != nullptr && !valid[row]) {
                continue;
            }
            const auto& array = values[row];
            if (array.length() < 0 ||
                !CompatibleArrayElementType(array.get_element_type(),
                                            element_type_)) {
                ThrowInfo(DataTypeInvalid,
                          "AUTO ARRAY row {} has incompatible element type",
                          row);
            }
            if (array.length() != 0 && array.data() == nullptr) {
                ThrowInfo(DataFormatBroken,
                          "AUTO ARRAY row {} has null element data",
                          row);
            }
            for (int i = 0; i < array.length() && Size() < limit; ++i) {
                Insert(array, i);
            }
        }
    }

    size_t
    Size() const {
        return std::visit([](const auto& values) { return values.size(); },
                          values_);
    }

 private:
    void
    Insert(const ArrayView& array, int offset) {
        switch (element_type_) {
            case DataType::BOOL:
                std::get<std::set<bool>>(values_).insert(
                    array.get_data<bool>(offset));
                return;
            case DataType::INT8: {
                const auto value = array.get_data<int32_t>(offset);
                if (value < std::numeric_limits<int8_t>::min() ||
                    value > std::numeric_limits<int8_t>::max()) {
                    ThrowInfo(DataFormatBroken,
                              "AUTO ARRAY INT8 value {} is out of range",
                              value);
                }
                std::get<std::set<int8_t>>(values_).insert(
                    static_cast<int8_t>(value));
                return;
            }
            case DataType::INT16: {
                const auto value = array.get_data<int32_t>(offset);
                if (value < std::numeric_limits<int16_t>::min() ||
                    value > std::numeric_limits<int16_t>::max()) {
                    ThrowInfo(DataFormatBroken,
                              "AUTO ARRAY INT16 value {} is out of range",
                              value);
                }
                std::get<std::set<int16_t>>(values_).insert(
                    static_cast<int16_t>(value));
                return;
            }
            case DataType::INT32:
                std::get<std::set<int32_t>>(values_).insert(
                    array.get_data<int32_t>(offset));
                return;
            case DataType::INT64:
                std::get<std::set<int64_t>>(values_).insert(
                    array.get_data<int64_t>(offset));
                return;
            case DataType::FLOAT:
                std::get<std::set<float>>(values_).insert(
                    array.get_data<float>(offset));
                return;
            case DataType::DOUBLE:
                std::get<std::set<double>>(values_).insert(
                    array.get_data<double>(offset));
                return;
            case DataType::STRING:
            case DataType::VARCHAR:
            case DataType::TEXT:
                std::get<std::set<std::string>>(values_).insert(
                    std::string(array.get_data<std::string_view>(offset)));
                return;
            default:
                ThrowInfo(DataTypeInvalid,
                          "AUTO does not support ARRAY element type {}",
                          static_cast<int>(element_type_));
        }
    }

    DataType element_type_;
    Sets values_;
};

template <typename T>
AutoBuildParams
ParseAutoBuildParams(const BuildParams& params) {
    AssertInfo(params.is_object(), "AUTO build parameters must be an object");
    AutoBuildParams result;
    result.delegate_params = params;
    const auto limit = RequiredInteger(params, BITMAP_INDEX_CARDINALITY_LIMIT);
    if (limit <= 0 || limit > std::numeric_limits<int32_t>::max()) {
        ThrowInfo(DataTypeInvalid,
                  "AUTO bitmap cardinality limit {} is outside (0, {}]",
                  limit,
                  std::numeric_limits<int32_t>::max());
    }
    result.cardinality_limit = static_cast<int32_t>(limit);
    const auto field_type = ParseDataType(params, "field_type", true);
    result.value_type = ParseDataType(params, "value_type", true);
    result.element_type = ParseElementType(params);
    const auto nested = ParseNested(params);

    if (!CompatibleValueType<T>(result.value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "AUTO typed builder conflicts with value_type {}",
                  static_cast<int>(result.value_type));
    }
    if (field_type == DataType::ARRAY) {
        if (!IsSupportedArrayElement(result.element_type)) {
            ThrowInfo(DataTypeInvalid,
                      "AUTO does not support ARRAY element type {}",
                      static_cast<int>(result.element_type));
        }
        if (nested) {
            if constexpr (std::is_same_v<T, ArrayView>) {
                ThrowInfo(DataTypeInvalid,
                          "nested AUTO ARRAY requires an element builder");
            } else if (!CompatibleValueType<T>(result.element_type)) {
                ThrowInfo(DataTypeInvalid,
                          "nested AUTO value_type conflicts with ARRAY "
                          "element type");
            }
        } else if constexpr (!std::is_same_v<T, ArrayView>) {
            ThrowInfo(DataTypeInvalid,
                      "ordinary AUTO ARRAY requires an ArrayView builder");
        }
    } else {
        if (nested) {
            ThrowInfo(DataTypeInvalid,
                      "nested AUTO build requires ARRAY field_type");
        }
        if constexpr (std::is_same_v<T, ArrayView>) {
            ThrowInfo(DataTypeInvalid,
                      "ArrayView AUTO build requires ARRAY field_type");
        }
        if (!CompatibleValueType<T>(field_type)) {
            ThrowInfo(DataTypeInvalid,
                      "AUTO field_type conflicts with its typed builder");
        }
    }

    if (field_type == DataType::ARRAY) {
        result.low_cardinality_family = families::kBitmap;
        result.high_cardinality_family = families::kInverted;
        return result;
    }

    const auto version = OptionalInteger(params,
                                         SCALAR_INDEX_ENGINE_VERSION,
                                         kLastVersionWithoutHybridIndexConfig);
    if (version >= kHybridIndexConfigVersion) {
        result.low_cardinality_family =
            GetLowCardinalityFamilyFromConfig(params);
        result.high_cardinality_family =
            GetHighCardinalityFamilyFromConfig(params);
    } else {
        result.low_cardinality_family = families::kBitmap;
        if constexpr (std::is_same_v<T, std::string_view>) {
            result.high_cardinality_family = families::kInverted;
        } else if constexpr (std::is_integral_v<T>) {
            result.high_cardinality_family = families::kSort;
        } else {
            result.high_cardinality_family = families::kInverted;
        }
    }
    return result;
}

}  // namespace

template <typename T>
class AutoIndexBuilder<T>::Impl {
 public:
    explicit Impl(DataType element_type) : probe(element_type) {
    }

    CardinalityProbe<T> probe;
};

template <typename T>
AutoIndexBuilder<T>::AutoIndexBuilder(AutoBuildParams params)
    : params_(std::move(params)),
      active_spec_{.form = BuilderInputSpec::Streaming,
                   .needs_second_pass = true},
      impl_(std::make_unique<Impl>(params_.element_type)) {
    AssertInfo(params_.cardinality_limit > 0,
               "AUTO cardinality limit must be positive");
    AssertInfo(!params_.low_cardinality_family.empty() &&
                   !params_.high_cardinality_family.empty(),
               "AUTO family selection is not configured");
}

template <typename T>
AutoIndexBuilder<T>::~AutoIndexBuilder() = default;

template <typename T>
BuilderInputSpec
AutoIndexBuilder<T>::InputSpec() const {
    return active_spec_;
}

template <typename T>
void
AutoIndexBuilder<T>::Add(size_t n, const T* values, const bool* valid) {
    try {
        AssertInfo(state_ == State::Probe || state_ == State::Build,
                   "cannot add to a failed or consumed AUTO builder");
        AssertInfo(n == 0 || values != nullptr,
                   "AUTO Add received null values for {} rows",
                   n);
        if (state_ == State::Probe) {
            AssertInfo(!CurrentPassComplete(),
                       "AUTO probe received data after early completion");
            impl_->probe.Add(n,
                             values,
                             valid,
                             static_cast<size_t>(params_.cardinality_limit));
            return;
        }
        delegate_->Add(n, values, valid);
    } catch (...) {
        state_ = State::Failed;
        throw;
    }
}

template <typename T>
bool
AutoIndexBuilder<T>::CurrentPassComplete() const {
    AssertInfo(state_ == State::Probe || state_ == State::Build,
               "cannot inspect a failed or consumed AUTO builder");
    return state_ == State::Probe && impl_ != nullptr &&
           impl_->probe.Size() >=
               static_cast<size_t>(params_.cardinality_limit);
}

template <typename T>
void
AutoIndexBuilder<T>::FinishPass() {
    try {
        AssertInfo(state_ == State::Probe,
                   "AUTO probe pass can be finished exactly once");
        const auto family = SelectFamily(impl_->probe.Size());
        delegate_ = BuilderRegistry<T>::Instance().Create(
            family, params_.delegate_params);
        if (delegate_ == nullptr) {
            ThrowInfo(Unsupported,
                      "AUTO selected family {} has no builder for value type "
                      "{}",
                      family,
                      static_cast<int>(params_.value_type));
        }
        active_spec_ = delegate_->InputSpec();
        AssertInfo(!active_spec_.needs_second_pass,
                   "AUTO selected another multi-pass builder");
        selected_selector_ = static_cast<uint8_t>(SelectorForFamily(family));
        impl_.reset();
        state_ = State::Build;
    } catch (...) {
        state_ = State::Failed;
        throw;
    }
}

template <typename T>
std::string
AutoIndexBuilder<T>::SelectFamily(size_t distinct_count) const {
    return distinct_count >= static_cast<size_t>(params_.cardinality_limit)
               ? params_.high_cardinality_family
               : params_.low_cardinality_family;
}

template <typename T>
storage::ArtifactPtr
AutoIndexBuilder<T>::Seal() && {
    try {
        AssertInfo(state_ == State::Build,
                   "AUTO must finish its probe and build pass before Seal");
        state_ = State::Consumed;
        auto delegate = std::move(delegate_);
        auto artifact = std::move(*delegate).Seal();
        return std::make_unique<AutoIndexArtifact>(
            std::move(artifact),
            static_cast<ScalarIndexType>(selected_selector_));
    } catch (...) {
        state_ = State::Failed;
        throw;
    }
}

namespace {

template <typename T>
bool
RegisterAutoBuilder() {
    BuilderRegistry<T>::Instance().Register(
        families::kAuto, [](const BuildParams& params) {
            return std::make_unique<AutoIndexBuilder<T>>(
                ParseAutoBuildParams<T>(params));
        });
    return true;
}

const bool kRegistered =
    RegisterAutoBuilder<bool>() && RegisterAutoBuilder<int8_t>() &&
    RegisterAutoBuilder<int16_t>() && RegisterAutoBuilder<int32_t>() &&
    RegisterAutoBuilder<int64_t>() && RegisterAutoBuilder<float>() &&
    RegisterAutoBuilder<double>() && RegisterAutoBuilder<std::string_view>() &&
    RegisterAutoBuilder<ArrayView>();

}  // namespace

#define INSTANTIATE_AUTO_BUILDER(T) template class AutoIndexBuilder<T>;
INSTANTIATE_AUTO_BUILDER(bool)
INSTANTIATE_AUTO_BUILDER(int8_t)
INSTANTIATE_AUTO_BUILDER(int16_t)
INSTANTIATE_AUTO_BUILDER(int32_t)
INSTANTIATE_AUTO_BUILDER(int64_t)
INSTANTIATE_AUTO_BUILDER(float)
INSTANTIATE_AUTO_BUILDER(double)
INSTANTIATE_AUTO_BUILDER(std::string_view)
INSTANTIATE_AUTO_BUILDER(ArrayView)
#undef INSTANTIATE_AUTO_BUILDER

}  // namespace milvus::index
