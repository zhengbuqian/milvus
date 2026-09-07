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

#include "index/scalar/bitmap/BitmapIndexBuilder.h"

#include <limits>
#include <utility>

#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/bitmap/BitmapIndexArtifact.h"

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
    return type == DataType::STRING || type == DataType::VARCHAR;
}

bool
CompatibleArrayType(DataType actual, DataType expected) {
    return actual == expected ||
           (IsStringType(actual) && IsStringType(expected));
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
            DataTypeInvalid, "bitmap parameter {} must be a data type", key);
    }
    const auto text = value.get<std::string>();
    try {
        size_t parsed = 0;
        const auto numeric = std::stoi(text, &parsed);
        if (parsed == text.size()) {
            return static_cast<DataType>(numeric);
        }
    } catch (const std::exception&) {
        // Continue with the human-readable spellings used by local callers.
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
        {"ARRAY", DataType::ARRAY},
    };
    auto it = kNames.find(text);
    if (it == kNames.end()) {
        ThrowInfo(DataTypeInvalid,
                  "unsupported bitmap data type {} for parameter {}",
                  text,
                  key);
    }
    return it->second;
}

BitmapBuildParams
ParseBuildParams(const Config& params, DataType default_type, bool array) {
    BitmapBuildParams parsed;
    parsed.nested =
        GetValueFromConfig<bool>(params, "nested")
            .value_or(GetValueFromConfig<bool>(params, "is_nested_index")
                          .value_or(false));
    parsed.nullable =
        GetValueFromConfig<bool>(params, "nullable").value_or(false);
    parsed.offset_cache =
        GetValueFromConfig<bool>(params, ENABLE_OFFSET_CACHE).value_or(false);

    const auto field_type = ParseDataType(
        params, "field_type", array ? DataType::ARRAY : default_type);
    const auto array_element_type =
        ParseDataType(params, "array_element_type", DataType::NONE);
    const auto configured_value_type =
        ParseDataType(params, "value_type", DataType::NONE);
    auto value_type =
        field_type == DataType::ARRAY && array_element_type != DataType::NONE
            ? array_element_type
            : configured_value_type;
    if (value_type == DataType::NONE || value_type == DataType::ARRAY) {
        value_type = field_type == DataType::ARRAY ? default_type : field_type;
    }
    parsed.value_type = value_type;
    parsed.value_lookup = field_type != DataType::ARRAY || parsed.nested;
    return parsed;
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

void
CheckAppend(size_t current, size_t count) {
    constexpr uint64_t kMaxCoordinateCount =
        static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;
    if (count > std::numeric_limits<size_t>::max() - current ||
        current > kMaxCoordinateCount ||
        count > kMaxCoordinateCount - current) {
        ThrowInfo(DataTypeInvalid,
                  "bitmap coordinate count {} + {} exceeds Roaring's uint32 "
                  "domain",
                  current,
                  count);
    }
}

template <typename T>
bitmap_stored_t<T>
OwnValue(const T& value) {
    if constexpr (std::is_same_v<T, std::string_view>) {
        return std::string(value);
    } else {
        return value;
    }
}

}  // namespace

template <typename T>
BitmapIndexBuilder<T>::BitmapIndexBuilder(BitmapBuildParams params)
    : params_(std::move(params)) {
    if (params_.value_type == DataType::NONE ||
        params_.value_type == DataType::ARRAY) {
        params_.value_type = CppDataType<T>();
    }
}

template <typename T>
BitmapIndexBuilder<T>::~BitmapIndexBuilder() = default;

template <typename T>
BuilderInputSpec
BitmapIndexBuilder<T>::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Contiguous,
                            .needs_second_pass = false};
}

template <typename T>
void
BitmapIndexBuilder<T>::Add(size_t n, const T* values, const bool* valid) {
    AssertInfo(n == 0 || values != nullptr,
               "bitmap builder received null values with non-zero count");
    CheckAppend(total_num_rows_, n);
    validity_.reserve(total_num_rows_ + n);
    for (size_t i = 0; i < n; ++i) {
        const auto coordinate = total_num_rows_ + i;

        // Nested callers already removed null/empty source rows and flattened
        // elements. Every supplied item therefore owns one valid element
        // coordinate; a source-row validity array must not create holes here.
        const bool is_valid = params_.nested || valid == nullptr || valid[i];
        validity_.push_back(is_valid ? 1 : 0);
        if (is_valid) {
            postings_[OwnValue(values[i])].add(
                static_cast<uint32_t>(coordinate));
        }
    }
    total_num_rows_ += n;
}

template <typename T>
storage::ArtifactPtr
BitmapIndexBuilder<T>::Seal() && {
    if (total_num_rows_ == 0) {
        ThrowInfo(DataIsEmpty, "bitmap index cannot build empty input");
    }
    return std::make_unique<BitmapIndexArtifact<StoredT>>(
        std::move(postings_),
        MakeValidity(validity_),
        total_num_rows_,
        params_.value_type,
        params_.nested,
        params_.nullable,
        params_.value_lookup,
        params_.offset_cache);
}

template <typename T>
size_t
BitmapIndexBuilder<T>::DistinctCount() const {
    return postings_.size();
}

class BitmapArrayIndexBuilder::Impl {
 public:
    virtual ~Impl() = default;

    virtual void
    AddRow(const ArrayView& value, uint32_t coordinate) = 0;

    virtual size_t
    DistinctCount() const = 0;

    virtual storage::ArtifactPtr
    Seal(TargetBitmap validity,
         size_t count,
         const BitmapBuildParams& params) = 0;
};

namespace {

template <typename T>
class BitmapArrayBuilderImpl final : public BitmapArrayIndexBuilder::Impl {
 public:
    void
    AddRow(const ArrayView& value, uint32_t coordinate) override {
        for (int i = 0; i < value.length(); ++i) {
            postings_[OwnValue(value.get_data<T>(i))].add(coordinate);
        }
    }

    size_t
    DistinctCount() const override {
        return postings_.size();
    }

    storage::ArtifactPtr
    Seal(TargetBitmap validity,
         size_t count,
         const BitmapBuildParams& params) override {
        return std::make_unique<BitmapIndexArtifact<bitmap_stored_t<T>>>(
            std::move(postings_),
            std::move(validity),
            count,
            params.value_type,
            false,
            params.nullable,
            false,
            false);
    }

 private:
    std::map<bitmap_stored_t<T>, roaring::Roaring> postings_;
};

std::unique_ptr<BitmapArrayIndexBuilder::Impl>
MakeArrayImpl(DataType element_type) {
    switch (element_type) {
        case DataType::BOOL:
            return std::make_unique<BitmapArrayBuilderImpl<bool>>();
        case DataType::INT8:
            return std::make_unique<BitmapArrayBuilderImpl<int8_t>>();
        case DataType::INT16:
            return std::make_unique<BitmapArrayBuilderImpl<int16_t>>();
        case DataType::INT32:
            return std::make_unique<BitmapArrayBuilderImpl<int32_t>>();
        case DataType::INT64:
            return std::make_unique<BitmapArrayBuilderImpl<int64_t>>();
        case DataType::FLOAT:
            return std::make_unique<BitmapArrayBuilderImpl<float>>();
        case DataType::DOUBLE:
            return std::make_unique<BitmapArrayBuilderImpl<double>>();
        case DataType::STRING:
        case DataType::VARCHAR:
            return std::make_unique<BitmapArrayBuilderImpl<std::string_view>>();
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported ARRAY element type {} for bitmap index",
                      static_cast<int>(element_type));
    }
}

}  // namespace

BitmapArrayIndexBuilder::BitmapArrayIndexBuilder(BitmapBuildParams params)
    : params_(std::move(params)), impl_(MakeArrayImpl(params_.value_type)) {
    AssertInfo(!params_.nested,
               "nested ARRAY bitmap input must be flattened into typed "
               "elements");
    params_.value_lookup = false;
}

BitmapArrayIndexBuilder::~BitmapArrayIndexBuilder() = default;

BuilderInputSpec
BitmapArrayIndexBuilder::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Contiguous,
                            .needs_second_pass = false};
}

void
BitmapArrayIndexBuilder::Add(size_t n,
                             const ArrayView* values,
                             const bool* valid) {
    AssertInfo(n == 0 || values != nullptr,
               "bitmap ARRAY builder received null values with non-zero "
               "count");
    CheckAppend(total_num_rows_, n);
    validity_.reserve(total_num_rows_ + n);
    for (size_t i = 0; i < n; ++i) {
        const auto coordinate = total_num_rows_ + i;
        const bool is_valid = valid == nullptr || valid[i];
        validity_.push_back(is_valid ? 1 : 0);
        if (!is_valid) {
            continue;
        }
        AssertInfo(CompatibleArrayType(values[i].get_element_type(),
                                       params_.value_type),
                   "ARRAY element type {} does not match bitmap type {}",
                   static_cast<int>(values[i].get_element_type()),
                   static_cast<int>(params_.value_type));
        // All elements use the same row coordinate. Roaring de-duplicates
        // repeated values within the row, while different values retain their
        // separate postings. A valid empty row reaches here, adds no posting,
        // but remains valid and still advances the coordinate below.
        impl_->AddRow(values[i], static_cast<uint32_t>(coordinate));
    }
    total_num_rows_ += n;
}

storage::ArtifactPtr
BitmapArrayIndexBuilder::Seal() && {
    if (total_num_rows_ == 0) {
        ThrowInfo(DataIsEmpty, "bitmap ARRAY index cannot build empty input");
    }
    return impl_->Seal(MakeValidity(validity_), total_num_rows_, params_);
}

size_t
BitmapArrayIndexBuilder::DistinctCount() const {
    return impl_->DistinctCount();
}

namespace {

template <typename T>
bool
RegisterBitmapBuilder() {
    BuilderRegistry<T>::Instance().Register(
        families::kBitmap, [](const BuildParams& params) {
            return std::make_unique<BitmapIndexBuilder<T>>(
                ParseBuildParams(params, CppDataType<T>(), false));
        });
    return true;
}

bool
RegisterBitmapArrayBuilder() {
    BuilderRegistry<ArrayView>::Instance().Register(
        families::kBitmap, [](const BuildParams& params) {
            auto parsed = ParseBuildParams(params, DataType::NONE, true);
            if (parsed.value_type == DataType::NONE ||
                parsed.value_type == DataType::ARRAY) {
                ThrowInfo(DataTypeInvalid,
                          "bitmap ARRAY builder requires array_element_type");
            }
            return std::make_unique<BitmapArrayIndexBuilder>(std::move(parsed));
        });
    return true;
}

const bool kRegistered =
    RegisterBitmapBuilder<bool>() && RegisterBitmapBuilder<int8_t>() &&
    RegisterBitmapBuilder<int16_t>() && RegisterBitmapBuilder<int32_t>() &&
    RegisterBitmapBuilder<int64_t>() && RegisterBitmapBuilder<float>() &&
    RegisterBitmapBuilder<double>() &&
    RegisterBitmapBuilder<std::string_view>() && RegisterBitmapArrayBuilder();

}  // namespace

#define INSTANTIATE_BITMAP_BUILDER(T) template class BitmapIndexBuilder<T>;
INSTANTIATE_BITMAP_BUILDER(bool)
INSTANTIATE_BITMAP_BUILDER(int8_t)
INSTANTIATE_BITMAP_BUILDER(int16_t)
INSTANTIATE_BITMAP_BUILDER(int32_t)
INSTANTIATE_BITMAP_BUILDER(int64_t)
INSTANTIATE_BITMAP_BUILDER(float)
INSTANTIATE_BITMAP_BUILDER(double)
INSTANTIATE_BITMAP_BUILDER(std::string_view)
#undef INSTANTIATE_BITMAP_BUILDER

}  // namespace milvus::index
