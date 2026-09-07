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

#include "indexbuilder/BuildDriver.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <limits>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/Array.h"
#include "common/EasyAssert.h"
#include "index/Families.h"
#include "indexbuilder/JsonProjectedBuildDriver.h"
#include "indexbuilder/VectorBuildDriver.h"

namespace milvus::indexbuilder {

namespace {

bool
IsStringType(DataType type) {
    return type == DataType::STRING || type == DataType::VARCHAR ||
           type == DataType::TEXT;
}

bool
CompatibleType(DataType actual, DataType expected) {
    return actual == expected ||
           (IsStringType(actual) && IsStringType(expected)) ||
           ((actual == DataType::INT64 || actual == DataType::TIMESTAMPTZ) &&
            (expected == DataType::INT64 || expected == DataType::TIMESTAMPTZ));
}

bool
CompatibleArrayElementType(DataType actual, DataType expected) {
    return CompatibleType(actual, expected) ||
           (actual == DataType::INT32 &&
            (expected == DataType::INT8 || expected == DataType::INT16));
}

std::optional<DataType>
DataTypeByName(std::string name) {
    std::transform(name.begin(), name.end(), name.begin(), [](unsigned char c) {
        return static_cast<char>(std::toupper(c));
    });
    if (name == "BOOL") {
        return DataType::BOOL;
    }
    if (name == "INT8") {
        return DataType::INT8;
    }
    if (name == "INT16") {
        return DataType::INT16;
    }
    if (name == "INT32") {
        return DataType::INT32;
    }
    if (name == "INT64") {
        return DataType::INT64;
    }
    if (name == "FLOAT") {
        return DataType::FLOAT;
    }
    if (name == "DOUBLE") {
        return DataType::DOUBLE;
    }
    if (name == "STRING") {
        return DataType::STRING;
    }
    if (name == "VARCHAR") {
        return DataType::VARCHAR;
    }
    if (name == "TEXT") {
        return DataType::TEXT;
    }
    if (name == "ARRAY") {
        return DataType::ARRAY;
    }
    if (name == "JSON") {
        return DataType::JSON;
    }
    if (name == "GEOMETRY") {
        return DataType::GEOMETRY;
    }
    if (name == "TIMESTAMPTZ") {
        return DataType::TIMESTAMPTZ;
    }
    if (name == "VECTOR_BINARY") {
        return DataType::VECTOR_BINARY;
    }
    if (name == "VECTOR_FLOAT") {
        return DataType::VECTOR_FLOAT;
    }
    if (name == "VECTOR_FLOAT16") {
        return DataType::VECTOR_FLOAT16;
    }
    if (name == "VECTOR_BFLOAT16") {
        return DataType::VECTOR_BFLOAT16;
    }
    if (name == "VECTOR_SPARSE_U32_F32") {
        return DataType::VECTOR_SPARSE_U32_F32;
    }
    if (name == "VECTOR_INT8") {
        return DataType::VECTOR_INT8;
    }
    if (name == "VECTOR_ARRAY") {
        return DataType::VECTOR_ARRAY;
    }
    return std::nullopt;
}

DataType
ParseDataTypeValue(const nlohmann::json& encoded, std::string_view key) {
    try {
        int64_t numeric;
        if (encoded.is_number_unsigned()) {
            const auto value = encoded.get<uint64_t>();
            if (value >
                static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
                ThrowInfo(DataTypeInvalid,
                          "build-driver data type {} is out of range",
                          key);
            }
            numeric = static_cast<int64_t>(value);
        } else if (encoded.is_number_integer()) {
            numeric = encoded.get<int64_t>();
        } else if (encoded.is_string()) {
            auto text = encoded.get<std::string>();
            if (const auto named = DataTypeByName(text); named.has_value()) {
                return *named;
            }
            const auto* begin = text.data();
            const auto* end = begin + text.size();
            const auto [parsed_end, ec] = std::from_chars(begin, end, numeric);
            if (text.empty() || ec != std::errc{} || parsed_end != end) {
                ThrowInfo(DataTypeInvalid,
                          "build-driver parameter {} is not a data type: {}",
                          key,
                          text);
            }
        } else {
            ThrowInfo(DataTypeInvalid,
                      "build-driver parameter {} must be a data type",
                      key);
        }
        if (numeric < std::numeric_limits<int32_t>::min() ||
            numeric > std::numeric_limits<int32_t>::max()) {
            ThrowInfo(DataTypeInvalid,
                      "build-driver data type {} is out of range",
                      key);
        }
        return static_cast<DataType>(static_cast<int32_t>(numeric));
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataTypeInvalid,
                  "invalid build-driver data type in {}: {}",
                  key,
                  error.what());
    }
}

std::optional<DataType>
ReadDataType(const index::BuildParams& params, std::string_view key) {
    if (!params.contains(key) || params.at(key).is_null()) {
        return std::nullopt;
    }
    return ParseDataTypeValue(params.at(key), key);
}

std::optional<std::string>
ReadString(const index::BuildParams& params, std::string_view key) {
    if (!params.contains(key) || params.at(key).is_null()) {
        return std::nullopt;
    }
    if (!params.at(key).is_string()) {
        ThrowInfo(
            DataTypeInvalid, "build-driver parameter {} must be a string", key);
    }
    try {
        return params.at(key).get<std::string>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataTypeInvalid,
                  "invalid build-driver string in {}: {}",
                  key,
                  error.what());
    }
}

bool
ParseBoolValue(const nlohmann::json& encoded, std::string_view key) {
    try {
        if (encoded.is_boolean()) {
            return encoded.get<bool>();
        }
        if (encoded.is_number_unsigned()) {
            const auto value = encoded.get<uint64_t>();
            if (value == 0 || value == 1) {
                return value != 0;
            }
        } else if (encoded.is_number_integer()) {
            const auto value = encoded.get<int64_t>();
            if (value == 0 || value == 1) {
                return value != 0;
            }
        }
        if (encoded.is_string()) {
            auto text = encoded.get<std::string>();
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
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataTypeInvalid,
                  "invalid build-driver boolean in {}: {}",
                  key,
                  error.what());
    }
    ThrowInfo(
        DataTypeInvalid, "build-driver parameter {} must be boolean", key);
}

bool
ParseNested(const index::BuildParams& params) {
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
                      "build-driver nested parameters {} and {} disagree",
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
ResolveArrayElementType(const index::BuildParams& params) {
    auto current = ReadDataType(params, "array_element_type");
    auto legacy = ReadDataType(params, "element_type");
    if (current == DataType::NONE) {
        current.reset();
    }
    if (legacy == DataType::NONE) {
        legacy.reset();
    }
    if (current.has_value() && legacy.has_value() &&
        !CompatibleType(*current, *legacy)) {
        ThrowInfo(DataTypeInvalid,
                  "build-driver array_element_type {} conflicts with "
                  "element_type {}",
                  static_cast<int>(*current),
                  static_cast<int>(*legacy));
    }
    return current.value_or(legacy.value_or(DataType::NONE));
}

bool
IsSupportedScalarType(DataType type) {
    return type == DataType::BOOL || type == DataType::INT8 ||
           type == DataType::INT16 || type == DataType::INT32 ||
           type == DataType::INT64 || type == DataType::TIMESTAMPTZ ||
           type == DataType::FLOAT || type == DataType::DOUBLE ||
           IsStringType(type) || type == DataType::GEOMETRY;
}

size_t
FixedArrayElementSize(DataType type) {
    switch (type) {
        case DataType::BOOL:
            return sizeof(bool);
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
            return sizeof(int32_t);
        case DataType::INT64:
            return sizeof(int64_t);
        case DataType::FLOAT:
            return sizeof(float);
        case DataType::DOUBLE:
            return sizeof(double);
        default:
            return 0;
    }
}

void
ValidateArrayRow(const Array& array,
                 DataType expected_type,
                 size_t row,
                 bool validate_narrow_values) {
    if (array.length() < 0) {
        ThrowInfo(DataFormatBroken,
                  "ARRAY row {} has negative element count {}",
                  row,
                  array.length());
    }
    if (!CompatibleArrayElementType(array.get_element_type(), expected_type)) {
        ThrowInfo(DataTypeInvalid,
                  "ARRAY row {} element type {} does not match expected {}",
                  row,
                  static_cast<int>(array.get_element_type()),
                  static_cast<int>(expected_type));
    }
    const auto count = static_cast<size_t>(array.length());
    if (count == 0) {
        return;
    }
    if (array.data() == nullptr) {
        ThrowInfo(DataFormatBroken,
                  "ARRAY row {} has null data for {} elements",
                  row,
                  count);
    }
    if (IsVariableDataType(expected_type)) {
        const auto* offsets = array.get_offsets_data();
        if (offsets == nullptr) {
            ThrowInfo(DataFormatBroken,
                      "ARRAY row {} has no offsets for variable-width values",
                      row);
        }
        size_t previous = 0;
        for (size_t i = 0; i < count; ++i) {
            const auto offset = static_cast<size_t>(offsets[i]);
            if (offset < previous || offset > array.byte_size()) {
                ThrowInfo(DataFormatBroken,
                          "ARRAY row {} has invalid element offset {} at {}",
                          row,
                          offset,
                          i);
            }
            previous = offset;
        }
    } else {
        const auto width = FixedArrayElementSize(expected_type);
        if (width == 0 || count > std::numeric_limits<size_t>::max() / width ||
            array.byte_size() < count * width) {
            ThrowInfo(DataFormatBroken,
                      "ARRAY row {} has invalid byte size {} for {} elements",
                      row,
                      array.byte_size(),
                      count);
        }
    }
    if (validate_narrow_values &&
        (expected_type == DataType::INT8 || expected_type == DataType::INT16)) {
        const auto min = expected_type == DataType::INT8
                             ? std::numeric_limits<int8_t>::min()
                             : std::numeric_limits<int16_t>::min();
        const auto max = expected_type == DataType::INT8
                             ? std::numeric_limits<int8_t>::max()
                             : std::numeric_limits<int16_t>::max();
        for (size_t i = 0; i < count; ++i) {
            const auto value = array.get_data<int32_t>(static_cast<int>(i));
            if (value < min || value > max) {
                ThrowInfo(DataFormatBroken,
                          "ARRAY row {} value {} at {} exceeds {} range",
                          row,
                          value,
                          i,
                          expected_type == DataType::INT8 ? "INT8" : "INT16");
            }
        }
    }
}

template <typename T>
std::unique_ptr<index::IndexBuilder<T>>
CreateBuilder(const index::IndexFamily& family,
              const index::BuildParams& params,
              DataType value_type) {
    auto builder = index::BuilderRegistry<T>::Instance().Create(family, params);
    if (builder == nullptr) {
        ThrowInfo(Unsupported,
                  "index family {} has no scalar builder for value type {}",
                  family,
                  static_cast<int>(value_type));
    }
    return builder;
}

class JsonFlatBuildDriver final : public TypedBuildDriver<std::string_view> {
 public:
    explicit JsonFlatBuildDriver(
        std::unique_ptr<index::IndexBuilder<std::string_view>> builder)
        : TypedBuildDriver<std::string_view>(std::move(builder),
                                             DataType::JSON),
          validity_(std::make_unique<bool[]>(kChunkRows)) {
        views_.reserve(kChunkRows);
    }

    FeedControl
    Feed(const FieldDataPtr& batch) override {
        try {
            ValidateFeed(batch);
            const auto count = batch->Length();
            if (count == 0) {
                return AddProjected(0, nullptr, nullptr);
            }

            const auto* documents = static_cast<const Json*>(batch->Data());
            if (documents == nullptr) {
                ThrowInfo(DataFormatBroken,
                          "JSON field-data batch has null data for {} rows",
                          count);
            }
            const auto nullable = batch->IsNullable();
            const auto* packed_validity =
                nullable ? batch->ValidData() : nullptr;
            if (nullable && packed_validity == nullptr) {
                ThrowInfo(DataFormatBroken,
                          "nullable JSON field-data batch has no validity "
                          "bitmap");
            }

            for (size_t begin = 0; begin < count; begin += kChunkRows) {
                const auto chunk = std::min(kChunkRows, count - begin);
                views_.resize(chunk);
                for (size_t i = 0; i < chunk; ++i) {
                    const auto row = begin + i;
                    const bool valid =
                        !nullable || ((packed_validity[row >> 3] >>
                                       static_cast<unsigned>(row & 7)) &
                                      1U) != 0;
                    validity_[i] = valid;
                    views_[i] =
                        valid ? documents[row].data() : std::string_view{};
                }
                const auto control = AddProjected(
                    chunk, views_.data(), nullable ? validity_.get() : nullptr);
                if (control == FeedControl::PassComplete) {
                    return control;
                }
            }
            return FeedControl::Continue;
        } catch (...) {
            MarkFailed();
            throw;
        }
    }

 private:
    static constexpr size_t kChunkRows = 64 * 1024;

    std::vector<std::string_view> views_;
    std::unique_ptr<bool[]> validity_;
};

template <typename T>
class NestedArrayBuildDriver final : public TypedBuildDriver<T> {
 public:
    NestedArrayBuildDriver(std::unique_ptr<index::IndexBuilder<T>> builder,
                           DataType element_type)
        : TypedBuildDriver<T>(
              std::move(builder), DataType::ARRAY, element_type) {
    }

    FeedControl
    Feed(const FieldDataPtr& batch) override {
        this->ValidateFeed(batch);
        const auto row_count = batch->Length();
        const auto* valid = this->UnpackValidity(batch);
        const auto* arrays = static_cast<const Array*>(batch->Data());
        if (row_count != 0 && arrays == nullptr) {
            ThrowInfo(DataFormatBroken,
                      "nested ARRAY batch has null row data for {} rows",
                      row_count);
        }

        for (size_t row = 0; row < row_count; ++row) {
            if (valid != nullptr && !valid[row]) {
                continue;
            }
            ValidateArrayRow(arrays[row], this->ArrayElementType(), row, true);
        }

        constexpr size_t kProbeChunkElements = 64 * 1024;
        if constexpr (std::is_same_v<T, bool>) {
            auto flattened = std::make_unique<bool[]>(kProbeChunkElements);
            size_t buffered = 0;
            for (size_t row = 0; row < row_count; ++row) {
                if (valid != nullptr && !valid[row]) {
                    continue;
                }
                for (int i = 0; i < arrays[row].length(); ++i) {
                    flattened[buffered++] = arrays[row].get_data<bool>(i);
                    if (buffered == kProbeChunkElements) {
                        if (this->AddProjected(
                                buffered, flattened.get(), nullptr) ==
                            FeedControl::PassComplete) {
                            return FeedControl::PassComplete;
                        }
                        buffered = 0;
                    }
                }
            }
            if (buffered != 0) {
                return this->AddProjected(buffered, flattened.get(), nullptr);
            }
        } else {
            std::vector<T> flattened;
            flattened.reserve(kProbeChunkElements);
            for (size_t row = 0; row < row_count; ++row) {
                if (valid != nullptr && !valid[row]) {
                    continue;
                }
                for (int i = 0; i < arrays[row].length(); ++i) {
                    if constexpr (std::is_same_v<T, int8_t> ||
                                  std::is_same_v<T, int16_t>) {
                        flattened.push_back(
                            static_cast<T>(arrays[row].get_data<int32_t>(i)));
                    } else {
                        flattened.push_back(arrays[row].get_data<T>(i));
                    }
                    if (flattened.size() == kProbeChunkElements) {
                        if (this->AddProjected(
                                flattened.size(), flattened.data(), nullptr) ==
                            FeedControl::PassComplete) {
                            return FeedControl::PassComplete;
                        }
                        flattened.clear();
                    }
                }
            }
            if (!flattened.empty()) {
                return this->AddProjected(
                    flattened.size(), flattened.data(), nullptr);
            }
        }
        return this->AddProjected(0, nullptr, nullptr);
    }
};

template <typename T>
BuildDriverPtr
MakeScalarDriver(DataType source_type,
                 DataType value_type,
                 const index::IndexFamily& family,
                 const index::BuildParams& params) {
    return std::make_unique<TypedBuildDriver<T>>(
        CreateBuilder<T>(family, params, value_type), source_type);
}

template <typename T>
BuildDriverPtr
MakeNestedDriver(DataType value_type,
                 const index::IndexFamily& family,
                 const index::BuildParams& params) {
    return std::make_unique<NestedArrayBuildDriver<T>>(
        CreateBuilder<T>(family, params, value_type), value_type);
}

}  // namespace

template <typename T>
TypedBuildDriver<T>::TypedBuildDriver(
    std::unique_ptr<index::IndexBuilder<T>> builder,
    DataType source_type,
    DataType array_element_type)
    : builder_(std::move(builder)),
      source_type_(source_type),
      array_element_type_(array_element_type) {
    AssertInfo(builder_ != nullptr, "build driver cannot own a null builder");
    spec_ = builder_->InputSpec();
}

template <typename T>
const index::BuilderInputSpec&
TypedBuildDriver<T>::InputSpec() const {
    return spec_;
}

template <typename T>
void
TypedBuildDriver<T>::AssertOpen(const char* operation) const {
    AssertInfo(state_ == State::Open,
               "cannot {} a {} build driver",
               operation,
               state_ == State::Failed ? "failed" : "consumed");
}

template <typename T>
void
TypedBuildDriver<T>::ValidateFeed(const FieldDataPtr& batch) const {
    AssertOpen("feed");
    AssertInfo(!source_set_, "cannot Feed after SetSourceFile");
    AssertInfo(spec_.form != index::BuilderInputSpec::LocalFile,
               "LocalFile build driver must be fed with SetSourceFile");
    AssertInfo(batch != nullptr,
               "build driver received a null field-data batch");
    if (!CompatibleType(batch->get_data_type(), source_type_)) {
        ThrowInfo(DataTypeInvalid,
                  "field-data type {} does not match build-driver source type "
                  "{}",
                  static_cast<int>(batch->get_data_type()),
                  static_cast<int>(source_type_));
    }
}

template <typename T>
const bool*
TypedBuildDriver<T>::UnpackValidity(const FieldDataPtr& batch) {
    const auto count = batch->Length();
    if (!batch->IsNullable() || count == 0) {
        return nullptr;
    }
    const auto* packed = batch->ValidData();
    if (packed == nullptr) {
        ThrowInfo(DataFormatBroken,
                  "nullable field-data batch has no validity bitmap");
    }
    if (count > validity_capacity_) {
        validity_scratch_ = std::make_unique<bool[]>(count);
        validity_capacity_ = count;
    }
    for (size_t i = 0; i < count; ++i) {
        validity_scratch_[i] =
            ((packed[i >> 3] >> static_cast<unsigned>(i & 7)) & 1U) != 0;
    }
    return validity_scratch_.get();
}

template <typename T>
void
TypedBuildDriver<T>::MarkFailed() noexcept {
    state_ = State::Failed;
}

template <typename T>
FeedControl
TypedBuildDriver<T>::AddProjected(size_t n,
                                  const T* values,
                                  const bool* valid) {
    AssertOpen("feed");
    try {
        builder_->Add(n, values, valid);
        fed_ = true;
        return builder_->CurrentPassComplete() ? FeedControl::PassComplete
                                               : FeedControl::Continue;
    } catch (...) {
        state_ = State::Failed;
        throw;
    }
}

template <typename T>
FeedControl
TypedBuildDriver<T>::Feed(const FieldDataPtr& batch) {
    ValidateFeed(batch);
    const auto count = batch->Length();
    const auto* valid = UnpackValidity(batch);

    if constexpr (std::is_same_v<T, std::string_view>) {
        const auto* strings = static_cast<const std::string*>(batch->Data());
        if (count != 0 && strings == nullptr) {
            ThrowInfo(DataFormatBroken,
                      "string field-data batch has null data for {} rows",
                      count);
        }
        std::vector<std::string_view> views;
        views.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            views.emplace_back(strings[i].data(), strings[i].size());
        }
        return AddProjected(views.size(), views.data(), valid);
    } else if constexpr (std::is_same_v<T, ArrayView>) {
        const auto* arrays = static_cast<const Array*>(batch->Data());
        if (count != 0 && arrays == nullptr) {
            ThrowInfo(DataFormatBroken,
                      "ARRAY field-data batch has null data for {} rows",
                      count);
        }
        for (size_t row = 0; row < count; ++row) {
            if (valid == nullptr || valid[row]) {
                ValidateArrayRow(arrays[row], array_element_type_, row, true);
            }
        }

        char empty_data = 0;
        uint32_t empty_offset = 0;
        std::vector<ArrayView> views;
        views.reserve(count);
        for (size_t row = 0; row < count; ++row) {
            const auto is_valid = valid == nullptr || valid[row];
            const auto is_empty = !is_valid || arrays[row].length() == 0;
            auto* data =
                is_empty ? &empty_data : const_cast<char*>(arrays[row].data());
            auto* offsets = is_empty && IsVariableDataType(array_element_type_)
                                ? &empty_offset
                                : arrays[row].get_offsets_data();
            views.emplace_back(data,
                               is_empty ? 0 : arrays[row].length(),
                               is_empty ? 0 : arrays[row].byte_size(),
                               array_element_type_,
                               offsets);
        }
        return AddProjected(views.size(), views.data(), valid);
    } else {
        const auto* values = static_cast<const T*>(batch->Data());
        if (count != 0 && values == nullptr) {
            ThrowInfo(DataFormatBroken,
                      "field-data batch has null data for {} rows",
                      count);
        }
        return AddProjected(count, values, valid);
    }
}

template <typename T>
void
TypedBuildDriver<T>::FinishPass() {
    AssertOpen("finish pass on");
    AssertInfo(spec_.needs_second_pass,
               "FinishPass called after the build pass was selected");
    try {
        builder_->FinishPass();
        spec_ = builder_->InputSpec();
    } catch (...) {
        state_ = State::Failed;
        throw;
    }
}

template <typename T>
void
TypedBuildDriver<T>::SetSourceFile(const std::string& path) {
    AssertOpen("set source file on");
    AssertInfo(spec_.form == index::BuilderInputSpec::LocalFile,
               "SetSourceFile is only valid for LocalFile build drivers");
    AssertInfo(!fed_, "cannot SetSourceFile after Feed");
    AssertInfo(!source_set_, "build driver source file is already set");
    AssertInfo(!path.empty(), "build driver source file path is empty");
    try {
        builder_->SetSourceFile(path);
        MarkSourceSet();
    } catch (...) {
        state_ = State::Failed;
        throw;
    }
}

template <typename T>
void
TypedBuildDriver<T>::MarkSourceSet() {
    AssertOpen("mark source set on");
    AssertInfo(spec_.form == index::BuilderInputSpec::LocalFile,
               "source state is only valid for LocalFile build drivers");
    AssertInfo(!fed_, "cannot mark a source after Feed");
    AssertInfo(!source_set_, "build driver source is already set");
    source_set_ = true;
}

template <typename T>
storage::ArtifactPtr
TypedBuildDriver<T>::Seal() && {
    AssertOpen("seal");
    if (spec_.form == index::BuilderInputSpec::LocalFile) {
        AssertInfo(source_set_, "LocalFile build driver has no source file");
    }
    state_ = State::Consumed;
    auto builder = std::move(builder_);
    return std::move(*builder).Seal();
}

BuildDriverPtr
MakeBuildDriver(DataType value_type,
                const index::IndexFamily& family,
                const index::BuildParams& params) {
    if (!params.is_object()) {
        ThrowInfo(DataTypeInvalid, "build-driver parameters must be an object");
    }
    const auto field_type = ReadDataType(params, "field_type");
    if (!field_type.has_value() || *field_type == DataType::NONE) {
        ThrowInfo(DataTypeInvalid,
                  "build-driver requires normalized field_type");
    }
    if (IsVectorDataType(*field_type)) {
        return MakeVectorBuildDriver(*field_type, value_type, family, params);
    }

    const auto configured_value = ReadDataType(params, "value_type");
    if (*field_type == DataType::JSON) {
        if (ParseNested(params)) {
            ThrowInfo(DataTypeInvalid,
                      "JSON BuildDriver supports only the row coordinate "
                      "domain");
        }
        const auto cast = ReadString(params, "json_cast_type");
        const bool flat = family == index::families::kJsonFlat &&
                          value_type == DataType::JSON;
        if (flat && configured_value.has_value() &&
            *configured_value == DataType::JSON && cast.has_value() &&
            *cast == "JSON") {
            return std::make_unique<JsonFlatBuildDriver>(
                CreateBuilder<std::string_view>(family, params, value_type));
        }
        if (family == index::families::kJsonFlat ||
            value_type == DataType::JSON || !configured_value.has_value()) {
            ThrowInfo(DataTypeInvalid,
                      "JSON flat BuildDriver requires JSON value_type and "
                      "json_cast_type JSON");
        }
        return MakeJsonProjectedBuildDriver(value_type, family, params);
    }
    if (configured_value.has_value() && *configured_value != DataType::NONE &&
        !CompatibleType(*configured_value, value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "build-driver value type {} conflicts with configured {}",
                  static_cast<int>(value_type),
                  static_cast<int>(*configured_value));
    }
    const auto nested = ParseNested(params);
    const auto element_type = ResolveArrayElementType(params);

    if (*field_type == DataType::ARRAY) {
        if (element_type == DataType::NONE ||
            !IsSupportedScalarType(element_type) ||
            element_type == DataType::GEOMETRY ||
            element_type == DataType::TIMESTAMPTZ) {
            ThrowInfo(DataTypeInvalid,
                      "unsupported ARRAY element type {}",
                      static_cast<int>(element_type));
        }
        if (!nested) {
            if (value_type != DataType::ARRAY) {
                ThrowInfo(DataTypeInvalid,
                          "ordinary ARRAY build requires ArrayView value type");
            }
            return std::make_unique<TypedBuildDriver<ArrayView>>(
                CreateBuilder<ArrayView>(family, params, element_type),
                DataType::ARRAY,
                element_type);
        }
        if (!CompatibleType(value_type, element_type)) {
            ThrowInfo(DataTypeInvalid,
                      "nested ARRAY value type {} conflicts with element type "
                      "{}",
                      static_cast<int>(value_type),
                      static_cast<int>(element_type));
        }
        switch (value_type) {
            case DataType::BOOL:
                return MakeNestedDriver<bool>(value_type, family, params);
            case DataType::INT8:
                return MakeNestedDriver<int8_t>(value_type, family, params);
            case DataType::INT16:
                return MakeNestedDriver<int16_t>(value_type, family, params);
            case DataType::INT32:
                return MakeNestedDriver<int32_t>(value_type, family, params);
            case DataType::INT64:
                return MakeNestedDriver<int64_t>(value_type, family, params);
            case DataType::FLOAT:
                return MakeNestedDriver<float>(value_type, family, params);
            case DataType::DOUBLE:
                return MakeNestedDriver<double>(value_type, family, params);
            case DataType::STRING:
            case DataType::VARCHAR:
            case DataType::TEXT:
                return MakeNestedDriver<std::string_view>(
                    value_type, family, params);
            default:
                ThrowInfo(
                    Unsupported,
                    "nested ARRAY BuildDriver does not support value type "
                    "{}",
                    static_cast<int>(value_type));
        }
    }

    if (nested) {
        ThrowInfo(DataTypeInvalid,
                  "nested build input requires ARRAY field_type");
    }
    if (!IsSupportedScalarType(*field_type) ||
        !CompatibleType(*field_type, value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "field type {} does not match scalar build value type {}",
                  static_cast<int>(*field_type),
                  static_cast<int>(value_type));
    }

    switch (value_type) {
        case DataType::BOOL:
            return MakeScalarDriver<bool>(
                *field_type, value_type, family, params);
        case DataType::INT8:
            return MakeScalarDriver<int8_t>(
                *field_type, value_type, family, params);
        case DataType::INT16:
            return MakeScalarDriver<int16_t>(
                *field_type, value_type, family, params);
        case DataType::INT32:
            return MakeScalarDriver<int32_t>(
                *field_type, value_type, family, params);
        case DataType::INT64:
            return MakeScalarDriver<int64_t>(
                *field_type, value_type, family, params);
        case DataType::FLOAT:
            return MakeScalarDriver<float>(
                *field_type, value_type, family, params);
        case DataType::DOUBLE:
            return MakeScalarDriver<double>(
                *field_type, value_type, family, params);
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
        case DataType::GEOMETRY:
            return MakeScalarDriver<std::string_view>(
                *field_type, value_type, family, params);
        default:
            ThrowInfo(Unsupported,
                      "scalar BuildDriver does not support value type {}",
                      static_cast<int>(value_type));
    }
}

#define INSTANTIATE_TYPED_BUILD_DRIVER(T) template class TypedBuildDriver<T>;
INSTANTIATE_TYPED_BUILD_DRIVER(bool)
INSTANTIATE_TYPED_BUILD_DRIVER(int8_t)
INSTANTIATE_TYPED_BUILD_DRIVER(int16_t)
INSTANTIATE_TYPED_BUILD_DRIVER(int32_t)
INSTANTIATE_TYPED_BUILD_DRIVER(int64_t)
INSTANTIATE_TYPED_BUILD_DRIVER(float)
INSTANTIATE_TYPED_BUILD_DRIVER(double)
INSTANTIATE_TYPED_BUILD_DRIVER(std::string_view)
INSTANTIATE_TYPED_BUILD_DRIVER(ArrayView)
INSTANTIATE_TYPED_BUILD_DRIVER(bin1)
INSTANTIATE_TYPED_BUILD_DRIVER(float16)
INSTANTIATE_TYPED_BUILD_DRIVER(bfloat16)
INSTANTIATE_TYPED_BUILD_DRIVER(sparse_u32_f32)
#undef INSTANTIATE_TYPED_BUILD_DRIVER

}  // namespace milvus::indexbuilder
