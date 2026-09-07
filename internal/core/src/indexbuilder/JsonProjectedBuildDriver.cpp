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

#include "indexbuilder/JsonProjectedBuildDriver.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "common/Array.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/JsonCastFunction.h"
#include "common/JsonCastType.h"
#include "common/JsonUtils.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/scalar/json/JsonProjectedIndexArtifact.h"
#include "index/scalar/ngram/JsonProjectedString.h"
#include "simdjson/dom/array.h"
#include "simdjson/dom/element.h"

namespace milvus::indexbuilder {
namespace {

constexpr size_t kChunkRows = 64 * 1024;

struct ProjectionParams {
    std::string json_path;
    std::vector<std::string> path_tokens;
    JsonCastType cast_type;
    JsonCastFunction cast_function;
};

enum class RowPresence {
    FieldNull,
    NoValue,
    Present,
};

std::string
ReadString(const index::BuildParams& params,
           std::string_view key,
           bool required) {
    if (!params.contains(key)) {
        if (required) {
            ThrowInfo(
                DataTypeInvalid, "typed JSON build requires parameter {}", key);
        }
        return {};
    }
    try {
        return params.at(key).get<std::string>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataTypeInvalid,
                  "typed JSON parameter {} must be a string: {}",
                  key,
                  error.what());
    }
}

JsonCastType
ParseCastType(const index::BuildParams& params) {
    const auto value = ReadString(params, JSON_CAST_TYPE, true);
    if (value != "BOOL" && value != "DOUBLE" && value != "VARCHAR" &&
        value != "ARRAY_BOOL" && value != "ARRAY_DOUBLE" &&
        value != "ARRAY_VARCHAR") {
        ThrowInfo(
            DataTypeInvalid, "unsupported typed JSON cast type {}", value);
    }
    return JsonCastType::FromString(value);
}

DataType
ValueTypeForCast(JsonCastType cast_type) {
    switch (cast_type.element_type()) {
        case JsonCastType::DataType::BOOL:
            return DataType::BOOL;
        case JsonCastType::DataType::DOUBLE:
            return DataType::DOUBLE;
        case JsonCastType::DataType::VARCHAR:
            return DataType::VARCHAR;
        default:
            ThrowInfo(
                DataTypeInvalid, "unsupported typed JSON cast {}", cast_type);
    }
}

int64_t
ReadDataType(const index::BuildParams& params, std::string_view key) {
    if (!params.contains(key)) {
        ThrowInfo(
            DataTypeInvalid, "typed JSON build requires parameter {}", key);
    }
    const auto& value = params.at(key);
    if (value.is_number_unsigned()) {
        const auto parsed = value.get<uint64_t>();
        if (parsed <=
            static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
            return static_cast<int64_t>(parsed);
        }
    } else if (value.is_number_integer()) {
        const auto parsed = value.get<int64_t>();
        if (parsed >= std::numeric_limits<int32_t>::min() &&
            parsed <= std::numeric_limits<int32_t>::max()) {
            return parsed;
        }
    }
    ThrowInfo(DataTypeInvalid,
              "typed JSON parameter {} is not a normalized data type",
              key);
}

bool
ReadBool(const index::BuildParams& params, std::string_view key) {
    try {
        if (!params.at(key).is_boolean()) {
            ThrowInfo(DataTypeInvalid,
                      "typed JSON parameter {} must be boolean",
                      key);
        }
        return params.at(key).get<bool>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataTypeInvalid,
                  "invalid typed JSON parameter {}: {}",
                  key,
                  error.what());
    }
}

ProjectionParams
ParseProjectionParams(DataType value_type,
                      const index::IndexFamily& family,
                      const index::BuildParams& params) {
    AssertInfo(params.is_object(),
               "typed JSON build parameters must be an object");
    if (static_cast<DataType>(ReadDataType(params, "field_type")) !=
        DataType::JSON) {
        ThrowInfo(DataTypeInvalid, "typed JSON build requires JSON field_type");
    }
    std::optional<bool> nested;
    for (const auto key : {std::string_view("nested"),
                           std::string_view("is_nested"),
                           std::string_view("is_nested_index")}) {
        if (!params.contains(key)) {
            continue;
        }
        const auto value = ReadBool(params, key);
        if (nested.has_value() && *nested != value) {
            ThrowInfo(DataTypeInvalid, "typed JSON nested parameters disagree");
        }
        nested = value;
    }
    if (!nested.has_value() || *nested) {
        ThrowInfo(DataTypeInvalid,
                  "typed JSON projection requires normalized row domain");
    }
    for (const auto key : {std::string_view("element_type"),
                           std::string_view("array_element_type")}) {
        if (params.contains(key) && static_cast<DataType>(ReadDataType(
                                        params, key)) != DataType::NONE) {
            ThrowInfo(DataTypeInvalid,
                      "typed JSON outer parameter {} must be NONE",
                      key);
        }
    }
    const auto cast_type = ParseCastType(params);
    if (value_type != ValueTypeForCast(cast_type) ||
        static_cast<DataType>(ReadDataType(params, "value_type")) !=
            value_type) {
        ThrowInfo(DataTypeInvalid,
                  "typed JSON value_type disagrees with cast {}",
                  cast_type);
    }
    const auto index_type = ReadString(params, index::INDEX_TYPE, true);
    const bool scalar = cast_type.data_type() != JsonCastType::DataType::ARRAY;
    bool family_supported = false;
    if (family == index::families::kAuto) {
        family_supported = index_type == index::HYBRID_INDEX_TYPE && scalar;
    } else if (family == index::families::kInverted) {
        family_supported = index_type == index::INVERTED_INDEX_TYPE;
    } else if (family == index::families::kSort) {
        family_supported =
            index_type == index::ASCENDING_SORT && scalar &&
            (value_type == DataType::DOUBLE || value_type == DataType::VARCHAR);
    } else if (family == index::families::kBitmap) {
        family_supported =
            index_type == index::BITMAP_INDEX_TYPE && scalar &&
            (value_type == DataType::BOOL || value_type == DataType::VARCHAR);
    } else if (family == index::families::kNgram) {
        family_supported = index_type == index::NGRAM_INDEX_TYPE && scalar &&
                           value_type == DataType::VARCHAR;
    }
    if (!family_supported ||
        (!scalar && family != index::families::kInverted)) {
        ThrowInfo(DataTypeInvalid,
                  "typed JSON cast {} is not supported by family {}",
                  cast_type,
                  family);
    }

    const bool has_json_path = params.contains(JSON_PATH);
    const bool has_nested_path = params.contains("nested_path");
    if (!has_json_path && !has_nested_path) {
        ThrowInfo(DataTypeInvalid, "typed JSON build requires json_path");
    }
    const auto json_path =
        has_json_path ? ReadString(params, JSON_PATH, true) : std::string{};
    const auto nested_path = has_nested_path
                                 ? ReadString(params, "nested_path", true)
                                 : std::string{};
    if (has_json_path && has_nested_path && json_path != nested_path) {
        ThrowInfo(DataTypeInvalid,
                  "typed JSON json_path and nested_path disagree");
    }
    auto path = has_json_path ? json_path : nested_path;
    static_cast<void>(index::JsonProjectedIndexSpec(path, cast_type, 0));
    auto path_tokens = parse_json_pointer(path);

    const auto cast_function_name =
        ReadString(params, JSON_CAST_FUNCTION, false);
    if (!cast_function_name.empty() &&
        (cast_function_name != "STRING_TO_DOUBLE" ||
         cast_type.data_type() != JsonCastType::DataType::DOUBLE)) {
        ThrowInfo(DataTypeInvalid,
                  "unsupported typed JSON cast function {} for {}",
                  cast_function_name,
                  cast_type);
    }
    return ProjectionParams{std::move(path),
                            std::move(path_tokens),
                            cast_type,
                            JsonCastFunction::FromString(cast_function_name)};
}

template <typename T>
std::unique_ptr<index::IndexBuilder<T>>
CreateBuilder(const index::IndexFamily& family,
              const index::BuildParams& params,
              DataType value_type) {
    auto builder = index::BuilderRegistry<T>::Instance().Create(family, params);
    if (builder == nullptr) {
        ThrowInfo(Unsupported,
                  "index family {} has no typed JSON builder for value type {}",
                  family,
                  static_cast<int>(value_type));
    }
    return builder;
}

index::BuildParams
MakeInnerParams(const index::IndexFamily& family,
                JsonCastType cast_type,
                DataType value_type,
                const index::BuildParams& outer) {
    auto inner = outer;
    if (family == index::families::kAuto) {
        inner["field_type"] = static_cast<int32_t>(value_type);
        inner["element_type"] = static_cast<int32_t>(DataType::NONE);
        inner["array_element_type"] = static_cast<int32_t>(DataType::NONE);
    } else if (cast_type.data_type() == JsonCastType::DataType::ARRAY) {
        inner["field_type"] = static_cast<int32_t>(DataType::ARRAY);
        inner["element_type"] = static_cast<int32_t>(value_type);
        inner["array_element_type"] = static_cast<int32_t>(value_type);
    }
    return inner;
}

class JsonBatch final {
 public:
    explicit JsonBatch(const FieldDataPtr& batch) : batch_(batch) {
        AssertInfo(batch_ != nullptr,
                   "typed JSON build received a null field-data batch");
        if (batch_->get_data_type() != DataType::JSON) {
            ThrowInfo(DataTypeInvalid,
                      "typed JSON driver received field-data type {}",
                      static_cast<int>(batch_->get_data_type()));
        }
        count_ = batch_->Length();
        documents_ = static_cast<const Json*>(batch_->Data());
        if (count_ != 0 && documents_ == nullptr) {
            ThrowInfo(DataFormatBroken,
                      "JSON field-data batch has null data for {} rows",
                      count_);
        }
        nullable_ = batch_->IsNullable();
        validity_ = nullable_ ? batch_->ValidData() : nullptr;
        if (nullable_ && count_ != 0 && validity_ == nullptr) {
            ThrowInfo(DataFormatBroken,
                      "nullable JSON field-data batch has no validity bitmap");
        }
    }

    size_t
    Count() const {
        return count_;
    }

    bool
    IsValid(size_t row) const {
        return !nullable_ ||
               ((validity_[row >> 3] >> static_cast<unsigned int>(row & 7)) &
                1U) != 0;
    }

    const Json&
    Document(size_t row) const {
        return documents_[row];
    }

 private:
    FieldDataPtr batch_;
    const Json* documents_{nullptr};
    const uint8_t* validity_{nullptr};
    size_t count_{0};
    bool nullable_{false};
};

RowPresence
Locate(const JsonBatch& batch, size_t row, const ProjectionParams& params) {
    if (!batch.IsValid(row)) {
        return RowPresence::FieldNull;
    }
    const auto& document = batch.Document(row);
    if (document.data().empty()) {
        ThrowInfo(
            DataFormatBroken, "valid JSON row {} has an empty document", row);
    }
    auto root = document.dom_doc();
    if (!path_exists(root.value(), params.path_tokens) ||
        !document.exist(params.json_path)) {
        return RowPresence::NoValue;
    }
    return RowPresence::Present;
}

template <typename T>
std::optional<T>
ExtractScalar(const Json& document, const ProjectionParams& params) {
    if constexpr (std::is_same_v<T, double>) {
        if (params.cast_function.match<double>()) {
            return JsonCastFunction::CastJsonValue<double>(
                params.cast_function, document, params.json_path);
        }
    }
    auto value = document.at<T>(params.json_path);
    if (value.error() != simdjson::SUCCESS) {
        return std::nullopt;
    }
    return value.value();
}

template <typename T>
class ProjectedDriverBase : public BuildDriver {
 public:
    ProjectedDriverBase(std::unique_ptr<index::IndexBuilder<T>> builder,
                        ProjectionParams params)
        : builder_(std::move(builder)), params_(std::move(params)) {
        AssertInfo(builder_ != nullptr,
                   "typed JSON driver cannot own a null builder");
        spec_ = builder_->InputSpec();
        collect_non_exist_ = !spec_.needs_second_pass;
    }

    const index::BuilderInputSpec&
    InputSpec() const override {
        return spec_;
    }

    void
    FinishPass() override {
        AssertOpen("finish pass on");
        AssertInfo(spec_.needs_second_pass,
                   "typed JSON FinishPass called on a one-pass builder");
        try {
            builder_->FinishPass();
            spec_ = builder_->InputSpec();
            AssertInfo(!spec_.needs_second_pass,
                       "typed JSON delegate requested another probe pass");
            row_count_ = 0;
            non_exist_offsets_.clear();
            collect_non_exist_ = true;
        } catch (...) {
            state_ = State::Failed;
            throw;
        }
    }

    void
    SetSourceFile(const std::string&) override {
        AssertOpen("set source file on");
        state_ = State::Failed;
        ThrowInfo(Unsupported,
                  "typed JSON projection requires decoded field data");
    }

    storage::ArtifactPtr
        Seal() &&
        override {
        AssertOpen("seal");
        AssertInfo(!spec_.needs_second_pass,
                   "typed JSON AUTO build must finish its probe pass");
        AssertInfo(row_count_ <=
                       static_cast<size_t>(std::numeric_limits<int64_t>::max()),
                   "typed JSON row count exceeds int64 domain");
        state_ = State::Consumed;
        auto builder = std::move(builder_);
        auto inner = std::move(*builder).Seal();
        return std::make_unique<index::JsonProjectedIndexArtifact>(
            std::move(inner),
            params_.json_path,
            params_.cast_type,
            static_cast<int64_t>(row_count_),
            std::move(non_exist_offsets_));
    }

 protected:
    const ProjectionParams&
    Params() const {
        return params_;
    }

    FeedControl
    AddProjected(size_t n,
                 const T* values,
                 const bool* valid,
                 const std::vector<size_t>& local_non_exist) {
        AssertOpen("feed");
        if (n > std::numeric_limits<size_t>::max() - row_count_) {
            ThrowInfo(DataFormatBroken,
                      "typed JSON row count overflows size_t");
        }
        for (const auto offset : local_non_exist) {
            if (offset >= n) {
                ThrowInfo(UnexpectedError,
                          "typed JSON local non-exist offset is out of range");
            }
        }
        try {
            if (collect_non_exist_) {
                if (local_non_exist.size() >
                    std::numeric_limits<size_t>::max() -
                        non_exist_offsets_.size()) {
                    ThrowInfo(DataFormatBroken,
                              "typed JSON non-exist offset count overflows");
                }
                for (const auto offset : local_non_exist) {
                    non_exist_offsets_.push_back(row_count_ + offset);
                }
            }
            builder_->Add(n, values, valid);
            row_count_ += n;
            return builder_->CurrentPassComplete() ? FeedControl::PassComplete
                                                   : FeedControl::Continue;
        } catch (...) {
            state_ = State::Failed;
            throw;
        }
    }

    void
    MarkFailed() noexcept {
        state_ = State::Failed;
    }

 private:
    enum class State {
        Open,
        Failed,
        Consumed,
    };

    void
    AssertOpen(const char* operation) const {
        AssertInfo(state_ == State::Open,
                   "cannot {} a {} typed JSON driver",
                   operation,
                   state_ == State::Failed ? "failed" : "consumed");
    }

    std::unique_ptr<index::IndexBuilder<T>> builder_;
    ProjectionParams params_;
    index::BuilderInputSpec spec_;
    std::vector<size_t> non_exist_offsets_;
    size_t row_count_{0};
    State state_{State::Open};
    bool collect_non_exist_{false};
};

template <typename T>
class ScalarProjectedDriver final : public ProjectedDriverBase<T> {
 public:
    using ProjectedDriverBase<T>::ProjectedDriverBase;

    FeedControl
    Feed(const FieldDataPtr& input) override {
        try {
            const JsonBatch batch(input);
            for (size_t begin = 0; begin < batch.Count(); begin += kChunkRows) {
                const auto count = std::min(kChunkRows, batch.Count() - begin);
                values_.resize(count);
                if (count > valid_capacity_) {
                    valid_ = std::make_unique<bool[]>(count);
                    valid_capacity_ = count;
                }
                local_non_exist_.clear();
                for (size_t i = 0; i < count; ++i) {
                    const auto row = begin + i;
                    const auto presence = Locate(batch, row, this->Params());
                    if (presence != RowPresence::Present) {
                        values_[i] = T{};
                        valid_[i] = false;
                        local_non_exist_.push_back(i);
                        continue;
                    }
                    const auto value =
                        ExtractScalar<T>(batch.Document(row), this->Params());
                    values_[i] = value.value_or(T{});
                    valid_[i] = value.has_value();
                }
                const auto control = this->AddProjected(
                    count, values_.data(), valid_.get(), local_non_exist_);
                if (control == FeedControl::PassComplete) {
                    return control;
                }
            }
            if (batch.Count() == 0) {
                return this->AddProjected(0, nullptr, nullptr, {});
            }
            return FeedControl::Continue;
        } catch (...) {
            this->MarkFailed();
            throw;
        }
    }

 private:
    std::vector<T> values_;
    std::unique_ptr<bool[]> valid_;
    size_t valid_capacity_{0};
    std::vector<size_t> local_non_exist_;
};

template <>
class ScalarProjectedDriver<bool> final : public ProjectedDriverBase<bool> {
 public:
    using ProjectedDriverBase<bool>::ProjectedDriverBase;

    FeedControl
    Feed(const FieldDataPtr& input) override {
        try {
            const JsonBatch batch(input);
            for (size_t begin = 0; begin < batch.Count(); begin += kChunkRows) {
                const auto count = std::min(kChunkRows, batch.Count() - begin);
                EnsureCapacity(count);
                local_non_exist_.clear();
                for (size_t i = 0; i < count; ++i) {
                    const auto row = begin + i;
                    const auto presence = Locate(batch, row, Params());
                    if (presence != RowPresence::Present) {
                        values_[i] = false;
                        valid_[i] = false;
                        local_non_exist_.push_back(i);
                        continue;
                    }
                    const auto value =
                        ExtractScalar<bool>(batch.Document(row), Params());
                    values_[i] = value.value_or(false);
                    valid_[i] = value.has_value();
                }
                const auto control = AddProjected(
                    count, values_.get(), valid_.get(), local_non_exist_);
                if (control == FeedControl::PassComplete) {
                    return control;
                }
            }
            if (batch.Count() == 0) {
                return AddProjected(0, nullptr, nullptr, {});
            }
            return FeedControl::Continue;
        } catch (...) {
            MarkFailed();
            throw;
        }
    }

 private:
    void
    EnsureCapacity(size_t count) {
        if (count <= capacity_) {
            return;
        }
        values_ = std::make_unique<bool[]>(count);
        valid_ = std::make_unique<bool[]>(count);
        capacity_ = count;
    }

    std::unique_ptr<bool[]> values_;
    std::unique_ptr<bool[]> valid_;
    size_t capacity_{0};
    std::vector<size_t> local_non_exist_;
};

template <>
class ScalarProjectedDriver<std::string_view> final
    : public ProjectedDriverBase<std::string_view> {
 public:
    using ProjectedDriverBase<std::string_view>::ProjectedDriverBase;

    FeedControl
    Feed(const FieldDataPtr& input) override {
        try {
            const JsonBatch batch(input);
            for (size_t begin = 0; begin < batch.Count(); begin += kChunkRows) {
                const auto count = std::min(kChunkRows, batch.Count() - begin);
                values_.resize(count);
                owning_values_.resize(count);
                if (count > valid_capacity_) {
                    valid_ = std::make_unique<bool[]>(count);
                    valid_capacity_ = count;
                }
                local_non_exist_.clear();
                for (size_t i = 0; i < count; ++i) {
                    const auto row = begin + i;
                    const auto presence = Locate(batch, row, Params());
                    if (presence != RowPresence::Present) {
                        owning_values_[i].clear();
                        values_[i] = {};
                        valid_[i] = false;
                        local_non_exist_.push_back(i);
                        continue;
                    }
                    const auto value = ExtractScalar<std::string_view>(
                        batch.Document(row), Params());
                    if (!value.has_value()) {
                        owning_values_[i].clear();
                        values_[i] = {};
                        valid_[i] = false;
                        continue;
                    }
                    owning_values_[i].assign(value->data(), value->size());
                    values_[i] = owning_values_[i];
                    valid_[i] = true;
                }
                const auto control = AddProjected(
                    count, values_.data(), valid_.get(), local_non_exist_);
                if (control == FeedControl::PassComplete) {
                    return control;
                }
            }
            if (batch.Count() == 0) {
                return AddProjected(0, nullptr, nullptr, {});
            }
            return FeedControl::Continue;
        } catch (...) {
            MarkFailed();
            throw;
        }
    }

 private:
    std::vector<std::string_view> values_;
    std::vector<std::string> owning_values_;
    std::unique_ptr<bool[]> valid_;
    size_t valid_capacity_{0};
    std::vector<size_t> local_non_exist_;
};

class NgramProjectedDriver final
    : public ProjectedDriverBase<index::JsonProjectedString> {
 public:
    using ProjectedDriverBase<index::JsonProjectedString>::ProjectedDriverBase;

    FeedControl
    Feed(const FieldDataPtr& input) override {
        try {
            const JsonBatch batch(input);
            for (size_t begin = 0; begin < batch.Count(); begin += kChunkRows) {
                const auto count = std::min(kChunkRows, batch.Count() - begin);
                values_.resize(count);
                owning_values_.resize(count);
                local_non_exist_.clear();
                for (size_t i = 0; i < count; ++i) {
                    const auto row = begin + i;
                    const auto presence = Locate(batch, row, Params());
                    if (presence == RowPresence::FieldNull) {
                        owning_values_[i].clear();
                        values_[i] = {
                            {}, index::JsonProjectedStringState::FieldNull};
                        local_non_exist_.push_back(i);
                        continue;
                    }
                    if (presence == RowPresence::NoValue) {
                        owning_values_[i].clear();
                        values_[i] = {{},
                                      index::JsonProjectedStringState::NoValue};
                        local_non_exist_.push_back(i);
                        continue;
                    }
                    const auto value = ExtractScalar<std::string_view>(
                        batch.Document(row), Params());
                    if (value.has_value()) {
                        owning_values_[i].assign(value->data(), value->size());
                        values_[i] = {owning_values_[i],
                                      index::JsonProjectedStringState::Value};
                    } else {
                        owning_values_[i].clear();
                        values_[i] = {{},
                                      index::JsonProjectedStringState::NoValue};
                    }
                }
                const auto control = AddProjected(
                    count, values_.data(), nullptr, local_non_exist_);
                if (control == FeedControl::PassComplete) {
                    return control;
                }
            }
            if (batch.Count() == 0) {
                return AddProjected(0, nullptr, nullptr, {});
            }
            return FeedControl::Continue;
        } catch (...) {
            MarkFailed();
            throw;
        }
    }

 private:
    std::vector<index::JsonProjectedString> values_;
    std::vector<std::string> owning_values_;
    std::vector<size_t> local_non_exist_;
};

class ArrayProjectedDriver final : public ProjectedDriverBase<ArrayView> {
 public:
    ArrayProjectedDriver(
        std::unique_ptr<index::IndexBuilder<ArrayView>> builder,
        ProjectionParams params,
        DataType element_type)
        : ProjectedDriverBase<ArrayView>(std::move(builder), std::move(params)),
          element_type_(element_type) {
    }

    FeedControl
    Feed(const FieldDataPtr& input) override {
        try {
            const JsonBatch batch(input);
            for (size_t row = 0; row < batch.Count(); ++row) {
                const auto presence = Locate(batch, row, Params());
                const bool field_null = presence == RowPresence::FieldNull;
                const bool non_exist = presence != RowPresence::Present;
                ResetScratch();
                local_non_exist_.clear();
                if (presence == RowPresence::Present) {
                    ExtractArray(batch.Document(row));
                }
                auto view = MakeView();
                const bool valid = !field_null;
                if (non_exist) {
                    local_non_exist_.push_back(0);
                }
                const auto control =
                    AddProjected(1, &view, &valid, local_non_exist_);
                if (control == FeedControl::PassComplete) {
                    return control;
                }
            }
            if (batch.Count() == 0) {
                return AddProjected(0, nullptr, nullptr, {});
            }
            return FeedControl::Continue;
        } catch (...) {
            MarkFailed();
            throw;
        }
    }

 private:
    void
    ResetScratch() {
        bool_values_.clear();
        double_values_.clear();
        string_bytes_.clear();
        string_offsets_.clear();
    }

    void
    ExtractArray(const Json& document) {
        auto root = document.dom_doc();
        auto array = root.value().at_pointer(Params().json_path).get_array();
        if (array.error() != simdjson::SUCCESS) {
            return;
        }
        for (const auto element : array.value()) {
            if (element_type_ == DataType::BOOL) {
                auto value = element.get<bool>();
                if (value.error() == simdjson::SUCCESS) {
                    bool_values_.push_back(value.value() ? 1 : 0);
                }
            } else if (element_type_ == DataType::DOUBLE) {
                auto value = element.get<double>();
                if (value.error() == simdjson::SUCCESS) {
                    double_values_.push_back(value.value());
                }
            } else {
                auto value = element.get<std::string_view>();
                if (value.error() != simdjson::SUCCESS) {
                    continue;
                }
                if (string_bytes_.size() >
                    std::numeric_limits<uint32_t>::max()) {
                    ThrowInfo(DataFormatBroken,
                              "typed JSON string ARRAY exceeds uint32 offset "
                              "domain");
                }
                string_offsets_.push_back(
                    static_cast<uint32_t>(string_bytes_.size()));
                if (value.value().size() >
                    std::numeric_limits<uint32_t>::max() -
                        string_bytes_.size()) {
                    ThrowInfo(DataFormatBroken,
                              "typed JSON string ARRAY exceeds uint32 byte "
                              "domain");
                }
                string_bytes_.append(value.value());
            }
        }
    }

    ArrayView
    MakeView() {
        if (element_type_ == DataType::BOOL) {
            if (bool_values_.size() >
                static_cast<size_t>(std::numeric_limits<int>::max())) {
                ThrowInfo(DataFormatBroken,
                          "typed JSON BOOL ARRAY exceeds int length domain");
            }
            return ArrayView(bool_values_.empty()
                                 ? &empty_data_
                                 : reinterpret_cast<char*>(bool_values_.data()),
                             static_cast<int>(bool_values_.size()),
                             bool_values_.size(),
                             DataType::BOOL,
                             nullptr);
        }
        if (element_type_ == DataType::DOUBLE) {
            if (double_values_.size() >
                    static_cast<size_t>(std::numeric_limits<int>::max()) ||
                double_values_.size() >
                    std::numeric_limits<size_t>::max() / sizeof(double)) {
                ThrowInfo(DataFormatBroken,
                          "typed JSON DOUBLE ARRAY exceeds size domain");
            }
            return ArrayView(
                double_values_.empty()
                    ? &empty_data_
                    : reinterpret_cast<char*>(double_values_.data()),
                static_cast<int>(double_values_.size()),
                double_values_.size() * sizeof(double),
                DataType::DOUBLE,
                nullptr);
        }
        if (string_offsets_.size() >
            static_cast<size_t>(std::numeric_limits<int>::max())) {
            ThrowInfo(DataFormatBroken,
                      "typed JSON VARCHAR ARRAY exceeds int length domain");
        }
        return ArrayView(
            string_bytes_.empty() ? &empty_data_ : string_bytes_.data(),
            static_cast<int>(string_offsets_.size()),
            string_bytes_.size(),
            DataType::VARCHAR,
            string_offsets_.empty() ? &empty_offset_ : string_offsets_.data());
    }

    DataType element_type_{DataType::NONE};
    std::vector<uint8_t> bool_values_;
    std::vector<double> double_values_;
    std::string string_bytes_;
    std::vector<uint32_t> string_offsets_;
    std::vector<size_t> local_non_exist_;
    char empty_data_{0};
    uint32_t empty_offset_{0};
};

}  // namespace

BuildDriverPtr
MakeJsonProjectedBuildDriver(DataType value_type,
                             const index::IndexFamily& family,
                             const index::BuildParams& params) {
    auto projection = ParseProjectionParams(value_type, family, params);
    auto inner_params =
        MakeInnerParams(family, projection.cast_type, value_type, params);
    if (projection.cast_type.data_type() == JsonCastType::DataType::ARRAY) {
        return std::make_unique<ArrayProjectedDriver>(
            CreateBuilder<ArrayView>(family, inner_params, value_type),
            std::move(projection),
            value_type);
    }
    if (family == index::families::kNgram) {
        return std::make_unique<NgramProjectedDriver>(
            CreateBuilder<index::JsonProjectedString>(
                family, inner_params, value_type),
            std::move(projection));
    }
    switch (value_type) {
        case DataType::BOOL:
            return std::make_unique<ScalarProjectedDriver<bool>>(
                CreateBuilder<bool>(family, inner_params, value_type),
                std::move(projection));
        case DataType::DOUBLE:
            return std::make_unique<ScalarProjectedDriver<double>>(
                CreateBuilder<double>(family, inner_params, value_type),
                std::move(projection));
        case DataType::VARCHAR:
            return std::make_unique<ScalarProjectedDriver<std::string_view>>(
                CreateBuilder<std::string_view>(
                    family, inner_params, value_type),
                std::move(projection));
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported typed JSON value type {}",
                      static_cast<int>(value_type));
    }
}

}  // namespace milvus::indexbuilder
