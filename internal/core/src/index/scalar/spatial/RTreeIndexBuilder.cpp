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

#include "index/scalar/spatial/RTreeIndexBuilder.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cctype>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/spatial/RTreeIndexArtifact.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

std::string
CreateStagingDirectory(const std::string& configured_parent) {
    std::error_code error;
    const auto parent = configured_parent.empty()
                            ? std::filesystem::temp_directory_path(error)
                            : std::filesystem::path(configured_parent);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to locate temporary directory for R-Tree build: {}",
                  error.message());
    }
    std::filesystem::create_directories(parent, error);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to create R-Tree staging parent {}: {}",
                  parent.string(),
                  error.message());
    }
    auto pattern = (parent / "milvus-rtree-XXXXXX").string();
    std::vector<char> path(pattern.begin(), pattern.end());
    path.push_back('\0');
    if (::mkdtemp(path.data()) == nullptr) {
        ThrowInfo(FileCreateFailed,
                  "failed to create unique R-Tree staging directory under {}: "
                  "{}",
                  parent.string(),
                  std::strerror(errno));
    }
    try {
        return std::string(path.data());
    } catch (...) {
        std::error_code ignored;
        std::filesystem::remove_all(path.data(), ignored);
        throw;
    }
}

void
RemoveDirectory(const std::string& path) {
    if (path.empty()) {
        return;
    }
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
}

DataType
ParseDataTypeValue(const nlohmann::json& value, std::string_view key) {
    int64_t numeric = 0;
    if (value.is_number_unsigned()) {
        const auto unsigned_numeric = value.get<uint64_t>();
        if (unsigned_numeric >
            static_cast<uint64_t>(std::numeric_limits<int>::max())) {
            ThrowInfo(DataTypeInvalid,
                      "R-Tree parameter {} is not a valid data type",
                      key);
        }
        numeric = static_cast<int64_t>(unsigned_numeric);
    } else if (value.is_number_integer()) {
        numeric = value.get<int64_t>();
    } else if (value.is_string()) {
        const auto text = value.get<std::string>();
        if (text == "GEOMETRY") {
            return DataType::GEOMETRY;
        }
        if (text == "NONE") {
            return DataType::NONE;
        }
        const auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), numeric);
        if (error != std::errc() || end != text.data() + text.size()) {
            ThrowInfo(DataTypeInvalid,
                      "unsupported R-Tree data type {} for parameter {}",
                      text,
                      key);
        }
    } else {
        ThrowInfo(
            DataTypeInvalid, "R-Tree parameter {} must be a data type", key);
    }
    if (numeric < std::numeric_limits<int>::min() ||
        numeric > std::numeric_limits<int>::max()) {
        ThrowInfo(DataTypeInvalid,
                  "R-Tree parameter {} is not a valid data type",
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
        const auto numeric = value.get<uint64_t>();
        if (numeric == 0 || numeric == 1) {
            return numeric != 0;
        }
    } else if (value.is_number_integer()) {
        const auto numeric = value.get<int64_t>();
        if (numeric == 0 || numeric == 1) {
            return numeric != 0;
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
    ThrowInfo(DataTypeInvalid, "R-Tree parameter {} must be a boolean", key);
}

void
ValidateGeometryParams(const Config& params) {
    for (const auto key :
         {std::string_view("field_type"), std::string_view("value_type")}) {
        const auto type = ReadDataType(params, key);
        if (type.has_value() && *type != DataType::GEOMETRY) {
            ThrowInfo(DataTypeInvalid,
                      "R-Tree parameter {} must be GEOMETRY, got {}",
                      key,
                      static_cast<int>(*type));
        }
    }
    for (const auto key : {std::string_view("array_element_type"),
                           std::string_view("element_type")}) {
        const auto type = ReadDataType(params, key);
        if (type.has_value() && *type != DataType::NONE) {
            ThrowInfo(DataTypeInvalid,
                      "R-Tree parameter {} must be NONE, got {}",
                      key,
                      static_cast<int>(*type));
        }
    }

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
                      "R-Tree nested parameters {} and {} disagree",
                      first_key,
                      key);
        }
        if (!nested.has_value()) {
            nested = value;
            first_key = key;
        }
    }
    if (nested.value_or(false)) {
        ThrowInfo(DataTypeInvalid, "R-Tree does not support nested input");
    }
}

RTreeBuildParams
ParseBuildParams(const BuildParams& params) {
    ValidateGeometryParams(params);
    RTreeBuildParams result;
    if (params.contains("local_dir") && !params.at("local_dir").is_null()) {
        if (!params.at("local_dir").is_string()) {
            ThrowInfo(DataTypeInvalid,
                      "R-Tree parameter local_dir must be a string");
        }
        result.local_dir = params.at("local_dir").get<std::string>();
    }
    return result;
}

}  // namespace

RTreeIndexBuilder::RTreeIndexBuilder(RTreeBuildParams params)
    : params_(std::move(params)) {
    params_.local_dir = CreateStagingDirectory(params_.local_dir);
    owns_local_dir_ = true;
    try {
        engine_ = std::make_unique<RTreeBuildEngine>(
            (std::filesystem::path(params_.local_dir) / "index_file").string());
    } catch (...) {
        RemoveDirectory(params_.local_dir);
        owns_local_dir_ = false;
        throw;
    }
}

RTreeIndexBuilder::~RTreeIndexBuilder() {
    engine_.reset();
    if (owns_local_dir_) {
        RemoveDirectory(params_.local_dir);
    }
}

BuilderInputSpec
RTreeIndexBuilder::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Contiguous,
                            .needs_second_pass = false};
}

void
RTreeIndexBuilder::Add(size_t n,
                       const std::string_view* values,
                       const bool* valid) {
    AssertInfo(!sealed_, "R-Tree builder cannot Add after Seal");
    AssertInfo(!failed_, "R-Tree builder cannot Add after a failed Add");
    AssertInfo(n == 0 || values != nullptr,
               "R-Tree builder received null values with non-zero count");
    if (n > static_cast<size_t>(std::numeric_limits<int64_t>::max()) ||
        total_num_rows_ >
            std::numeric_limits<int64_t>::max() - static_cast<int64_t>(n)) {
        ThrowInfo(DataTypeInvalid,
                  "R-Tree coordinate count {} + {} exceeds int64 domain",
                  total_num_rows_,
                  n);
    }

    try {
        for (size_t i = 0; i < n; ++i) {
            const auto row = total_num_rows_ + static_cast<int64_t>(i);
            if (valid != nullptr && !valid[i]) {
                null_offsets_.push_back(static_cast<size_t>(row));
                continue;
            }
            engine_->AddGeometry(
                reinterpret_cast<const uint8_t*>(values[i].data()),
                values[i].size(),
                row);
        }
        total_num_rows_ += static_cast<int64_t>(n);
    } catch (...) {
        failed_ = true;
        throw;
    }
}

storage::ArtifactPtr
RTreeIndexBuilder::Seal() && {
    AssertInfo(!sealed_, "R-Tree builder cannot Seal more than once");
    AssertInfo(!failed_, "R-Tree builder cannot Seal after a failed Add");
    if (total_num_rows_ == 0) {
        ThrowInfo(DataIsEmpty, "R-Tree index cannot build empty input");
    }
    sealed_ = true;
    engine_->Finish();
    auto artifact = std::make_unique<RTreeIndexArtifact>(
        params_.local_dir, std::move(null_offsets_), total_num_rows_, true);
    owns_local_dir_ = false;
    engine_.reset();
    return artifact;
}

namespace {

const bool kRTreeBuilderRegistered = [] {
    BuilderRegistry<std::string_view>::Instance().Register(
        families::kRTree, [](const BuildParams& params) {
            return std::make_unique<RTreeIndexBuilder>(
                ParseBuildParams(params));
        });
    return true;
}();

}  // namespace

}  // namespace milvus::index
