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

#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>

#include "common/EasyAssert.h"
#include "nlohmann/json.hpp"

namespace milvus::index {

// Missing/null handling and rejection diagnostics belong to the caller.
// This decoder accepts only booleans and the exact strings "true"/"false".
inline std::optional<bool>
TryParseBooleanLiteral(const nlohmann::json& value) {
    if (value.is_boolean()) {
        return value.get<bool>();
    }
    if (value.is_string()) {
        const auto text = value.get<std::string>();
        if (text == "true") {
            return true;
        }
        if (text == "false") {
            return false;
        }
    }
    return std::nullopt;
}

// The broader scalar encoding also accepts integer 0/1, strings "0"/"1",
// and case-insensitive boolean strings. It does not accept floating point.
inline std::optional<bool>
TryParseBooleanWithNumericStrings(const nlohmann::json& value) {
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
    return std::nullopt;
}

// These string parameters default only when absent, not when explicitly null.
inline std::string
ReadStringParam(const nlohmann::json& params,
                std::string_view key,
                std::string fallback,
                std::string_view context) {
    if (!params.is_object() || !params.contains(key)) {
        return fallback;
    }
    try {
        return params.at(key).get<std::string>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataTypeInvalid,
                  "invalid {} parameter {}: {}",
                  context,
                  key,
                  error.what());
    }
}

inline constexpr std::initializer_list<std::string_view> kNestedParamKeys = {
    "nested", "is_nested", "is_nested_index"};

// The caller controls decoding and whether null counts as missing. Visit keys
// in order, keeping the first supplied value and its diagnostic key. Defaulting
// and required-value checks stay with the caller. Callables are not type-erased.
template <typename T, typename Read, typename OnConflict>
std::optional<T>
ReadAliasedParam(std::initializer_list<std::string_view> keys,
                 Read&& read,
                 OnConflict&& on_conflict) {
    std::optional<T> result;
    std::string_view first_key;
    for (const auto key : keys) {
        const auto value = read(key);
        if (!value.has_value()) {
            continue;
        }
        if (result.has_value() && *result != *value) {
            on_conflict(first_key, key);
        }
        if (!result.has_value()) {
            result = *value;
            first_key = key;
        }
    }
    return result;
}

template <typename Read>
std::optional<bool>
ReadNestedParam(Read&& read, std::string_view context) {
    return ReadAliasedParam<bool>(
        kNestedParamKeys, read, [context](auto first, auto second) {
            ThrowInfo(DataTypeInvalid,
                      "{} nested parameters {} and {} disagree",
                      context,
                      first,
                      second);
        });
}

}  // namespace milvus::index
