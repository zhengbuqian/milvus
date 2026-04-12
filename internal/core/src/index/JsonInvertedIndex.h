// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once
#include <cstdint>
#include <string>
#include <unordered_map>

#include "common/JsonCastType.h"
#include "common/Types.h"
#include "index/JsonScalarIndexWrapper.h"
#include "index/InvertedIndexTantivy.h"
#include "log/Log.h"
#include "simdjson/error.h"

namespace milvus::index {

namespace json {
bool
IsDataTypeSupported(JsonCastType cast_type, DataType data_type, bool is_array);
}  // namespace json

// JsonInvertedIndex is now just a type alias for JsonScalarIndexWrapper
// wrapping InvertedIndexTantivy, the same way it wraps BitmapIndex or
// ScalarIndexSort. The wrapper handles JSON data extraction, EXISTS
// semantics, and both v2/v3 serialization formats.
template <typename T>
using JsonInvertedIndex = JsonScalarIndexWrapper<T, InvertedIndexTantivy<T>>;

// Error recorder used by NgramInvertedIndex for JSON parse error tracking.
class JsonInvertedIndexParseErrorRecorder {
 public:
    struct ErrorInstance {
        std::string json_str;
        std::string pointer;
    };
    struct ErrorStats {
        int64_t count;
        ErrorInstance first_instance;
    };
    void
    Record(const std::string_view& json_str,
           const std::string& pointer,
           const simdjson::error_code& error_code) {
        error_map_[error_code].count++;
        if (error_map_[error_code].count == 1) {
            error_map_[error_code].first_instance = {std::string(json_str),
                                                     pointer};
        }
    }

    void
    PrintErrStats() {
        if (error_map_.empty()) {
            LOG_INFO("No error found");
            return;
        }
        for (const auto& [error_code, stats] : error_map_) {
            LOG_INFO("Error code: {}, count: {}, first instance: {}",
                     error_message(error_code),
                     stats.count,
                     stats.first_instance.json_str);
        }
    }

    std::unordered_map<simdjson::error_code, ErrorStats>&
    GetErrorMap() {
        return error_map_;
    }

 private:
    std::unordered_map<simdjson::error_code, ErrorStats> error_map_;
};

}  // namespace milvus::index
