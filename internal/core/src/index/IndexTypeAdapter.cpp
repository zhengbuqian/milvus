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

#include "index/IndexTypeAdapter.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <limits>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/JsonCastType.h"
#include "fmt/format.h"
#include "index/Meta.h"
#include "index/vector/VectorLoadResource.h"
#include "storage/artifact/FileSource.h"

namespace milvus::index {

namespace {

bool
ConfigBool(const Config& config, const std::string& key, bool fallback) {
    if (!config.contains(key)) {
        return fallback;
    }
    const auto& value = config.at(key);
    if (value.is_boolean()) {
        return value.get<bool>();
    }
    if (value.is_string()) {
        auto text = value.get<std::string>();
        std::transform(text.begin(), text.end(), text.begin(), [](char c) {
            return static_cast<char>(
                std::tolower(static_cast<unsigned char>(c)));
        });
        return text == "true";
    }
    return value.get<int64_t>() != 0;
}

JsonCastType
JsonCast(const Config& params) {
    AssertInfo(params.contains(JSON_CAST_TYPE),
               "json_cast_type is required for a JSON index");
    return JsonCastType::FromString(
        params.at(JSON_CAST_TYPE).get<std::string>());
}

bool
IsStringType(DataType type) {
    return type == DataType::STRING || type == DataType::VARCHAR ||
           type == DataType::TEXT;
}

bool
IsPrimitiveScalar(DataType type) {
    return type == DataType::BOOL || type == DataType::INT8 ||
           type == DataType::INT16 || type == DataType::INT32 ||
           type == DataType::INT64 || type == DataType::FLOAT ||
           type == DataType::DOUBLE || type == DataType::TIMESTAMPTZ ||
           IsStringType(type);
}

ScalarIndexType
ScalarArtifactType(const IndexFamily& family) {
    if (family == families::kBitmap) {
        return ScalarIndexType::BITMAP;
    }
    if (family == families::kSort) {
        return ScalarIndexType::STLSORT;
    }
    if (family == families::kMarisa) {
        return ScalarIndexType::MARISA;
    }
    if (family == families::kAuto) {
        return ScalarIndexType::HYBRID;
    }
    if (family == families::kRTree) {
        return ScalarIndexType::RTREE;
    }
    if (family == families::kNgram) {
        return ScalarIndexType::NGRAM;
    }
    if (family == families::kFmIndex) {
        return ScalarIndexType::FMINDEX;
    }
    if (family == families::kInverted || family == families::kText ||
        family == families::kJsonFlat) {
        return ScalarIndexType::INVERTED;
    }
    return ScalarIndexType::NONE;
}

IndexFamily
ScalarFamily(const std::string& index_type,
             DataType value_type,
             bool is_text_match) {
    if (is_text_match) {
        AssertInfo(IsStringType(value_type),
                   "text match index requires a string field");
        AssertInfo(index_type == INVERTED_INDEX_TYPE,
                   "text match requires INVERTED index type");
        return families::kText;
    }
    if (index_type == INVERTED_INDEX_TYPE) {
        return families::kInverted;
    }
    if (index_type == BITMAP_INDEX_TYPE) {
        return families::kBitmap;
    }
    if (index_type == HYBRID_INDEX_TYPE) {
        return families::kAuto;
    }
    if (index_type == NGRAM_INDEX_TYPE) {
        AssertInfo(IsStringType(value_type),
                   "NGRAM index requires a string value type");
        return families::kNgram;
    }
    if (index_type == FMINDEX_INDEX_TYPE) {
        AssertInfo(IsStringType(value_type),
                   "FMINDEX requires a string value type");
        return families::kFmIndex;
    }
    if (index_type == MARISA_TRIE || index_type == MARISA_TRIE_UPPER) {
        AssertInfo(IsStringType(value_type),
                   "Trie index requires a string value type");
        return families::kMarisa;
    }
    if (index_type == ASCENDING_SORT) {
        return families::kSort;
    }

    // The old primitive factory used sort as the fallback for non-string
    // scalar values. The string specialization rejected unknown spellings.
    AssertInfo(!IsStringType(value_type),
               "unsupported string index type: {}",
               index_type);
    return families::kSort;
}

DataType
JsonValueType(const JsonCastType& cast) {
    switch (cast.element_type()) {
        case JsonCastType::DataType::BOOL:
            return DataType::BOOL;
        case JsonCastType::DataType::DOUBLE:
            return DataType::DOUBLE;
        case JsonCastType::DataType::VARCHAR:
            return DataType::VARCHAR;
        case JsonCastType::DataType::JSON:
            return DataType::JSON;
        default:
            ThrowInfo(DataTypeInvalid, "unsupported JSON cast type: {}", cast);
    }
}

uint8_t
ReadHybridSelector(const nlohmann::json& value,
                   std::string_view artifact_generation) {
    uint64_t selector = 0;
    if (value.is_number_unsigned()) {
        selector = value.get<uint64_t>();
    } else if (value.is_number_integer()) {
        const auto signed_selector = value.get<int64_t>();
        if (signed_selector < 0) {
            ThrowInfo(DataFormatBroken,
                      "HYBRID {} index_type selector is negative: {}",
                      artifact_generation,
                      signed_selector);
        }
        selector = static_cast<uint64_t>(signed_selector);
    } else {
        ThrowInfo(DataFormatBroken,
                  "HYBRID {} index_type selector must be an integer",
                  artifact_generation);
    }
    if (selector > std::numeric_limits<uint8_t>::max()) {
        ThrowInfo(DataFormatBroken,
                  "HYBRID {} index_type selector is out of range: {}",
                  artifact_generation,
                  selector);
    }
    return static_cast<uint8_t>(selector);
}

}  // namespace

AdaptedIndexType
AdaptIndexType(const IndexTypeAdapterRequest& request) {
    AssertInfo(!request.index_type.empty(), "index type is empty");

    AdaptedIndexType result;
    result.params = request.params;
    result.params[INDEX_TYPE] = request.index_type;
    result.params[INDEX_ENGINE_VERSION] =
        std::to_string(request.index_engine_version);
    result.params["field_type"] = static_cast<int32_t>(request.field_type);
    result.params[ELEMENT_TYPE_KEY] =
        static_cast<int32_t>(request.element_type);
    result.params["array_element_type"] =
        static_cast<int32_t>(request.element_type);
    result.params["nested"] = request.is_nested;
    result.params["is_nested"] = request.is_nested;

    if (IsVectorDataType(request.field_type)) {
        result.value_type = request.field_type == DataType::VECTOR_ARRAY
                                ? request.element_type
                                : request.field_type;
        result.family =
            VectorUsesDiskLoad(request.index_type, request.index_engine_version)
                ? families::kVectorDisk
                : families::kVectorMem;
        result.params["value_type"] = static_cast<int32_t>(result.value_type);
        return result;
    }

    if (request.field_type == DataType::GEOMETRY) {
        AssertInfo(request.index_type == RTREE_INDEX_TYPE,
                   "geometry requires RTREE index, got {}",
                   request.index_type);
        result.family = families::kRTree;
        result.value_type = DataType::GEOMETRY;
    } else if (request.field_type == DataType::JSON) {
        auto cast = JsonCast(request.params);
        result.value_type = JsonValueType(cast);
        if (cast.element_type() == JsonCastType::DataType::JSON) {
            AssertInfo(request.index_type == INVERTED_INDEX_TYPE ||
                           request.index_type == NGRAM_INDEX_TYPE,
                       "JSON flat index requires INVERTED or NGRAM spelling");
            result.family = families::kJsonFlat;
        } else {
            const auto cast_type = cast.element_type();
            if (request.index_type == ASCENDING_SORT) {
                AssertInfo(cast_type == JsonCastType::DataType::DOUBLE ||
                               cast_type == JsonCastType::DataType::VARCHAR,
                           "JSON sort index requires DOUBLE or VARCHAR cast");
            } else if (request.index_type == BITMAP_INDEX_TYPE) {
                AssertInfo(cast_type == JsonCastType::DataType::BOOL ||
                               cast_type == JsonCastType::DataType::VARCHAR,
                           "JSON bitmap index requires BOOL or VARCHAR cast");
            } else if (request.index_type == NGRAM_INDEX_TYPE) {
                AssertInfo(cast_type == JsonCastType::DataType::VARCHAR,
                           "JSON NGRAM index requires VARCHAR cast");
            } else {
                AssertInfo(request.index_type == INVERTED_INDEX_TYPE ||
                               request.index_type == HYBRID_INDEX_TYPE,
                           "unsupported JSON index type: {}",
                           request.index_type);
            }
            result.family =
                ScalarFamily(request.index_type, result.value_type, false);
        }
    } else if (request.field_type == DataType::ARRAY) {
        result.value_type =
            request.is_nested ? request.element_type : DataType::ARRAY;
        if (request.is_nested) {
            result.family =
                request.index_type == HYBRID_INDEX_TYPE
                    ? families::kAuto
                    : request.index_type == BITMAP_INDEX_TYPE
                          ? families::kBitmap
                          : request.index_type == INVERTED_INDEX_TYPE
                                ? families::kInverted
                                : families::kSort;
        } else {
            AssertInfo(request.index_type == HYBRID_INDEX_TYPE ||
                           request.index_type == BITMAP_INDEX_TYPE ||
                           request.index_type == INVERTED_INDEX_TYPE,
                       "unsupported ARRAY index type: {}",
                       request.index_type);
            result.family =
                ScalarFamily(request.index_type, request.element_type, false);
        }
    } else {
        AssertInfo(IsPrimitiveScalar(request.field_type),
                   "invalid data type to build index: {}",
                   request.field_type);
        result.value_type = request.field_type == DataType::TIMESTAMPTZ
                                ? DataType::INT64
                                : request.field_type;
        auto text_match = request.is_text_match ||
                          ConfigBool(request.params, "is_text_match", false);
        result.family =
            ScalarFamily(request.index_type, result.value_type, text_match);
    }

    result.params["value_type"] = static_cast<int32_t>(result.value_type);
    result.artifact_type = ScalarArtifactType(result.family);
    return result;
}

IndexFamily
FamilyFromScalarIndexType(ScalarIndexType type) {
    switch (type) {
        case ScalarIndexType::BITMAP:
            return families::kBitmap;
        case ScalarIndexType::STLSORT:
            return families::kSort;
        case ScalarIndexType::MARISA:
            return families::kMarisa;
        case ScalarIndexType::INVERTED:
            return families::kInverted;
        default:
            return {};
    }
}

ResolvedLoadTarget
ResolveLoadFamily(const IndexFamily& requested_family,
                  storage::FileSource& source) {
    if (requested_family != families::kAuto) {
        return {.family = requested_family, .auto_selector = std::nullopt};
    }

    uint8_t encoded_type = 0;
    if (source.Gen() == storage::Generation::V3) {
        auto value = source.GetMeta(INDEX_TYPE);
        if (!value.has_value()) {
            ThrowInfo(DataFormatBroken,
                      "HYBRID V3 artifact has no index_type selector");
        }
        encoded_type = ReadHybridSelector(*value, "V3");
    } else {
        auto bytes = source.ReadEntry(INDEX_TYPE);
        if (bytes.size() != sizeof(encoded_type)) {
            ThrowInfo(DataFormatBroken,
                      "invalid HYBRID V1/V2 index_type selector size: {}",
                      bytes.size());
        }
        std::memcpy(&encoded_type, bytes.data(), sizeof(encoded_type));
    }

    const auto selector = static_cast<ScalarIndexType>(encoded_type);
    auto family = FamilyFromScalarIndexType(selector);
    if (family.empty()) {
        ThrowInfo(DataFormatBroken,
                  "unsupported HYBRID internal index type: {}",
                  encoded_type);
    }
    return {.family = std::move(family), .auto_selector = selector};
}

std::string
PackedScalarIndexFileName(ScalarIndexType type) {
    auto type_name = ToString(type);
    AssertInfo(type != ScalarIndexType::NONE && type_name != "UNKNOWN",
               "invalid scalar artifact type for V3 file name");
    std::transform(type_name.begin(),
                   type_name.end(),
                   type_name.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return "milvus_packed_" + type_name + "_index.v3";
}

}  // namespace milvus::index
