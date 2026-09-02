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

// Leaf helpers shared by the index families.
//
// TWO CHAINS ARE CUT HERE, and cutting them is the point of this file's edit.
//
// (a) `index/ -> common/QueryInfo.h -> knowhere/config.h`. 01-scalar-index.md
//     §12.1(a) identifies this as ONE OF THE TWO transitive paths by which
//     knowhere reaches every scalar index translation unit — almost every
//     scalar `.cpp` includes this header, and this header included
//     `QueryInfo.h`. §11.2 rule 5 / §10 rule 6 require the scalar families to
//     be knowhere-free. The only declaration that needed it was
//     `CheckAndUpdateKnowhereRangeSearchParam`, which took a `SearchInfo&` and
//     filled a `knowhere::Json&` — a VECTOR-side concern. §12.1(a) states the
//     fix in as many words: "declare the narrow parameter type (including
//     `knowhere::Json`) in the vector family's own header — the vector family
//     is allowed to see knowhere by §11.2 rule 5 — and then neither
//     `common/QueryInfo.h` nor `index/Utils.h` needs knowhere". It is removed
//     from here; see the handoff note where it used to be declared.
//
// (b) `index/ -> index/ScalarIndex.h` for the single enum `ScalarIndexType`.
//     `ScalarIndex<T>` is retired (§11.2 rule 3), and the two config getters
//     that returned that enum now return FAMILY NAMES (index/Families.h).
//
// A THIRD CHAIN IS NOT CUT AND CANNOT BE CUT HERE: `common/Types.h` itself
// includes four knowhere headers (`common/Types.h:43-46`) and defines
// `BinarySet` / `IndexType` / `IndexVersion` as knowhere aliases (:681-687).
// Every file that needs `TargetBitmap` includes it — including the contract
// layer. §12.1(a) names only chains (a) and (b) as the baseline; this one is a
// third, and no amount of work inside index/ removes it. Reported, not worked
// around.

#include <unordered_map>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <fcntl.h>
#include <sys/stat.h>
#include <tuple>
#include <map>
#include <string>
#include <boost/algorithm/string.hpp>

#include "common/Common.h"
#include "common/Types.h"
#include "common/FieldData.h"
#include "index/IndexInfo.h"
#include "storage/Types.h"
#include "storage/DataCodec.h"
#include "log/Log.h"

namespace milvus::index {

size_t
get_file_size(int fd);

// REMOVED FROM HERE — handoff to the vector family, same reason as
// `CheckAndUpdateKnowhereRangeSearchParam` below.
//
// `bool is_unsupported(const IndexType&, const MetricType&)` (was Utils.h:62-63)
// and the three tables behind it (`NM_List`, `BIN_List`,
// `unsupported_index_combinations`, Utils.cpp:59-108) are a hardcoded list of
// knowhere index-type / metric-type pairs. `is_unsupported` had one consumer,
// `index/VectorMemIndex.cpp`; the other five had none at all. Keeping them here
// is what forced `#include "knowhere/comp/index_param.h"` into Utils.cpp and,
// with it, knowhere into a translation unit every scalar family links against
// (§10 rule 6, §11.2 rule 5).

bool
CheckKeyInConfig(const Config& cfg, const std::string& key);

void
ParseFromString(google::protobuf::Message& params, const std::string& str);

template <typename T>
inline std::optional<T>
GetValueFromConfig(const Config& cfg, const std::string& key) {
    if (!cfg.contains(key)) {
        return std::nullopt;
    }

    const auto& value = cfg.at(key);
    if (value.is_null()) {
        return std::nullopt;
    }

    try {
        if constexpr (std::is_same_v<T, bool>) {
            if (value.is_boolean()) {
                return value.get<bool>();
            }
            // compatibility for boolean string
            return boost::algorithm::to_lower_copy(value.get<std::string>()) ==
                   "true";
        }
        // compatibility for numeric string (e.g., from index_params which is map<string,string>)
        if constexpr (std::is_integral_v<T>) {
            if (value.is_string()) {
                auto str = value.get<std::string>();
                if constexpr (std::is_signed_v<T>) {
                    return static_cast<T>(std::stoll(str));
                } else {
                    return static_cast<T>(std::stoull(str));
                }
            }
        }
        return value.get<T>();
    } catch (const nlohmann::json::type_error& e) {
        if (!CONFIG_PARAM_TYPE_CHECK_ENABLED) {
            LOG_WARN("config type mismatch for key {}: {}", key, e.what());
            return std::nullopt;
        }
        ThrowInfo(ErrorCode::UnexpectedError,
                  "config type error for key {}: {}",
                  key,
                  e.what());
    } catch (const std::exception& e) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Unexpected error for key {}: {}",
                  key,
                  e.what());
    }
    return std::nullopt;
}

template <typename T>
inline void
CheckMetricTypeSupport(const MetricType& metric_type) {
    if constexpr (std::is_same_v<T, bin1>) {
        AssertInfo(
            IsBinaryVectorMetricType(metric_type),
            "binary vector does not support metric type: " + metric_type);
    } else if constexpr (std::is_same_v<T, int8>) {
        AssertInfo(IsIntVectorMetricType(metric_type),
                   "int vector does not support metric type: " + metric_type);
    } else {
        AssertInfo(IsFloatVectorMetricType(metric_type),
                   "float vector does not support metric type: " + metric_type);
    }
}

int64_t
GetDimFromConfig(const Config& config);

std::string
GetMetricTypeFromConfig(const Config& config);

std::string
GetIndexTypeFromConfig(const Config& config);

IndexVersion
GetIndexEngineVersionFromConfig(const Config& config);

int32_t
GetBitmapCardinalityLimitFromConfig(const Config& config);

// Return a FAMILY NAME (index/Families.h), not a `ScalarIndexType`.
// The enum lived on the retired `ScalarIndex<T>` header, and persisting its
// ORDINAL is exactly the fragility the `auto` family removes — see
// `index/scalar/auto/AutoIndexBuilder.cpp`. Consumed by `AutoBuildParams`.
std::string
GetLowCardinalityFamilyFromConfig(const Config& config);

std::string
GetHighCardinalityFamilyFromConfig(const Config& config);

Config
ParseConfigFromIndexParams(
    const std::map<std::string, std::string>& index_params);

struct IndexDataCodec {
    std::list<std::unique_ptr<storage::DataCodec>> codecs_{};
    int64_t size_{0};
};

std::map<std::string, IndexDataCodec>
CompactIndexDatas(
    std::map<std::string, std::unique_ptr<storage::DataCodec>>& index_datas);

IndexDataCodec
CompactIndexDatasByKey(
    const std::string& key,
    std::unique_ptr<storage::DataCodec> slice_meta,
    std::map<std::string, std::unique_ptr<storage::DataCodec>>& index_datas);

std::unique_ptr<storage::DataCodec>
AssembleIndexDataCodec(const IndexDataCodec& index_slices);

std::unique_ptr<storage::DataCodec>
AssembleIndexDataCodec(IndexDataCodec&& index_slices);

void
AssembleIndexDatas(
    std::map<std::string, std::unique_ptr<storage::DataCodec>>& index_datas,
    BinarySet& index_binary_set);

void
AssembleIndexDatas(std::map<std::string, IndexDataCodec>& index_datas,
                   BinarySet& index_binary_set);

void
AssembleIndexDatas(std::map<std::string, FieldDataChannelPtr>& index_datas,
                   std::unordered_map<std::string, FieldDataPtr>& result);

// On Linux, read() (and similar system calls) will transfer at most 0x7ffff000 (2,147,479,552) bytes once
void
ReadDataFromFD(int fd, void* buf, size_t size, size_t chunk_size = 0x7ffff000);

// REMOVED FROM HERE — handoff to the vector family.
//
// `bool CheckAndUpdateKnowhereRangeSearchParam(const SearchInfo&, int64_t topk,
//  const MetricType&, knowhere::Json&)` (was Utils.h:228-232, defined at
// Utils.cpp:512-544) is the ONLY declaration in this header that needed
// `common/QueryInfo.h`, and through it `knowhere/config.h`. Its consumers are
// `index/vector/...` and `query/CachedSearchIterator.cpp:66` — all vector-side.
//
// Per §12.1(a) it must be re-declared in the VECTOR family's own header, which
// is allowed to see knowhere (§11.2 rule 5, §12.1(c)). It is not moved here
// because the vector half of this wave is a separate change; if it has not
// landed when this one does, that call fails to resolve, which is the intended
// forcing function rather than an accident.

// for unused
void inline SetBitsetUnused(void* bitset, const uint32_t* doc_id, uintptr_t n) {
    ThrowInfo(ErrorCode::UnexpectedError, "SetBitsetUnused is not supported");
}

// For sealed segment, the doc_id is guaranteed to be less than bitset size which equals to the doc count of tantivy before querying.
void inline SetBitsetSealed(void* bitset, const uint32_t* doc_id, uintptr_t n) {
    TargetBitmap* bitmap = static_cast<TargetBitmap*>(bitset);

    for (uintptr_t i = 0; i < n; ++i) {
        assert(doc_id[i] < bitmap->size());
        (*bitmap)[doc_id[i]] = true;
    }
}

// For growing segment, concurrent insert exists, so the doc_id may exceed bitset size.
void inline SetBitsetGrowing(void* bitset,
                             const uint32_t* doc_id,
                             uintptr_t n) {
    TargetBitmap* bitmap = static_cast<TargetBitmap*>(bitset);
    const auto bitmap_size = bitmap->size();

    for (uintptr_t i = 0; i < n; ++i) {
        const auto id = doc_id[i];
        if (id >= bitmap_size) {
            // Ideally, the doc_id is sorted and we can return directly. But I don't want to have this strong guarantee.
            continue;
        }
        (*bitmap)[id] = true;
    }
}

// Get the SSO (Small String Optimization) threshold for std::string.
// Strings with capacity <= this threshold store data inline (no heap allocation).
inline size_t
GetStringSSOThreshold() {
    static const size_t threshold = std::string().capacity();
    return threshold;
}

}  // namespace milvus::index
