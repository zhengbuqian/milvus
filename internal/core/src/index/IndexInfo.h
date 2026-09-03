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

// `CreateIndexInfo` IS THE PARAMETER BAG THAT §11.2 RULE 4 BREAKS UP
// ("split `CreateIndexInfo`; a family-level loader/builder registry replaces
// `IndexFactory`'s God switch").
//
// It carries 15 fields for every index of every family: `json_cast_type` and
// `json_path` are meaningless to a vector index, `metric_type` and `dim` are
// meaningless to a bitmap, `ngram_params` and `fmindex_params` are each read by
// exactly one branch of the old switch. Its replacement is already in place —
// each family declares its OWN build parameters next to its builder
// (`TextIndexBuildParams`, `NgramBuildParams`, `FmIndexBuildParams`,
// `InvertedBuildParams`, `BitmapBuildParams`, `SortedBuildParams`,
// `RTreeBuildParams`, `JsonFlatBuildParams`, `AutoBuildParams`), and the
// registry hands the raw `BuildParams` (a `Config`) to the family's factory,
// which reads the keys it understands (index/contracts/Registry.h, §6.1).
//
// THIS FILE IS KEPT FOR NOW because the struct is also constructed and read
// outside index/ — `indexbuilder/ScalarIndexCreator.cpp:175`,
// `indexbuilder/VecIndexCreator.cpp:50`, `segcore/load_index_c.cpp:117`,
// `segcore/Utils.cpp:1484`,
// `segcore/storagev1translator/V1SealedIndexTranslator.cpp:119` — and those
// call sites belong to the segcore/indexbuilder half of this phase.
// `NgramParams::loading_index` is already dead in the new shape: it existed to
// tell ONE constructor whether it was building or loading, and building and
// loading are now two interfaces.


#include "common/JsonCastType.h"
#include "common/Types.h"
#include "common/Consts.h"

namespace milvus::index {

struct NgramParams {
    bool loading_index;
    uintptr_t min_gram;
    uintptr_t max_gram;
};

struct FMIndexParams {
    uint32_t sa_sample_rate = 8;  // suffix-array sampling rate (default 8)
    uint32_t block_bytes = 64;  // rank-block granularity in bytes (default 64)
};

struct CreateIndexInfo {
    DataType field_type;
    IndexType index_type;
    MetricType metric_type;
    IndexVersion index_engine_version;
    std::string field_name;
    int64_t dim;
    int32_t scalar_index_engine_version{1};
    uint32_t tantivy_index_version{7};
    JsonCastType json_cast_type{JsonCastType::UNKNOWN};
    std::string json_path;
    std::string json_cast_function{UNKNOW_CAST_FUNCTION_NAME};
    std::optional<NgramParams> ngram_params{std::nullopt};
    std::optional<FMIndexParams> fmindex_params{std::nullopt};
    bool is_text_match{false};
    std::string analyzer_extra_info;
};

}  // namespace milvus::index
