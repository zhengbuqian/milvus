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

#include <memory>
#include <optional>

#include "ArrayOffsets.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "nlohmann/json.hpp"

// NO KNOWHERE HEADER IN THIS FILE — see core_refactor/01-scalar-index.md
// §12.1(a) and §10 rule 6.
//
// THE CHAIN THIS BREAKS. §10 rule 6 requires "zero knowhere includes in the
// scalar families' contracts and implementations", and the current-state
// baseline recorded there says the violation is NOT any scalar index including
// knowhere directly (their own headers count zero) — it is two transitive
// chains, and this file is one of them:
//
//     common/QueryInfo.h -> knowhere/config.h
//     index/Utils.h      -> common/QueryInfo.h
//     nearly every scalar family .cpp -> index/Utils.h
//       (BitmapIndex.cpp, ScalarIndexSort.cpp, StringIndexMarisa.cpp,
//        StringIndexSort.cpp, InvertedIndexTantivy.cpp, FMIndex.cpp,
//        RTreeIndex.cpp, NgramInvertedIndex.cpp, HybridScalarIndex.cpp,
//        ScalarIndex.cpp, bson_inverted.cpp, ...)
//
// so every scalar index translation unit compiles knowhere today. The other
// chain is `index/Index.h`, the shared root, which has three direct references;
// breaking that one belongs to the index-side work.
//
// THE FIX IS TO BREAK THE CHAIN, NOT TO SPLIT `SearchInfo`. §12.1(a) is
// explicit that "how to split `SearchInfo`" was investigated and CLOSED with
// the opposite conclusion: `index/` reads exactly FOUR of its fields —
// `search_params_`, `metric_type_`, `topk_`, `trace_ctx_` — and never touches
// `array_offsets_`, `active_count_`, `group_by_field_ids_`,
// `iterative_filter_execution`, `iterator_v2_info_` or the refine ratios.
// A wide struct passed by `const&` is not a wide dependency: the extra fields
// create no edge and are never read. (That also disproves a more worrying
// guess: since index never reads `array_offsets_`, the element-level fold does
// NOT happen inside an index today, consistent with §5.8.) So the correct move
// is the narrowing this design uses everywhere else: THE FACE DECLARES ITS OWN
// PARAMETER TYPE. That type is `index::VectorSearchParams` in
// `index/contracts/VectorFaces.h` — the vector family's own header, where
// knowhere is allowed (§11.2 item 5, and §12.1(c) which rules that knowhere
// types may appear freely inside the vector family). `SearchInfo` stays what it
// always was: exec's own aggregate, projected onto the narrow type at the call
// site.
//
// WHY `nlohmann::json` IS THE RIGHT SPELLING HERE, AND WHAT IT DOES NOT BUY.
// `knowhere::Json` is a typedef for `nlohmann::json` (knowhere/config.h:35), so
// naming the underlying type costs nothing at the call sites — building
// `index::VectorSearchParams{search_params_, ...}` needs no conversion. BE
// HONEST ABOUT THE SCOPE OF THE WIN: this removes a knowhere INCLUDE from every
// scalar translation unit (which is the stated benefit — compile isolation, and
// a knowhere upgrade no longer rebuilding every scalar index), it does not make
// the parameter blob independent of knowhere's schema. The params are still
// knowhere search parameters; only the header dependency is gone.

namespace milvus {

// The search-parameter blob. Same underlying type as `knowhere::Json`, named
// without the knowhere header — see the note above.
using SearchParamsJson = nlohmann::json;

struct SearchIteratorV2Info {
    std::string token = "";
    uint32_t batch_size = 0;
    std::optional<float> last_bound = std::nullopt;
};

// Brute-force index params sourced from the collection-level index metadata at
// plan creation. Used only by brute force when a segment predates a field added
// by add_function_field, so its per-segment metadata does not carry the field.
struct BruteForceIndexParams {
    std::optional<float> bm25_k1_;
    std::optional<float> bm25_b_;
    std::optional<int64_t> minhash_lsh_band_;
    std::optional<int64_t> minhash_element_bit_width_;
};

struct SearchInfo {
    int64_t topk_{0};
    int64_t group_size_{1};
    bool strict_group_size_{false};
    int64_t round_decimal_{0};
    FieldId field_id_;
    MetricType metric_type_;
    SearchParamsJson search_params_;
    BruteForceIndexParams brute_force_index_params_;
    std::vector<FieldId>
        group_by_field_ids_;  // Group by field IDs (single or multi-field)
    tracer::TraceContext trace_ctx_;
    bool materialized_view_involved = false;
    bool iterative_filter_execution = false;
    std::optional<SearchIteratorV2Info> iterator_v2_info_ = std::nullopt;
    std::optional<std::string> json_path_;
    std::optional<milvus::DataType> json_type_;
    bool strict_cast_{false};
    std::shared_ptr<const IArrayOffsets> array_offsets_{
        nullptr};  // For element-level search
    bool global_refine_enable_{false};
    float search_topk_ratio_{0.0f};
    float refine_topk_ratio_{0.0f};
    // Number of rows visible to a growing-segment search, in logical row space.
    // Growing plans decide it ONCE from get_active_count(timestamp) and carry
    // it down so no kernel re-derives it. Sealed plans leave it unset: their
    // indexes are immutable and retain the empty-bitset fast path.
    //
    // Re-deriving it is not safe on a growing segment: a concurrent insert
    // publishes rows into the column storage (and into a nullable field's
    // offset mapping) before ack_responder_ advances, so a later read sees
    // rows this search must not touch. Reduce then validates offsets against
    // the acknowledged count and rejects them ("invalid offset ... rows num
    // ..."), or, if the ack catches up first, silently returns rows newer
    // than the query's MVCC timestamp.
    //
    // -1 means "not supplied" (sealed searches and direct callers). Growing
    // kernels fall back to computing it themselves for direct calls.
    int64_t active_count_{-1};

    bool
    element_level() const {
        return array_offsets_ != nullptr;
    }

    bool
    has_group_by() const {
        return !group_by_field_ids_.empty();
    }
};

using SearchInfoPtr = std::shared_ptr<SearchInfo>;

}  // namespace milvus
