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

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "common/BitsetView.h"
#include "common/QueryResult.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/TypeTraits.h"
#include "knowhere/expected.h"
#include "knowhere/index/index_node.h"

namespace milvus {
struct OpContext;
}  // namespace milvus

// The vector query faces. Pure mixins, they do NOT derive from
// `IndexReaderBase` (§4) — same rule as every scalar face.
//
// See core_refactor/01-scalar-index.md §11.3 (vector's four-face RE-HOMING),
// §12.1(a) (the face declares its own narrow parameter type), §12.1(b) (the
// iterator lifetime question, deliberately untouched), §12.1(c) (knowhere types
// are ALLOWED here).
//
// SCOPE: RE-HOMING, NOT REDESIGN. The knowhere interaction moves across
// verbatim; behaviour does not change and the benchmarks are expected to be
// unchanged (§11.3).
//
// KNOWHERE TYPES ARE ALLOWED IN THIS FILE. §12.1(c) rules it: the vector
// family's binding to knowhere is a fait accompli, and wrapping it only pays off
// if the engine is swapped or several engines coexist — a requirement that does
// not exist today. §11.2 rule 5 draws the final boundary: KNOWHERE TYPES MAY
// APPEAR FREELY INSIDE THE VECTOR FAMILY AND NOWHERE ELSE — not in the shared
// root, not in any scalar family (§10 rule 6). Do not wrap these types.
//
// QUERY FACES ARE ZERO-SHARED BETWEEN THE FAMILIES, by decision (§11.2 rule 2).
// Scalar is `In/Range/bitmap`, vector is `Search(dataset, params)` — there is no
// abstraction to share, so no cross-family query contract is designed. (The
// Builder and Loader faces ARE shared: "feed data -> take shape -> produce an
// Artifact" really is isomorphic across the families, and the real difference is
// input FORM, which cuts ACROSS families — §11.3.)

namespace milvus::index {

// THE FACE DECLARES ITS OWN NARROW PARAMETER TYPE — §12.1(a).
//
// The audit behind this: everything in `index/` reads exactly FOUR fields of
// `SearchInfo` — `search_params_`, `metric_type_`, `topk_`, `trace_ctx_`
// (`VectorIndex.h:173-186` `PrepareSearchParams`, `VectorMemIndex.cpp:732`,
// `VectorDiskIndex.cpp:685,720`, `index/Utils.cpp:518,530,538`). It NEVER reads
// `array_offsets_`, `active_count_`, `group_by_field_ids_`,
// `iterative_filter_execution`, `iterator_v2_info_`, the refine ratios, or
// anything else of the 20-odd fields.
//
// So `SearchInfo` is NOT split: it stays exec's own aggregate and is projected
// onto this type at the call site. Splitting it by semantics would only buy
// defensiveness, at the cost of constructing and threading two objects at every
// call site.
//
// (Two corollaries recorded by §12.1(a): "the index knows about segment and
// executor" is a POTENTIAL hazard, not an actual dependency — a wide struct
// passed by const& is not a wide dependency; and since the index never reads
// `array_offsets_`, ELEMENT-LEVEL FOLDING ALREADY DOES NOT HAPPEN INSIDE THE
// INDEX, which is consistent with §5.8.)
//
// THE REAL COST WAS AN INCLUDE CHAIN, NOT THE FIELD MIXING:
// `common/QueryInfo.h:26` includes `knowhere/config.h`, `index/Utils.h` includes
// `QueryInfo.h`, and nearly every scalar family's .cpp includes `index/Utils.h`
// (`BitmapIndex.cpp`, `ScalarIndexSort.cpp`, `StringIndexMarisa.cpp`,
// `StringIndexSort.cpp`, `InvertedIndexTantivy.cpp`, `FMIndex.cpp`,
// `RTreeIndex.cpp`, `NgramInvertedIndex.cpp`, `HybridScalarIndex.cpp`,
// `ScalarIndex.cpp`, `bson_inverted.cpp`, ...). THAT is what breaks §10 rule 6
// today — not any direct include by a scalar index. Declaring the narrow type
// here (knowhere::Json included) is the chain-breaking fix: neither
// `common/QueryInfo.h` nor `index/Utils.h` needs knowhere afterwards.
//
// Field names deliberately mirror `SearchInfo`'s so the projection at the call
// site is a one-to-one read.
struct VectorSearchParams {
    knowhere::Json search_params_;
    MetricType metric_type_;
    int64_t topk_{0};
    tracer::TraceContext trace_ctx_;
};

class VectorSearchReader {
 public:
    virtual ~VectorSearchReader() = default;

    virtual void
    Search(const DatasetPtr& dataset,
           const VectorSearchParams& params,
           const BitsetView& bitset,
           milvus::OpContext* op_ctx,
           SearchResult& result) const = 0;

    // §12.1(b) — THE ITERATOR SHAPE IS DELIBERATELY LEFT AS IT IS.
    //
    // A raw `IteratorPtr`, carrying no pin. The audited facts:
    // `SearchOnSealedIndex`'s accessor is a FUNCTION LOCAL
    // (`query/SearchOnSealed.cpp:88`); `PrepareVectorIteratorsFromIndex` takes
    // `&index.GetOffsetMapping()`, a RAW POINTER INTO THE INDEX OBJECT
    // (`exec/operator/Utils.h:129-134`); both land in
    // `SearchResult::vector_iterators_` (`common/QueryResult.h:305`) with NO PIN
    // SAVED ALONGSIDE; and consumption happens in LATER operators
    // (`SearchGroupByNode.cpp:89`, `IterativeFilterNode.cpp:127`,
    // `IterativeElementFilterNode.cpp:117`).
    //
    // Whether this actually dangles depends on whether cachinglayer can evict
    // the cell within the same query after the pin is released — THAT AUDIT HAS
    // NOT BEEN DONE, so it is not asserted to be a defect. The ruling: it is
    // entirely made of existing code, is neither introduced nor amplified by
    // this refactor, and so W1 does not touch it. Do NOT pre-emptively change
    // this to a pin-carrying handle — that would pay contract complexity for an
    // unconfirmed defect. If the audit later confirms eviction is possible, the
    // fix is an anchor like `SearchResult::PinBitset`
    // (`common/QueryResult.h:309`), not a reopening of the contract shape.
    virtual knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
    Iterators(const DatasetPtr& dataset,
              const knowhere::Json& json,
              const BitsetView& bitset,
              milvus::OpContext* op_ctx) const = 0;

    virtual bool
    RefineEnabled() const = 0;
};

// The value face — the `GetVector` family, vector's counterpart of
// `ScalarValueReader<T>` (§11.3). Re-homed verbatim from `VectorIndex`:
// `GetVector` / `GetSparseVector` / `HasRawData`.
class VectorValueReader {
 public:
    virtual ~VectorValueReader() = default;

    // Whether the original vectors can be recovered from the index at all.
    // Today's `VectorIndex::HasRawData()`. Self-description, not a throw.
    virtual bool
    HasRawData() const = 0;

    virtual std::vector<uint8_t>
    GetVector(const DatasetPtr& dataset) const = 0;

    virtual std::unique_ptr<const knowhere::sparse::SparseRow<SparseValueType>[]>
    GetSparseVector(const DatasetPtr& dataset) const = 0;
};

// The vector Appender face is `GrowingVectorIndex`, declared beside
// `GrowingScalarIndex<T>` in GrowingIndex.h — the two are SIBLINGS with shared
// semantics (snapshot + watermark). See §7.1 and the notes there.

}  // namespace milvus::index
