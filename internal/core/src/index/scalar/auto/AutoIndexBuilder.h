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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/Types.h"
#include "index/contracts/IndexBuilder.h"
#include "storage/artifact/Artifact.h"

// The `auto` family: CARDINALITY-BASED INDEX SELECTION, AS A BUILD STRATEGY.
//
// See 01-scalar-index.md §6.3 in full:
//
//   > `HybridScalarIndex`'s "pick bitmap or inverted by cardinality" is a
//   > BUILD-TIME DECISION. The builder chooses at `Seal()` and records the
//   > choice in the artifact metadata; `Loader::Open` returns the chosen
//   > concrete reader directly. The runtime forwarding class is deleted.
//
// ==========================================================================
// WHAT IS DELETED: `HybridScalarIndex<T>` AND `JsonHybridScalarIndex<T>`.
//
// `HybridScalarIndex` is 248 header + 465 implementation lines of which the
// entire query surface — `In`, `NotIn`, `IsNull`, `IsNotNull`, `Query`,
// `ShouldUseOp`, `SupportPatternMatch`, `PatternMatch`, `Range` x2,
// `Reverse_Lookup`, `SupportFastReverseLookup`, `Count`, `Size`
// (`HybridScalarIndex.h:69-165`) — is a one-line forward to
// `internal_index_->Same()`. A virtual call per query, per predicate, forever,
// to re-answer a question that was settled once at build time. Every one of
// those forwards is also an unguarded null dereference if the internal index
// was never created.
//
// There is no reader in this directory. That is the whole point: after `Seal()`
// this family does not exist, only `bitmap` or `inverted` does.
// ==========================================================================

namespace milvus::index {

struct AutoBuildParams {
    // Distinct values at or above this count select the high-cardinality
    // family. `BITMAP_INDEX_CARDINALITY_LIMIT` (index/Meta.h:80).
    int32_t cardinality_limit{0};

    // Which families the two sides map to. `HYBRID_LOW/HIGH_CARDINALITY_INDEX_TYPE`
    // (index/Meta.h:82-85). Empty means the legacy default, resolved as in
    // `SelectIndexTypeByCardinality` (HybridScalarIndex.cpp:86-106): low =
    // bitmap; high = inverted for strings, sorted for integrals, inverted
    // otherwise.
    std::string low_cardinality_family;
    std::string high_cardinality_family;

    bool nested{false};
    DataType value_type{DataType::NONE};
};

template <typename T>
class AutoIndexBuilder final : public IndexBuilder<T> {
 public:
    explicit AutoIndexBuilder(AutoBuildParams params);

    ~AutoIndexBuilder() override;

    // Form **C** (§6.1.1): the selection needs a statistic before the real
    // build can start, so the input is scanned twice — the first pass counts
    // distinct values AND MAY STOP EARLY, as soon as the count reaches the
    // limit (`HybridScalarIndex.cpp:114`, `:130`), because the exact
    // cardinality above the threshold does not change the answer.
    //
    // ------------------------------------------------------------------
    // A GAP IN THE CONTRACT, NOT AN OVERSIGHT HERE.
    //
    // `BuilderInputSpec::needs_second_pass` (index/contracts/IndexBuilder.h)
    // tells the CALLER to rewind and feed the data again (§6.1.2:
    // "multi-pass must be in the contract: form C requires that
    // `ScanCursor::Seek(0)` be available"). But `IndexBuilder<T>` has no way
    // for the caller to say WHERE ONE PASS ENDS AND THE NEXT BEGINS — there is
    // no `EndPass()` / `BeginPass()`, and `Add` cannot tell pass 1 from pass 2.
    // Nor can the builder tell the caller that the first pass stopped early, so
    // the early exit is unreachable through the interface as declared.
    //
    // This skeleton does NOT invent the missing method: the contract layer is
    // owned elsewhere and the design document should decide the shape. It is
    // reported instead, and the family is written as if a pass boundary exists.
    // ------------------------------------------------------------------
    BuilderInputSpec
    InputSpec() const override;

    void
    Add(size_t n, const T* values, const bool* valid) override;

    // Picks the family, then returns THAT FAMILY'S artifact — with
    // `families::kFamilyMetaKey` set to the chosen family, not to "auto".
    // The loader side therefore needs no `auto` entry at all in the common
    // case; `AutoIndexLoader` exists only for artifacts written before the
    // choice was recorded that way.
    storage::ArtifactPtr
    Seal() && override;

 private:
    // §6.3's decision, and the only thing this class contributes.
    // Was `SelectIndexTypeByCardinality` (HybridScalarIndex.cpp:86-106).
    std::string
    SelectFamily(size_t distinct_count) const;

    AutoBuildParams params_;

    // Pass 1 state. Was `SelectBuildTypeForPrimitiveType`
    // (HybridScalarIndex.cpp:121-137) and `SelectBuildTypeForArrayType`
    // (:139-169).
    //
    // BUG TO FIX WHILE MOVING: the primitive counter iterates `RawValue(i)`
    // WITHOUT consulting `is_valid(i)`, so null rows inflate the cardinality
    // and can push a low-cardinality field onto the high-cardinality index.
    // `JsonHybridScalarIndex` exists partly to work around it — it duplicates
    // the whole loop twice (JsonHybridScalarIndex.h:71-83 and :129-141) purely
    // to add the validity check (see the comment at :127-128). One correct
    // counter here removes both copies.
    size_t distinct_count_{0};
    bool first_pass_done_{false};

    // Pass 2 delegate: the concrete family's builder, created once the family
    // is known.
    std::unique_ptr<IndexBuilder<T>> delegate_;
};

}  // namespace milvus::index
