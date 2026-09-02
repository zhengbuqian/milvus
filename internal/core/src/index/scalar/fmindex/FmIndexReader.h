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
#include <string>
#include <string_view>

#include "common/Types.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/NullReader.h"
#include "index/contracts/PatternMatchReader.h"
#include "index/fmindex/FMIndex.h"

// The READER of the FM-index family.
//
// See 01-scalar-index.md §5.2 (`PatternMatchReader`) and §8:
//   | `FMIndex` | `PatternMatchReader` **this predicate face and no other** |
//
// Today `FMIndex : ScalarIndex<std::string>` carries about twenty methods it
// cannot answer; SIX OF THEM ARE PURE THROW SHELLS
// (`FMIndex.h:82,91,100,148,156,268` and `FMIndex.cpp:379,386`). They are not
// re-declared here — §3 principle 3 and §10 rule 4: an unsupported operation
// must not exist on the type.
//
// FACES: `PatternMatchReader` + `NullReader`. The null face is real, not a
// courtesy: `FMIndex.cpp:391-403` implements `IsNull`/`IsNotNull` from
// `null_bitmap_`, and §5's cross-family note lists FMIndex among the seven
// scalar families that all really implement it.

namespace milvus::index {

class FmIndexReader final : public IndexReaderBase,
                            public PatternMatchReader,
                            public NullReader {
 public:
    // `cost_ratio` — see `ShouldUsePattern` below. INJECTED, not read from a
    // global (§8's mapping-table note for this row: "the `SegcoreConfig`
    // dependency becomes a constructor parameter").
    FmIndexReader(fmindex::FMIndex engine,
                  TargetBitmap null_bitmap,
                  int64_t total_rows,
                  int64_t total_tokens,
                  double cost_ratio);

    ~FmIndexReader() override;

    // ---- IndexReaderBase (§4.2) ----------------------------------------

    ReaderCaps
    Caps() const override;

    Domain
    CoordDomain() const override;

    int64_t
    Count() const override;

    DataType
    ValueType() const override;

    int64_t
    MemoryUsage() const override;

    ResourceUsage
    CellByteSize() const override;

    // ---- PatternMatchReader (§5.2) -------------------------------------

    // Only PrefixMatch / PostfixMatch / InnerMatch ever arrive; `Match` and
    // `RegexMatch` are declined by `ShouldUsePattern` below.
    TargetBitmap
    PatternMatch(std::string_view pattern, PatternOp op) const override;

    // ---- NullReader (§5) -----------------------------------------------

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

    // ---- A PER-CALL ROUTING GATE THAT THE CONTRACT LAYER DOES NOT YET HAVE --
    //
    // This is today's `ScalarIndex<T>::ShouldUseOp(op, pattern)`
    // (`ScalarIndex.h:227-240`, overridden at `FMIndex.h:199-244`), consumed by
    // exec at `exec/expression/Expr.h:2716`. It answers: "for THIS literal,
    // is the index cheaper than a raw scan?" — FMIndex counts occurrences in
    // O(|pattern|) and declines degenerate high-hit literals like `%a%`.
    //
    // IT CANNOT LIVE IN `ReaderCaps`: caps are per-index static data readable
    // WITHOUT a pin (§4.1), while this answer depends on the query literal.
    // The contract layer already has exactly this shape for the other candidate
    // family — `NgramReader::CanHandle(literal, op)` (§5.4) — and the design
    // document does not carry it over to `PatternMatchReader`, which is a GAP,
    // not a decision (see the report accompanying this skeleton). Declared here
    // as a family method so the live behaviour is not silently dropped; it
    // should be hoisted into `PatternMatchReader` alongside `CanHandle`.
    bool
    ShouldUsePattern(std::string_view pattern, PatternOp op) const;

 private:
    // O(|pattern|) occurrence count; -1 means "unknown, accept".
    int64_t
    PatternCount(std::string_view pattern, PatternOp op) const;

    TargetBitmap
    DocsToBitmap(const std::vector<uint64_t>& docs) const;

    // The ENGINE, composed (§3 principle 2). `fmindex::FMIndex` is a vendored
    // libsais-backed structure under index/fmindex/, not a base class.
    fmindex::FMIndex engine_;

    TargetBitmap null_bitmap_;
    int64_t total_rows_{0};
    int64_t total_tokens_{0};

    // WAS: `segcore::SegcoreConfig::default_config().get_fmindex_cost_ratio()`
    // read inline at `FMIndex.h:226-228`, the single `index/ -> segcore/`
    // header edge in the scalar tree (§2.2 row 6, §10 rule 1).
    //
    // BEHAVIOUR DELTA TO DECIDE WHEN THE LOGIC MOVES: the old read went through
    // a process-wide static (`SegcoreConfig.h:265`), so a live config change to
    // `queryNode.fmindexCostRatio` took effect on the next query. A value
    // copied at construction freezes it for the lifetime of the loaded index.
    // If live updates must be preserved, inject a policy callback instead of a
    // double — but make that an explicit choice, not an accident of the move.
    double cost_ratio_{0.001};
};

}  // namespace milvus::index
