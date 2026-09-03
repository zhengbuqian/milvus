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

#include <string>

#include "common/Types.h"
#include "index/contracts/PatternMatchReader.h"
#include "index/contracts/ScalarPredicateReader.h"
#include "index/contracts/SpatialReader.h"
#include "segcore/indexing/FieldIndexCapability.h"

// Execution-path selection: a PURE FUNCTION OF `index::ReaderCaps`.
//
// See core_refactor/01-scalar-index.md §4.1 ("exec's execution-path decision
// (`DetermineExecPath`) consumes only this struct — no `dynamic_cast`, no
// try/catch"), §9 (the consumer table) and §10 rules 3b and 4.
//
// -------------------------------------------------------------------------
// WHAT GOES AWAY. Today the decision is made in three ways at once:
//
//   a. `SegmentExpr::DetermineExecPath` (exec/expression/Expr.h:2807) asks the
//      segment "do you have an index" (`HasCompatibleScalarIndex`, `:2573`),
//      PINS, and then re-checks that the pin actually produced something.
//   b. It then REFINES by asking the PINNED, CONCRETE index — e.g.
//      `PhyUnaryRangeFilterExpr::DetermineExecPath`
//      (exec/expression/UnaryExpr.cpp:2135-2218) calls
//      `SegmentExpr::CanUseIndexForOp<T>` (`Expr.h:2702`), which
//      `dynamic_cast`s to `index::ScalarIndex<T>` and calls `ShouldUseOp()`;
//      `PinnedJsonIndexIsFlat()` (`Expr.h:2654`) `dynamic_cast`s to
//      `index::JsonFlatIndex`.
//   c. Capability gaps that are not covered by a `Support*` predicate surface
//      as `ThrowInfo(Unsupported)`, so a caller must either try or memorise
//      which `Support*` to call (§2.2, `ScalarIndex.h:140,187`).
//
// (b) and (c) both vanish here: (b) because every input is pure data available
// before the pin, (c) because absence is a false bit, never an exception.
//
// -------------------------------------------------------------------------
// THE RULE (§4.1): IF A USABLE INDEX EXISTS, USE IT; FALL BACK TO A COLUMN SCAN
// ONLY IF NONE DOES. Since shredding left `index/`, "this path has both a path
// index and a shredded typed sub-column" is no longer a choice BETWEEN TWO
// INDEXES — it is the ordinary index-vs-column-scan test. How fast the typed
// sub-column scans only affects how columnar-format scans internally; it is not
// an input to this decision, which is why merging the column's `ColumnCaps`
// with `FieldIndexCapability` is NOT a refactor phase 1 prerequisite.
//
// Known sub-optimal case, recorded rather than solved: under tiered storage,
// waking a cold index cell can cost more than scanning an already-resident
// typed sub-column. That is cost-model work; it does not change this contract.
//
// -------------------------------------------------------------------------
// NATIVE OPERATOR ENUMS ONLY. The parameters below use `index::CompareOp` /
// `index::PatternOp` / `index::SpatialOp`, not `proto::plan::OpType` or
// `proto::plan::GISFunctionFilterExpr_GISOp`. README §5 rule 2 keeps pb off
// contract signatures, and `milvus::OpType` IS `proto::plan::OpType`
// (common/Types.h:106). The plan -> native mapping happens where the physical
// expression is constructed.

namespace milvus::exec {

// Execution path for expression evaluation. Determines how the expression
// result bitmap is produced.
//
// MOVED HERE FROM `exec/expression/Expr.h:54` so that the enum and the function
// that decides it live together, and so that nothing on the decision path needs
// to include the expression kernels.
enum class ExprExecPath {
    RawData,      // brute-force scan of the raw column
    ScalarIndex,  // a pinned index reader interface
    PkIndex,      // segment_->pk_range / search_ids
    TextIndex,    // the text-match interface
    // JsonStats: the shredded JSON layout. NOTE (§1): this is NOT an index
    // path — after refactor phase 1 `JsonKeyStats` is a column layout under
    // `segcore/json_stats/`, and `ExecutorForShreddingData` is a full column
    // scan (it asserts `processed_size == num_rows`). It keeps a separate enum
    // value only because §1's transitional treatment explicitly does NOT change
    // exec's JSON call shape this phase. When the sub-columns are promoted to
    // first-class columnar-format objects this value folds into `RawData`.
    JsonStats,
};

// Which query interface this expression needs. One-to-one with the interfaces
// in `index/contracts/` (§4 contract tree).
enum class RequiredReader {
    Predicate,     // ScalarPredicateReader<T>: In / NotIn / Range
    PatternMatch,  // PatternMatchReader: the LIKE family
    TextMatch,     // TextMatchReader
    Ngram,         // NgramReader     — candidate family, caps.exact == false
    Spatial,       // SpatialReader   — candidate family, caps.exact == false
    Null,          // NullReader      — unconditional, carries no caps bit
    ValueLookup,   // ScalarValueReader<T>
    JsonPath,      // JsonIndexReader
};

// Everything the decision needs from the EXPRESSION side. All of it is known at
// plan time; none of it requires touching an index object.
struct ExprIndexRequirement {
    FieldId field_id;
    RequiredReader reader{RequiredReader::Predicate};

    // The value type the predicate is expressed in. For an element-level ARRAY
    // expression this is the element type, not `DataType::ARRAY`.
    DataType value_type{DataType::NONE};

    // Empty unless the expression addresses a JSON path. A per-path cast index
    // is registered in the inventory as `(field, path)` (§5.7).
    std::string json_path;

    // True when the expression evaluates per array element rather than per row.
    bool element_level{false};

    // False when exec is willing to take a candidate superset and refine it
    // itself (the normal case for the ngram / spatial / nested-ARRAY-equality
    // family, §5.6 "the shared shape of the candidate family"). True only where
    // no refine step exists downstream, in which case an index with
    // `caps.exact == false` is unusable and the answer is `RawData`.
    bool accepts_candidates{true};

    // Short-circuit paths that never reach the index inventory at all and
    // therefore must never pin (§4.3, "step 1 must not pin"): today's comment
    // in the code says the same thing — "the short-circuit paths
    // (TextIndex/PkIndex/JsonStats) and the RawData path never call it, so
    // scalar index cells stay cold under tiered storage".
    bool is_pk_compare{false};
    bool json_stats_eligible{false};

    // The upstream operator already handed down candidate offsets
    // (`has_offset_input_`). Some paths are unavailable then — e.g. today's
    // `CanUseNgramIndex()` (exec/expression/UnaryExpr.cpp:2346) refuses.
    bool has_offset_input{false};
};

struct ExecPathDecision {
    ExprExecPath path{ExprExecPath::RawData};

    // Which inventory entry to pin, valid when `path != RawData` and the path
    // is index-backed. The caller passes this straight to
    // `segcore::IndexInventory::Pin*` — ONCE, for the whole expression node.
    segcore::IndexKey key;

    // Mirrors the chosen entry's `caps.exact == false`. The consumer MUST run
    // the refine step (see CandidateRefine.h) when this is set; the index
    // returned a superset by contract.
    bool needs_refine{false};

    // Mirrors the chosen entry's `caps.nested`. The bitmap the reader returns
    // is in ELEMENT coordinates and its size is the element count, not the row
    // count (§5.8). Folding to rows is exec's, and WHERE it folds is the plan's
    // decision — see CandidateRefine.h.
    bool element_level_result{false};
};

// THE decision. No pin, no cast, no try/catch — a pure function of the two
// arguments. §10 rule 3b lints that this function contains no pin call.
//
// TODO: move existing logic here — the branches to fold in are
//   exec/expression/Expr.h:2807            (base: has-index + json path check)
//   exec/expression/UnaryExpr.cpp:2039     (text match / PK / json stats /
//                                           JSON array literal / int64 safety /
//                                           ARRAY literal type match, then the
//                                           per-type `CanUseIndexForOp` refine)
//   exec/expression/TermExpr.cpp,
//   exec/expression/BinaryRangeExpr.cpp,
//   exec/expression/NullExpr.cpp:126,
//   exec/expression/ExistsExpr.cpp,
//   exec/expression/JsonContainsExpr.h:495,
//   exec/expression/GISFunctionFilterExpr.cpp:196,
//   exec/expression/GISConjunctExpr.h:270,
//   exec/expression/BloomFilterExpr.h:266,
//   exec/expression/RoaringFilterExpr.h:64
// Each of them is currently "call the base, then downgrade to RawData if the
// pinned concrete index turns out not to support this op". The downgrade half
// becomes a caps read; the literal-shape half (e.g. "an ARRAY literal whose
// element types do not match the index's") is expression-side and stays in the
// caller that fills `ExprIndexRequirement`.
ExecPathDecision
DetermineExecPath(const ExprIndexRequirement& req,
                  const segcore::FieldIndexCapability& caps);

// Per-CALL capability, which by definition cannot live in `ReaderCaps`.
//
// `NgramReader::CanHandle(literal, op)` (§5.4) answers "is this literal long
// enough for my min_gram" — a property of the literal, not of the index — so it
// is asked AFTER the pin, on the pinned interface. It is still self-description
// and still not a throw (§3 principle 3). `caps.ngram_candidates` decides
// whether to pin at all; `CanHandle` decides whether this particular call uses
// the pin.
//
// Today the same two-level structure exists but the outer level pins:
// `CanUseNgramIndex()` (exec/expression/UnaryExpr.cpp:2346) reads
// `pinned_ngram_index_` and then calls `CanHandleLiteral`.
//
// The same shape covers FMIndex's per-literal cost guard, which today rides
// `ShouldUseOp(op, pattern)` on the pinned index (`Expr.h:2702` passes the
// literal through). It is a cost decision on a concrete literal, so it belongs
// on the pinned interface too, not in `ReaderCaps`.

}  // namespace milvus::exec
