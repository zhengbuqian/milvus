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
#include <functional>

#include "common/Types.h"

// The candidate family: index yields a superset, EXEC VERIFIES.
//
// See core_refactor/01-scalar-index.md §5.4 (ngram), §5.6 (spatial and "the
// shared trait of the candidate family"), §5.8 (nested ARRAY equality) and §9
// (the consumer table).
//
// THREE INSTANCES OF ONE PATTERN. §5.6 last bullet: `SpatialReader`,
// `NgramReader` and ARRAY equality over a nested index "are three instances of
// the same pattern — the index gives a superset, exec verifies exactly. They
// share the `caps.exact == false` semantic convention and the single bitmap
// output shape, so the consumer's skeleton is identical (take candidates ->
// fetch the original values -> re-evaluate). They differ only in the coordinate
// system the candidates live in: the first two are rows, nested is elements,
// which must first be folded by exec's projection."
//
// EXACT VERIFICATION LIVES IN EXEC, NOT IN THE INDEX. That is the whole point.
//
// -------------------------------------------------------------------------
// THE REFERENCE IMPLEMENTATION IS GEOMETRY, AND IT IS ALREADY CORRECT (§2.2
// "positive example" row, §5.6). `RTreeIndex::QueryCandidates` emits candidates
// only; the exact spatial relation is decided by `PhyGISCoarseConjunctExpr` /
// `PhyGISRefineConjunctExpr` (exec/expression/GISConjunctExpr.h:120,215 and
// GISFunctionFilterExpr.cpp:480-560). Refactor phase 1 changes only the
// contract underneath it: `SpatialReader::Candidates(SpatialOp, const
// Geometry&)` returning a `TargetBitmap` instead of `QueryCandidates(proto
// GISOp, Geometry, std::vector<int64_t>&)`. The coarse/refine skeleton itself
// does not move.
//
// (Two things worth knowing about that contract change. The proto enum becomes
// the native `index::SpatialOp` because README §5 rule 2 keeps pb off contract
// signatures. And the output becomes a bitmap because §5 fixes ONE output shape
// for every interface: selectivity is a runtime property of the query, not a
// static property of a family, and of the two shapes the bitmap is the one that
// does not explode when it degenerates.)
//
// -------------------------------------------------------------------------
// NGRAM IS THE UNFINISHED VERSION OF THE SAME PATTERN, AND IT DRAGS A REVERSE
// EDGE (§5.4, §10 rule 1).
//
//   Today:  NgramInvertedIndex::ExecutePhase1(literal, op, candidates)
//           NgramInvertedIndex::ExecutePhase2(literal, op,
//                                             exec::SegmentExpr*,   <-- HERE
//                                             candidates, offset, batch_size)
//           (index/NgramInvertedIndex.h:68,93; index/NgramInvertedIndex.cpp
//            includes exec/expression/Expr.h at :33 — one of the three
//            production `index -> exec` includes refactor phase 1 must zero.)
//
//   After:  Phase 1 = `index::NgramReader::Candidates(literal, op, candidates)`
//           Phase 2 = exec: fetch the candidates' original values through
//                     columnar-format `Scan`/`Take` and re-run the ordinary
//                     expression kernel on them.
//
// Verification needs the original values, and fetching values is
// columnar-format's job while evaluating a predicate is already the expression
// kernel's — so nothing has to be invented, and NO CALLBACK IS NEEDED either
// (the `ValueFetcher` callback that segcore-chapter 11 §2.4 once proposed is
// unnecessary once the split is drawn here).
//
// The call sites to move: `PhyUnaryRangeFilterExpr::ExecuteNgramPhase1` /
// `ExecuteNgramPhase2` (exec/expression/UnaryExpr.cpp:2355,2371) and
// `ExecNgramMatch` (`:2391`), which today passes `this` into the index.
//
// -------------------------------------------------------------------------
// NESTED ARRAY EQUALITY (§9 "exec ARRAY equality", §5.8).
//
//   Today:  `PhyUnaryRangeFilterExpr::ExecArrayEqualForIndex`
//           (exec/expression/UnaryExpr.cpp:804,807) uses
//           `ScalarIndex::InApplyCallback` to walk hits element by element,
//           intersects them in an `unordered_set`, converts coordinates with a
//           `to_row_offset` lambda, and finally verifies with `is_same_array`.
//
//   After:  the index returns an ELEMENT-LEVEL `In()` bitmap; exec does
//           `inplace_and` per element with its 1% early exit, folds to rows
//           through the projection below, then verifies with `is_same_array`.
//           `InApplyCallback` and the `unordered_set` disappear;
//           `to_row_offset` merges into the projection.
//
// Why `InApplyCallback` is not in the contract (§5.1): it exists to "avoid
// materialising a full bitmap", but both implementations do
// `TargetBitmap bitset(Count()); terms_query(...);
// apply_hits_with_callback(...)` — they materialise the full bitmap anyway
// (`InvertedIndexTantivy.cpp:428` still carries "todo: could push-down the
// callback to tantivy query"). Its sibling `InApplyFilter` has ZERO production
// call sites. The early exit is just as expressible with `In()` + a bitmap AND,
// and faster than the current set intersection.

namespace milvus::exec {

// The one skeleton all three instances share.
//
// `candidates` arrives from the reader as a superset in the reader's own
// coordinate system, and is narrowed IN PLACE to the exact answer. `verify` is
// called once per set bit with that bit's coordinate; returning false clears
// the bit.
//
// TODO: move existing logic here — the three verification bodies are
//   spatial : exec/expression/GISFunctionFilterExpr.cpp:480-560
//             (`PhyGISRefineConjunctExpr`, the model to copy)
//   ngram   : index/NgramInvertedIndex.cpp `ExecutePhase2` — the body moves OUT
//             of index and into exec; this is what deletes the `index -> exec`
//             include.
//   array   : exec/expression/UnaryExpr.cpp `ExecArrayEqualForIndex`
//             (`is_same_array` step)
// The value fetch under all three is columnar-format's `Take` / `Scan`
// (`ChunkedColumnScanCommon.h`, PR #51504), NOT a segcore accessor.
void
RefineCandidates(TargetBitmap& candidates,
                 const std::function<bool(int64_t coord)>& verify);

// -------------------------------------------------------------------------
// ELEMENT -> ROW PROJECTION. NOT IMPLEMENTED IN THIS REFACTOR PHASE — READ
// BEFORE ADDING A FOURTH COPY.
//
// A nested index works in ELEMENT coordinates: it reports
// `CoordDomain() == index::Domain::Element` and the bitmap it returns is sized
// by the element count, not the row count (§4.2, §5). The existential fold from
// elements to rows is exec's, AND WHERE IT HAPPENS IS DECIDED BY THE PLAN,
// because both directions are wrong for some query (§5.8):
//
//   FOLDING TOO EARLY IS WRONG for CORRELATED element predicates over a struct
//   array. `struct[*].a == 1 AND struct[*].b == 2` means
//   `exists i: (a[i]=1 and b[i]=2)`. Fold each side to rows first and you get
//   `(exists i: a[i]=1) and (exists j: b[j]=2)` — the two elements may differ,
//   so rows match that should not.
//
//   FOLDING TOO LATE IS WRONG for UNCORRELATED predicates over one array.
//   `contains(1) AND contains(2)` means `exists i: x[i]=1 and exists j: x[j]=2`.
//   AND-ing at element level demands one element be both 1 and 2, so rows are
//   missed. `NOT contains(1)` is the same trap: the correct meaning is
//   `not exists i: x[i]=1`, while negating at element level yields "some element
//   differs from 1".
//
// One nested index must serve both, and only the plan can tell them apart —
// hence the reader delivers element-level results and never folds. This is a
// description of today's architecture, not a new design: `array_offsets` is
// fetched by EXEC itself (`segment_->GetArrayOffsets(...)`,
// exec/expression/JsonContainsExpr.cpp:2393) and the index never sees it.
//
// TODAY THE FOLD IS THREE IMPLEMENTATIONS AT THREE GRANULARITIES:
//   exec/expression/JsonContainsExpr.cpp:2408  `ForEachRowElementRange` — per row
//   exec/expression/UnaryExpr.cpp:745          `ElementIDToRowID`       — per element
//   exec/expression/Expr.h:2209                `ElementIDRangeOfRow`    — range slice
// and the two placement strategies each have their own machinery:
//   correlated   -> `PhyElementFilterBitsNode` stashes the offsets in
//                   `QueryContext`, evaluates at element level and marks
//                   `set_bitset_is_element_level(true)`
//                   (exec/operator/ElementFilterBitsNode.cpp:96-134); the fold
//                   happens as late as possible, in the boundary operators
//                   (`ProjectNode`'s `find_first_n_element`,
//                   exec/operator/VectorSearchNode.cpp:129,147,
//                   query/ExecPlanNodeVisitor.cpp:74,332).
//   uncorrelated -> `result &= query_in(...)`
//                   (exec/expression/JsonContainsExpr.cpp:2497-2500): every
//                   value folds to rows first, then rows are AND-ed. Earliest
//                   possible.
//
// REGULARISING THESE INTO ONE EXPLICIT PROJECTION OPERATOR, PLACED BY THE PLAN,
// IS REFACTOR PHASE 4 WORK AND IS DELIBERATELY NOT DONE HERE (§5.8 "what this
// phase converges" and §12.5). Refactor phase 1 only has to guarantee the index
// side delivers clean element-level results. §12.5 also records the condition
// that would pull it forward: if refactor phase 1 gives MORE families
// element-level output, the three scattered folds start meeting combinations
// they have never covered, and the operator must land at the end of refactor
// refactor phase 1 instead of in refactor phase 4. The check is the §8 family
// list: if no family newly gains element-level output, refactor phase 4 is the
// right home.
//
// WHAT NOT TO DO MEANWHILE: do not add a fourth fold site, and do not push the
// fold into the reader. §5.8 also rules out the seemingly cheapest option —
// storing row ids directly in the postings (folding at build time) — because
// that freezes the decision the plan has to make, permanently breaking
// correlated element predicates over struct arrays.
//
// MULTI-LEVEL NESTING IS NOT A ROADMAP ITEM, IT IS THE SHAPE (§5.8 last part).
// Wide-table modelling fixes every nested sub-column at
// `Array<Array<...<T>>>`, one `offsets` per level and one `validity` per
// nullable level, so the element->row map is A CHAIN OF OFFSETS, not one
// `IArrayOffsets`, and folding composes prefix sums level by level. The
// ownership does not change (the chain belongs to exec / columnar-format), and
// the reader's outward shape does not change either: it always emits a bitmap
// in the INNERMOST element coordinate system and `CoordDomain()` distinguishes
// only row from element, never depth. So when this operator is written, WRITE
// IT FOR N LEVELS WITH N=1 AS THE DEGENERATE CASE — do not bake in a
// single-level assumption.

}  // namespace milvus::exec
