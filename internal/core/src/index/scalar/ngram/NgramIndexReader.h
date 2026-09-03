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
#include <string>
#include <string_view>
#include <vector>

#include "common/Types.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/NgramReader.h"
#include "index/contracts/NullReader.h"
#include "tantivy-wrapper.h"

// The READER of the ngram family.
//
// See 01-scalar-index.md §5.4 ("`NgramReader` — the right cut for two-phase
// execution") and §8's `NgramInvertedIndex` row: "Phase 2 deleted, the
// `index -> exec` edge disappears".
//
// ==========================================================================
// PHASE 2 IS NOT HERE, AND THAT IS THE POINT.
//
// `NgramInvertedIndex::ExecutePhase2(literal, op, exec::SegmentExpr* segment,
// candidates, segment_offset, batch_size)` (`NgramInvertedIndex.h:90-96`,
// `.cpp:992-1154`) is what "verify the candidates by re-reading the original
// values" looks like when it is written inside the index. It forces
// `NgramInvertedIndex.cpp:33` to `#include "exec/expression/Expr.h"` — ONE OF
// THE THREE index -> {exec,segcore,query} edges this refactor phase zeroes
// (§10 rule 1).
//
// Everything Phase 2 actually needed from `SegmentExpr` was ONE method:
// `ProcessDataChunkForRange<T>(functor, res, offset, batch_size)`
// (`exec/expression/Expr.h:1946`) — a raw-value chunk iterator. Fetching values
// is columnar-format's `Scan`/`Take`, and evaluating a predicate over them is
// exec's expression kernel. Neither is an index concern, so Phase 2 moves to
// exec whole (§5.4, §9's "exec ngram" row) — and no `ValueFetcher` callback
// needs to be invented to carry it (§5.4).
//
// This is not a new invention: `RTreeIndex::QueryCandidates` + exec's
// `PhyGISRefineConjunctExpr` have had exactly this split for a long time. Ngram
// was the unfinished half of the same pattern (§5.4, §2.2's positive-example row).
// ==========================================================================
//
// INTERFACES: `NgramReader` + `NullReader`. Not `ScalarPredicateReader` — an
// ngram index answers no point or range predicate; not `PatternMatchReader` —
// its answer is a SUPERSET (`caps.exact = false`), and conflating it with an
// exact LIKE interface is what would let a caller skip the refine step.

namespace milvus::index {

class NgramIndexReader final : public IndexReaderBase,
                               public NgramReader,
                               public NullReader {
 public:
    NgramIndexReader(std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
                     std::vector<size_t> null_offsets,
                     uintptr_t min_gram,
                     uintptr_t max_gram,
                     size_t avg_row_size,
                     std::string nested_path);

    ~NgramIndexReader() override;

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

    // ---- NgramReader (§5.4) --------------------------------------------

    // Per-CALL capability, which is why it is a method and not a `ReaderCaps`
    // bit: whether this index can serve a literal depends on the literal
    // (length vs `min_gram_`, extractable sub-literals for a regex). Still
    // self-description rather than a throw (§3 principle 3).
    bool
    CanHandle(std::string_view literal, PatternOp op) const override;

    // Phase 1 only. AND-merged into `candidates`, which the caller has already
    // sized and initialized.
    void
    Candidates(std::string_view literal,
               PatternOp op,
               TargetBitmap& candidates) const override;

    // ---- NullReader (§5) -----------------------------------------------

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    // Cost policy: for a very selective pre-filter, tokenize and intersect
    // iteratively instead of running the full ngram match query.
    bool
    ShouldUseBatchStrategy(double pre_filter_hit_rate) const;

    void
    ApplyIterativeNgramFilter(const std::vector<std::string>& sorted_terms,
                              size_t total_count,
                              TargetBitmap& bitset) const;

    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::vector<size_t> null_offsets_;

    uintptr_t min_gram_{0};
    uintptr_t max_gram_{0};

    // Persisted alongside the index (`NGRAM_AVG_ROW_SIZE_FILE_NAME`,
    // NgramInvertedIndex.cpp:55) and used only by the cost policy above.
    size_t avg_row_size_{0};

    // Non-empty when the index is built on a JSON path rather than a column.
    std::string nested_path_;
};

}  // namespace milvus::index
