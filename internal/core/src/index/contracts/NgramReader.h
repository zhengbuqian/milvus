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

#include <string_view>

#include "common/Types.h"
#include "index/contracts/PatternMatchReader.h"

// The ngram candidate family: phase-1 candidate generation only. Pure mixin,
// does NOT derive from `IndexReaderBase` (§4).
//
// See core_refactor/01-scalar-index.md §5.4.
//
// Results are a SUPERSET: `ReaderCaps::ngram_candidates = true` and
// `ReaderCaps::exact = false`. exec verifies.
//
// PHASE 2 IS DELETED FROM THE INDEX. Today `NgramInvertedIndex::ExecutePhase2(
// literal, op, exec::SegmentExpr*, ...)` (`NgramInvertedIndex.h:68,93`) is in
// substance "re-evaluate the original values of the candidate rows": fetching
// goes through columnar-format's `Take`/`Scan`, and evaluation is exec's
// expression kernel to begin with. Splitting it out removes the `index -> exec`
// reverse edge (§10 rule 1) and needs no callback — the `ValueFetcher` callback
// once proposed in segcore 11-cross-cutting §2.4 is not needed either.
//
// This is not a new invention: it aligns ngram with geometry, where
// `RTreeIndex::QueryCandidates` already produces candidates and
// `PhyGISRefineConjunctExpr` does the exact verification. ngram is that pattern
// LEFT UNFINISHED (§5.4, §2.2's "positive example" row).
//
// `PatternOp` is reused from PatternMatchReader.h — the ngram index answers the
// same LIKE-family operators, only approximately. The include is FOR THE ENUM
// ONLY; there is no inheritance between faces.

namespace milvus::index {

class NgramReader {
 public:
    virtual ~NgramReader() = default;

    // Can this index serve this literal at all (e.g. literal length >= min_gram)?
    // A capability question that is per-CALL, not per-index, so it cannot live in
    // `ReaderCaps` — but it is still self-description, not a throw (§3 rule 3).
    virtual bool
    CanHandle(std::string_view literal, PatternOp op) const = 0;

    // Phase 1: candidate generation. The result is AND-merged into `candidates`
    // (the caller initializes it non-empty). Semantically a superset.
    // Requires CanHandle(literal, op) == true.
    virtual void
    Candidates(std::string_view literal,
               PatternOp op,
               TargetBitmap& candidates) const = 0;
};

}  // namespace milvus::index
