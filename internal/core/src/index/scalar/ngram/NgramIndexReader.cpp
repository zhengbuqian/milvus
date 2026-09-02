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

#include "index/scalar/ngram/NgramIndexReader.h"

#include <utility>

namespace milvus::index {

NgramIndexReader::NgramIndexReader(
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::vector<size_t> null_offsets,
    uintptr_t min_gram,
    uintptr_t max_gram,
    size_t avg_row_size,
    std::string nested_path)
    : engine_(std::move(engine)),
      null_offsets_(std::move(null_offsets)),
      min_gram_(min_gram),
      max_gram_(max_gram),
      avg_row_size_(avg_row_size),
      nested_path_(std::move(nested_path)) {
}

NgramIndexReader::~NgramIndexReader() = default;

ReaderCaps
NgramIndexReader::Caps() const {
    return ReaderCaps{.ngram_candidates = true, .exact = false};
}

Domain
NgramIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
NgramIndexReader::Count() const {
    // TODO: `engine_->count()` (was InvertedIndexTantivy.h:145-148, inherited).
}

DataType
NgramIndexReader::ValueType() const {
    return DataType::VARCHAR;
}

int64_t
NgramIndexReader::MemoryUsage() const {
    // TODO: move existing logic here (see InvertedIndexTantivy.h:219-231, which
    // ngram inherited).
    //
    // KNOWN GAP THAT THIS MOVE CLOSES: `NgramInvertedIndex::Load`
    // (NgramInvertedIndex.cpp:291-323) does not call the base `Load`, and so
    // never calls `ComputeByteSize()` — the V2 ngram load path leaves the
    // cached size at whatever it was. Computing at construction removes the
    // possibility.
}

ResourceUsage
NgramIndexReader::CellByteSize() const {
    // See §12.3.
}

bool
NgramIndexReader::CanHandle(std::string_view literal, PatternOp op) const {
    // TODO: move existing logic here (see NgramInvertedIndex.cpp:801-837
    // CanHandleLiteral), taking the native PatternOp instead of
    // proto::plan::OpType.
}

void
NgramIndexReader::Candidates(std::string_view literal,
                             PatternOp op,
                             TargetBitmap& candidates) const {
    // TODO: move existing logic here (see NgramInvertedIndex.cpp:900-990
    // ExecutePhase1) — literal derivation per op (`split_by_wildcard` :928 for
    // Match, `extract_literals_from_regex` :938-944 for RegexMatch, the literal
    // itself :953 otherwise), then either the batch path
    // (`ngram_match_query` :961-965) or the iterative path
    // (`ngram_tokenize` + ApplyIterativeNgramFilter :968-972).
    //
    // ExecutePhase1 was ALREADY layer-clean — it never touched
    // `exec::SegmentExpr`. Only Phase 2 did. So this move is a rename plus an
    // enum change; the deletion happens next door.
}

TargetBitmap
NgramIndexReader::IsNull() const {
    // TODO: move existing logic here (see InvertedIndexTantivy.cpp:347-378,
    // inherited by ngram today).
}

TargetBitmap
NgramIndexReader::IsNotNull() const {
    // TODO: move existing logic here (see InvertedIndexTantivy.cpp:380-411).
}

bool
NgramIndexReader::ShouldUseBatchStrategy(double pre_filter_hit_rate) const {
    // TODO: move existing logic here (see NgramInvertedIndex.cpp:886-898, plus
    // the threshold constants at :62-76).
}

void
NgramIndexReader::ApplyIterativeNgramFilter(
    const std::vector<std::string>& sorted_terms,
    size_t total_count,
    TargetBitmap& bitset) const {
    // TODO: move existing logic here (see NgramInvertedIndex.cpp:858-884).
}

// MOVES TO exec, NOT HERE — the whole reason this family is being reshaped:
//   `ExecutePhase2`      NgramInvertedIndex.cpp:992-1154
//   `ExecuteQueryForUT`  NgramInvertedIndex.cpp:1156-1190 (Phase1+Phase2 in one
//                        call, tests only — it becomes a test helper on the
//                        exec side, since it is exec that now owns Phase 2)
// Both branches of Phase 2 (JSON at :1016-1098, string/varchar at :1099-1153)
// are the same shape: iterate the raw values of the candidate range and
// re-evaluate the predicate. In exec that is `ProcessDataChunkForRange` plus
// the existing per-op matchers — no new machinery.

}  // namespace milvus::index
