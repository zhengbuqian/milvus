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
#include <string_view>
#include <vector>

#include "common/JsonCastType.h"
#include "common/Types.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/JsonIndexReader.h"
#include "index/contracts/NullReader.h"
#include "index/contracts/PatternMatchReader.h"
#include "index/contracts/ScalarPredicateReader.h"
#include "tantivy-wrapper.h"

// The READERS of the JSON path-index family.
//
// See 01-scalar-index.md §5.7 (`JsonIndexReader` — path addressing), §12.4 (the
// cast-type vocabulary question), and §8's `JsonFlatIndex` row.
//
// SCOPE, per §5.7: this family covers only indexes built ON VALUES — the
// per-path cast indexes and `JsonFlatIndex` (one tantivy index over every path
// of the field). JSON SHREDDING IS NOT HERE and never will be: §1 rules that
// `JsonKeyStats` is a COLUMN LAYOUT, not an index (its shredded side is a full
// column scan; its shared side stores byte offsets inside a BSON blob and
// cannot be consumed away from that column). In W1 it moves to
// `segcore/json_stats/`; its final home for the sub-columns is
// columnar-format. Nothing in this directory routes to it.
//
// TWO CLASSES, MIRRORING §5.7'S TWO LEVELS:
//   `JsonFlatIndexReader`      — the routing face. `Resolve(path, cast_type)`
//                                and `Exists(path)`. Defines NO query semantics
//                                of its own.
//   `JsonPathPredicateReader<T>` — an ordinary predicate reader BOUND TO ONE
//                                PATH, which is what `Resolve` hands back.

namespace milvus::index {

// A predicate face over one JSON path. Was
// `JsonFlatIndexQueryExecutor<T>` (`JsonFlatIndex.h:33-726`).
//
// ==========================================================================
// WHAT CHANGES, AND WHY IT IS THE SHARPEST CASE IN THE WHOLE WAVE.
//
// `JsonFlatIndexQueryExecutor<T> : InvertedIndexTantivy<T>` is a READER that
// inherits a Builder AND a Loader, and then has to NEUTRALIZE them: it never
// calls a base constructor, it reaches into its parent index and aliases
// `wrapper_` and `null_offset_` (`JsonFlatIndex.h:797-798`, legal only through
// a `friend` declaration at `:732-733`), and its destructor nulls `wrapper_`
// (`:41`) so that the inherited base destructor does not tear down a tantivy
// reader it does not own. Calling any inherited build or load method on it
// would corrupt the parent.
//
// Composition removes the whole problem: this class holds a `shared_ptr` to the
// engine — which also makes the borrow explicit and keeps the parent alive,
// where today the lifetime is a comment ("Must outlive index_ptr",
// exec/expression/Expr.h:2488).
// ==========================================================================
template <typename T>
class JsonPathPredicateReader final : public IndexReaderBase,
                                      public ScalarPredicateReader<T>,
                                      public PatternMatchReader,
                                      public NullReader {
 public:
    JsonPathPredicateReader(
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::string tantivy_path,
        std::shared_ptr<const std::vector<size_t>> null_offsets,
        bool comparable_value_mask);

    ~JsonPathPredicateReader() override;

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

    TargetBitmap
    In(size_t n, const T* values) const override;

    TargetBitmap
    NotIn(size_t n, const T* values) const override;

    TargetBitmap
    Range(const T& value, CompareOp op) const override;

    TargetBitmap
    Range(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const override;

    TargetBitmap
    PatternMatch(std::string_view pattern, PatternOp op) const override;

    TargetBitmap
    IsNull() const override;

    // For a JSON path this is "the path holds a value of a type comparable with
    // T", not merely "the row is non-null" — see the numeric-widening note
    // below and `JsonFlatIndex.h:117-125`.
    TargetBitmap
    IsNotNull() const override;

 private:
    // Numeric widening: tantivy stores JSON numbers in three disjoint columns
    // (u64 / i64 / f64), so one typed predicate must be OR-ed across all three.
    // This is the bulk of the old executor (`JsonFlatIndex.h:221-707`, roughly
    // twenty private helpers) and it moves across unchanged — it is genuine
    // engine-shaped work, not lifecycle confusion.
    TargetBitmap
    TermBitset(size_t n, const T* values) const;

    TargetBitmap
    ComparableValueBitset() const;

    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;

    // Dotted tantivy form, e.g. `a.b.c`. The JSON-pointer -> dotted rewrite
    // happens once, in `JsonFlatIndexReader::Resolve` (was
    // `create_executor`, `JsonFlatIndex.h:752-764`).
    std::string tantivy_path_;

    // SHARED, not copied. `JsonFlatIndex.h:798` copies the entire null-offset
    // vector into every executor — i.e. once per expression node per query.
    std::shared_ptr<const std::vector<size_t>> null_offsets_;

    bool comparable_value_mask_{true};
};

// The routing face over one JSON field. Was `JsonFlatIndex`
// (`JsonFlatIndex.h:731-788`), whose own comment (`:728-730`) already said it
// "must not be used to execute queries" — it inherited
// `InvertedIndexTantivy<std::string>` purely to reuse build and load.
class JsonFlatIndexReader final : public IndexReaderBase,
                                  public JsonIndexReader {
 public:
    JsonFlatIndexReader(
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::string field_path_prefix,
        std::vector<size_t> null_offsets);

    ~JsonFlatIndexReader() override;

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

    // ---- JsonIndexReader (§5.7) -----------------------------------------

    // Returns the type-erased root; the consumer does ONE sibling cast to
    // `ScalarPredicateReader<T>` (or `PatternMatchReader`), guided by
    // `cast_type`. Null means "no index at this path" and exec falls back to a
    // column scan — where that scan lands (a shredded typed sub-column or the
    // raw JSON column) is columnar-format's business, not this family's.
    //
    // The vocabulary is `JsonCastType`, not `milvus::DataType`. §5.7's snippet
    // writes `DataType`, but §12.4 flags that as a bet rather than a
    // transcription: `JsonCastType` (`common/JsonCastType.h:25`) is a closed
    // six-value enum cut to TANTIVY's type system (it has `ToTantivyType()`),
    // while `DataType` can express GEOMETRY. Choosing `DataType` here would
    // already be choosing §12.4 option 1. The contract layer took the
    // conservative option and so does this.
    std::shared_ptr<const IndexReaderBase>
    Resolve(std::string_view path, JsonCastType cast_type) const override;

    // Answered from the index alone, with no read-back of the column
    // (`json_exist_query`) — which is precisely what distinguishes an index
    // built on VALUES from the BSON locator inverted index that must re-read
    // the blob (`ExistsExpr.cpp:264-270`) and is therefore column layout (§1).
    TargetBitmap
    Exists(std::string_view path,
           JsonValueType type = JsonValueType::Any) const override;

    std::vector<JsonCastType>
    CastTypesOf(std::string_view path) const override;

 private:
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::string field_path_prefix_;
    std::shared_ptr<const std::vector<size_t>> null_offsets_;
};

}  // namespace milvus::index
