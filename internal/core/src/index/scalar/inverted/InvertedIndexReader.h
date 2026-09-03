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
#include <string_view>
#include <vector>

#include "common/Types.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/NullReader.h"
#include "index/contracts/PatternMatchReader.h"
#include "index/contracts/ScalarPredicateReader.h"
#include "tantivy-wrapper.h"

// The READER of the inverted (tantivy) family.
//
// See 01-scalar-index.md §5.1, §5.2, and §8's first mapping row:
//   | `InvertedIndexTantivy<T>` | `ScalarPredicateReader<T>` + `PatternMatchReader` |
//   | the tantivy wrapper drops to an internal engine, no longer a base class |
//
// THIS CLASS IS NO LONGER A BASE CLASS. Today three families inherit it —
// `TextMatchIndex`, `NgramInvertedIndex`, `JsonFlatIndex` (and, through
// `JsonScalarIndexWrapper`, the JSON path indexes) — purely to reuse the
// tantivy plumbing, and each inherits `In`/`Range`/`Reverse_Lookup` it must
// never answer. Every one of them now COMPOSES `milvus::tantivy::
// TantivyIndexWrapper` directly, exactly as `BsonInvertedIndex`
// (`json_stats/bson_inverted.h:42`) already does. §10 rule 3 lints for it.
//
// INTERFACES: `ScalarPredicateReader<T>` + `PatternMatchReader` + `NullReader`.
// NOT `ScalarValueReader<T>` — this family cannot produce values:
// `HasRawData()` is hardcoded false (`InvertedIndexTantivy.h:204-206`) and
// `Reverse_Lookup` is a `ThrowInfo(NotImplemented)` shell (`.h:209-212`).
//
//   > A LATENT INCONSISTENCY WORTH FIXING WHILE MOVING: the class does not
//   > override `SupportFastReverseLookup()`, whose base default is `true`
//   > (`ScalarIndex.h:171-174`) — so today it ADVERTISES cheap reverse lookup
//   > while throwing on it. Consumers of that predicate
//   > (`exec/expression/BloomFilterExpr.h:398`,
//   > `exec/expression/RoaringFilterExpr.h:179`) are protected only because
//   > they check `HasRawData()` too. In the new shape the contradiction cannot
//   > be written: no value interface, no `cheap_value_lookup` bit.

namespace milvus::index {

template <typename T>
class InvertedIndexReader final : public IndexReaderBase,
                                  public ScalarPredicateReader<T>,
                                  public PatternMatchReader,
                                  public NullReader {
 public:
    InvertedIndexReader(
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::vector<size_t> null_offsets,
        bool is_nested_index);

    ~InvertedIndexReader() override;

    // ---- IndexReaderBase (§4.2) ----------------------------------------

    ReaderCaps
    Caps() const override;

    // Element when this index was built over ARRAY elements rather than rows
    // (today's `is_nested_index_`, `InvertedIndexTantivy.h:410`). §5.8: nested
    // is not a family, it is a mode bit, and its only outward expression is
    // this coordinate system. The index NEVER folds elements to rows.
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

    // ---- ScalarPredicateReader<T> (§5.1) -------------------------------

    TargetBitmap
    In(size_t n, const T* values) const override;

    TargetBitmap
    NotIn(size_t n, const T* values) const override;

    // `CompareOp` is the contract layer's NATIVE enum, not `milvus::OpType`.
    // That alias is `proto::plan::OpType` (`common/Types.h:106`), and README §5
    // rule 2 keeps protobuf off contract signatures. Watch out for the name
    // collision while moving: `Range(const T&, OpType)` and
    // `PatternMatch(..., proto::plan::OpType)` in the same class today refer to
    // the SAME enum under two spellings.
    TargetBitmap
    Range(const T& value, CompareOp op) const override;

    TargetBitmap
    Range(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const override;

    // ---- PatternMatchReader (§5.2) -------------------------------------

    // `RegexMatch` really does arrive here (`InvertedIndexTantivy.h:261-276`),
    // which is why the contract's `PatternOp` carries it even though §5.2's
    // prose lists only four operators.
    TargetBitmap
    PatternMatch(std::string_view pattern, PatternOp op) const override;

    // ---- NullReader (§5) -----------------------------------------------

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    // Internal helper behind PatternMatch; was the protected virtual
    // `PatternQuery` (`ScalarIndex.h:283-286`), which existed only so that
    // subclasses could override one half of pattern matching. With no
    // subclasses it is a private detail.
    TargetBitmap
    PatternQuery(std::string_view pattern) const;

    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;

    std::vector<size_t> null_offsets_;

    bool is_nested_index_{false};

    // GONE, and worth naming: `folly::SharedMutexWritePriority mutex_`
    // (`InvertedIndexTantivy.h:379`) and `bool is_growing_` (`:406`). The mutex
    // guarded `null_offset_` against concurrent growing appends, and
    // `is_growing_` selected whether `IsNull`/`IsNotNull`/`NotIn` took it
    // (`InvertedIndexTantivy.cpp:370,403,456`). A sealed reader is immutable
    // (§5), so both disappear — the growing writer is a different object
    // (`GrowingTextIndex` / the growing scalar appender), not a mode of this one.
};

}  // namespace milvus::index
