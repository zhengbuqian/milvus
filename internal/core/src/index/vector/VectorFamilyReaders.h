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
#include <utility>
#include <vector>

#include "common/BitsetView.h"
#include "common/OffsetMapping.h"
#include "common/Types.h"
#include "index/contracts/VectorReaders.h"
#include "knowhere/expected.h"
#include "knowhere/index/index_node.h"

// VECTOR-FAMILY-LOCAL QUERY INTERFACES.
//
// See core_refactor/01-scalar-index.md §11.3 (vector re-homing) and §11.2 rule
// 2 (query interfaces are zero-shared between the families, so the vector
// family owns its own interface set).
//
// !! THESE FOUR INTERFACES ARE *NOT* IN `index/contracts/VectorReaders.h` !!
//
// The contract file declares exactly `VectorSearchReader` (Search / Iterators /
// RefineEnabled) and `VectorValueReader` (HasRawData / GetVector /
// GetSparseVector). That set does NOT cover everything today's consumers call
// on a `VectorIndex*`, so re-homing the existing surface with no behaviour
// change (§11.3: "the knowhere interaction moves across verbatim, benchmarks
// unchanged") needs the interfaces below.
//
// They are declared HERE, inside the vector family, rather than by editing
// contracts/ — the contract layer is owned elsewhere and the design rule is
// "report it, do not bend the contract" . Each interface records the production
// call sites that force it. THE CONTRACT OWNER HAS TO DECIDE whether these get
// folded into contracts/VectorReaders.h or stay family-local; until then, treat
// this file as the gap list, not as a settled contract.
//
// Knowhere types are used freely here — §12.1(c) rules that they may appear
// anywhere inside the vector family (and nowhere else, §10 rule 6).

namespace milvus::index {

// ---------------------------------------------------------------------------
// GAP 1 — index self-description beyond `IndexReaderBase`
// ---------------------------------------------------------------------------
//
// `IndexReaderBase` gives Caps / CoordDomain / Count / ValueType / MemoryUsage
// (§4.2). It does NOT give metric type, dim, or the knowhere index type, and it
// cannot: those are vector-only vocabulary and the base class is shared with
// the scalar families (§10 rule 6 forbids knowhere there — `MetricType` and
// `IndexType` are `knowhere::MetricType` / `knowhere::IndexType` aliases,
// `common/Types.h:684,687`).
//
// FORCED BY:
//   - `query/CachedSearchIterator.cpp:67`  -> `index.GetMetricType()`
//   - `segcore/ChunkedSegmentSealedImpl.cpp:3883` -> `vec_index->GetMetricType()`
//
// `PrepareSearchParams` is the sharper half of this gap. `VectorSearchReader::
// Search` takes the narrow `VectorSearchParams` and prepares the knowhere Json
// internally, but `VectorSearchReader::Iterators` takes an ALREADY PREPARED
// `knowhere::Json` — and the preparation (metric + topk + trace ids folded into
// the search config) is today a method ON THE INDEX,
// `VectorIndex::PrepareSearchParams` (`index/VectorIndex.h:171-190`), called by
// exec at `exec/operator/Utils.h:122` right before `VectorIterators`. Something
// has to keep offering it, or `Iterators` is uncallable.
class VectorIndexMetaReader {
 public:
    virtual ~VectorIndexMetaReader() = default;

    virtual MetricType
    Metric() const = 0;

    virtual IndexType
    KnowhereIndexType() const = 0;

    virtual int64_t
    Dim() const = 0;

    // Re-homed verbatim from `VectorIndex::PrepareSearchParams`
    // (`index/VectorIndex.h:171-190`): copy `search_params_`, set METRIC_TYPE and
    // TOPK, and fold the trace/span ids in when the trace context carries them.
    // Takes the narrow §12.1(a) parameter type, not `SearchInfo`.
    virtual knowhere::Json
    PrepareSearchParams(const VectorSearchParams& params) const = 0;
};

// ---------------------------------------------------------------------------
// GAP 2 — nullable vectors: the logical->physical offset mapping
// ---------------------------------------------------------------------------
//
// A nullable vector field does not hand its null rows to knowhere, so the index
// lives in a PHYSICAL coordinate system that is denser than the segment's row
// space, and every consumer of a search result has to map back. The mapping
// object lives in the index (it is built at build time from the validity input)
// and is read by the query layer BY REFERENCE.
//
// FORCED BY:
//   - `query/SearchOnSealed.cpp:93`    -> `vec_index->GetOffsetMapping()`
//   - `query/SearchOnIndex.cpp:48`     -> `indexing.GetOffsetMapping()`
//   - `exec/operator/Utils.h:129-134`  -> `&index.GetOffsetMapping()` is stored
//                                         raw inside `ChunkMergeIterator`
//                                         (§12.1(b) — deliberately untouched)
//   - `segcore/ChunkedSegmentSealedImpl.cpp:3757,3821,3890` and
//     `segcore/SegmentGrowingImpl.cpp:3337` -> `vec_index->HasValidData()`
//   - `query/SearchOnSealed.cpp:101`, `query/SearchOnIndex.cpp:79`
//                                      -> `offset_mapping.GetValidCount() == 0`
//
// NOTE THE SHAPE PROBLEM this interface inherits and does not fix: it hands out
// a reference to state owned by the reader, and §12.1(b) has already ruled that
// the resulting raw pointer in `SearchResult::vector_iterators_` keeps no pin.
// Refactor phase 1 does not touch that (see the long note in
// contracts/VectorReaders.h).
class VectorNullableReader {
 public:
    virtual ~VectorNullableReader() = default;

    virtual bool
    HasValidData() const = 0;

    virtual int64_t
    ValidCount() const = 0;

    virtual bool
    IsRowValid(int64_t logical_offset) const = 0;

    virtual int64_t
    PhysicalOffset(int64_t logical_offset) const = 0;

    virtual int64_t
    LogicalOffset(int64_t physical_offset) const = 0;

    // Borrowed; valid as long as the reader is pinned. Re-homed from
    // `VectorIndex::GetOffsetMapping()` (`index/VectorIndex.h:238-241`).
    virtual const milvus::OffsetMapping&
    OffsetMapping() const = 0;
};

// ---------------------------------------------------------------------------
// GAP 3 — distance recomputation for the refine path
// ---------------------------------------------------------------------------
//
// FORCED BY: `segcore/ChunkedSegmentSealedImpl.cpp:6397`
// (`ChunkedSegmentSealedImpl::CalcDistByIDs`), reached from the search-result
// refine path. Today it is `VectorIndex::CalcDistByIDs`
// (`index/VectorIndex.h:95-105`) with an `expected`-returning default that says
// "not supported for current index type" — an `expected` error, not a throw, so
// it does not violate §3 principle 3; but it IS capability-by-return-value, and
// the family that cannot do it should simply not implement this interface. That
// is why it is a separate mixin rather than three more methods on
// `VectorSearchReader`.
class VectorRefineReader {
 public:
    virtual ~VectorRefineReader() = default;

    virtual knowhere::expected<knowhere::DataSetPtr>
    CalcDistByIDs(const knowhere::DataSetPtr& query_dataset,
                  const BitsetView& bitset,
                  const int64_t* labels,
                  size_t labels_len,
                  bool is_cosine,
                  milvus::OpContext* op_ctx) const = 0;
};

// ---------------------------------------------------------------------------
// GAP 4 — embedding lists (VECTOR_ARRAY)
// ---------------------------------------------------------------------------
//
// FORCED BY: `segcore/ChunkedSegmentSealedImpl.cpp:3924`
// (`vec_index->GetEmbListByIds(ids_ds, metric_type)`).
//
// It is the embedding-list counterpart of `VectorValueReader::GetVector`:
// given element ids, return the concatenated raw vectors plus a count+1 offsets
// array. Today `VectorIndex::GetEmbListByIds` (`index/VectorIndex.h:118-123`)
// defaults to `ThrowInfo(NotImplemented)` — THE EXACT PATTERN §3 principle 3
// and §10 rule 4 outlaw. Separating it into its own mixin is what removes the
// throw: a family that has no embedding lists does not inherit this interface,
// and `ReaderCaps` needs no bit for it (the caps struct is scalar-shaped today;
// see the report).
class EmbeddingListReader {
 public:
    virtual ~EmbeddingListReader() = default;

    virtual std::pair<std::vector<uint8_t>, std::vector<size_t>>
    GetEmbListByIds(const DatasetPtr& dataset,
                    const std::string& metric_type) const = 0;
};

}  // namespace milvus::index
