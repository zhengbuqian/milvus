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
#include <mutex>
#include <string>

#include "common/Types.h"
#include "index/contracts/GrowingIndex.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/VectorReaders.h"
#include "index/growing/GrowingAppenderBase.h"
#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorMemReader.h"
#include "index/vector/VectorValidData.h"

// The growing VECTOR appender (interim index).
//
// See core_refactor/01-scalar-index.md §7.1 — "THE APPENDER INTERFACE IS NOT
// SCALAR-ONLY: it exists on the vector side today, and it carries the same
// problem as `IndexBase`" — plus §11.3's interface table.
//
// WHAT IT REPLACES: `VectorFieldIndexing::AppendSegmentIndexDense` /
// `AppendSegmentIndexSparse` (`segcore/FieldIndexing.cpp:418-568` /
// `:268-415`).
//
// WHY DENSE AND SPARSE COLLAPSE INTO ONE `Append`, and why it is not about the
// signature (§7.1). Read either method: the two are the same skeleton —
// accumulate to `build_threshold` rows of contiguous memory, `GenDataSet`,
// id-map validity, `BuildWithDataset`, and from then on `AddWithDataset` per
// batch. The real differences are three and none of them justifies two
// interfaces: dense takes `dim` from the schema while sparse gets it per batch
// (`new_data_dim`); dense is fixed-width and can `FastMemcpy` while sparse
// assigns `SparseRow` objects one by one; sparse additionally calls
// `SetIsSparse(true)`.
//
// THE ACTUAL DEFECT IS THAT EACH METHOD IS A BUILDER *AND* AN APPENDER:
//   - the `!built_` branch (`FieldIndexing.cpp:292-336` sparse,
//     `:444-489` dense) GATHERS `[0, build_threshold)` OUT OF THE WHOLE
//     `ConcurrentVector` and does a `BuildWithDataset` — a cold-start full
//     build, §6.1.1 form B+, which is BUILDER work;
//   - the `built_` branch (`:340-414` / `:492-567`) uses only this batch for
//     `AddWithDataset` — that, and only that, is the appender.
// The `const VectorBase*` (the whole column) in today's signature exists ONLY
// for the first branch; the second uses it solely to read the new batch's
// validity, i.e. a `const bool* valid`. So the migration order §7.1 prescribes
// is: move the cold-start branch to the Builder interface first (segcore
// gathers once from the column when it crosses the threshold, feeds
// `IndexBuilder<T>`, hands the artifact to this appender as its starting
// point), after which `Append` degenerates to the single method the contract
// declares.
//
// !! WHAT §7.1's TABLE CLAIMS AND THE CODE DOES NOT DO YET.
// The table pairs "immutable snapshot, concurrently shared" with "knowhere
// interim index generation swap" for the vector column. THERE IS NO GENERATION
// SWAP IN THE CODE: `index_` is one live `knowhere::Index` that
// `AddWithDataset` mutates in place, and `get_segment_indexing()` hands readers
// a raw pointer to that same object (`FieldIndexing.h:332-335`). Concurrency
// rests entirely on knowhere tolerating add-during-search, and `sync_with_index_`
// is a flag, not a watermark. So for THIS family §7's snapshot model is a real
// behavioural change (same finding as the RTree appender), and the options are
// the same: snapshot by handle copy at commit (cheap — `knowhere::Index` is
// ref-counted, but the underlying node is still shared and still mutating), a
// true generation swap (needs knowhere support), or accept "the snapshot aliases
// the live index" and weaken §5's immutability claim for growing. Not decided
// here. See the report.
//
// !! Line references point at the tree before refactor phase 1 (master
// e255009e01) for index/ paths; `segcore/FieldIndexing.*` references are to the
// current tree.

namespace milvus::index {

template <typename T>
class KnowhereGrowingVectorIndex final : public GrowingVectorIndex,
                                         public GrowingAppenderBase {
 public:
    // `build_threshold` is segcore's `VecIndexConfig::GetBuildThreshold()`
    // (`segcore/FieldIndexing.h:320-323`), INJECTED — the appender does not read
    // `SegcoreConfig` (§8, §10 rule 1: index must not include segcore).
    // `build_params` is the knowhere interim-index config that
    // `VectorFieldIndexing::get_build_params` assembles today
    // (`FieldIndexing.cpp:570-580`).
    KnowhereGrowingVectorIndex(DataType value_type,
                               IndexType index_type,
                               MetricType metric_type,
                               IndexVersion version,
                               int64_t dim,
                               int64_t build_threshold,
                               knowhere::Json build_params);

    ~KnowhereGrowingVectorIndex() override = default;

    // --- GrowingVectorIndex (§7.1, §11.3) -----------------------------------

    // `dim` is the ONLY difference between dense and sparse: dense passes the
    // schema dim, sparse passes this batch's dim.
    //
    // PRECONDITION AFTER THE §7.1 SPLIT: the cold-start build has already
    // happened through the Builder interface and its artifact was handed to
    // `AdoptBuiltIndex` below. `Append` is `AddWithDataset` and nothing else.
    void
    Append(int64_t reserved_offset,
           size_t n,
           const void* data,
           int64_t dim,
           const bool* valid) override;

    // Null until the threshold is crossed — §7's "an empty snapshot means none
    // yet", which is how the contract absorbs the one genuine difference between
    // the families: the scalar threshold is always 0
    // (`FieldIndexing.h:196-199`), the vector one comes from config
    // (`:320-323`).
    std::shared_ptr<const VectorSearchReader>
    ReaderSnapshot() const override;

    int64_t
    CommittedRows() const override;

    // --- GrowingAppenderBase ------------------------------------------------

    DataType
    ValueType() const override;

    std::string
    Family() const override;

    IndexReaderBasePtr
    ReaderSnapshotErased() const override;

    // --- the Builder hand-off (§7.1, §7 point 3) ----------------------------

    // Takes the cold-start artifact that segcore built through
    // `IndexBuilder<T>` once the threshold was crossed, and becomes appendable.
    // THIS METHOD IS THE SEAM BETWEEN THE TWO INTERFACES; it exists precisely
    // so that "build-in-place" and "growing" stop living in one method body.
    void
    AdoptBuiltIndex(KnowhereEngine engine,
                    VectorValidData valid,
                    int64_t covered_rows);

 private:
    void
    Publish(int64_t covered_rows);

    const DataType value_type_;
    const int64_t build_threshold_;
    knowhere::Json build_params_;

    // COMPOSED (§3 principle 2). Same engine type the sealed readers hold,
    // which is what lets the published snapshot simply be a
    // `VectorMemReader<T>` — the growing and sealed sides share the READER
    // implementation while their WRITE interfaces stay separate, which is
    // exactly the split §3 principle 1 is after.
    KnowhereEngine engine_;
    VectorValidData valid_;

    mutable std::mutex mtx_;
    std::shared_ptr<const VectorMemReader<T>> snapshot_;
    int64_t committed_rows_{0};
};

}  // namespace milvus::index
