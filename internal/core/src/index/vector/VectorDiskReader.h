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
#include <utility>
#include <vector>

#include "common/BitsetView.h"
#include "common/QueryResult.h"
#include "common/Types.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/VectorReaders.h"
#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorFamilyReaders.h"
#include "index/vector/VectorValidData.h"
#include "knowhere/expected.h"
#include "knowhere/index/index_node.h"
#include "knowhere/sparse_utils.h"

// READER INTERFACE — DiskANN.
//
// See core_refactor/01-scalar-index.md §11.3. The query half of today's
// `VectorDiskAnnIndex<T>` (`index/VectorDiskIndex.h:192`).
//
// WHY DISKANN IS A SEPARATE READER RATHER THAN A FLAG ON THE MEMORY ONE: its
// whole lifecycle is different in the two places the design cares about —
// its BUILDER wants the raw data as a LOCAL FILE and explicitly not in memory
// (§6.1.1 form D, `CacheRawDataToDisk` -> `DISK_ANN_RAW_DATA_PATH`,
// `VectorDiskIndex.cpp:459-460`), and its ARTIFACT is a set of large local
// files that must never be fully resident (VectorDiskArtifact.h). The query
// interface is nearly the same shape, which is exactly the split §11.3
// predicts: same four interfaces, per-family interfaces inside them.
//
// !! CONTRACT GAP — `VectorValueReader` FORCES A THROW HERE.
// The contract bundles `GetVector` and `GetSparseVector` in one interface
// (contracts/VectorReaders.h). DiskANN has no sparse support: today
// `VectorDiskAnnIndex<T>::GetSparseVector` is
// `ThrowInfo(Unsupported, "get sparse vector not supported for disk index")`
// (`index/VectorDiskIndex.h:263-267`) — the precise pattern §3 principle 3
// forbids and §10 rule 4 lints for. Re-homing verbatim (§11.3) carries the
// throw across, so this skeleton keeps it AND flags it rather than silently
// inventing a fix. The fix is a contract-layer decision (split the sparse
// getter into its own mixin, or type the interface on the value shape); see the
// report.
//
// !! Line references point at the tree before refactor phase 1 (master
// e255009e01).

namespace milvus::index {

template <typename T>
class VectorDiskReader final : public IndexReaderBase,
                               public VectorSearchReader,
                               public VectorValueReader,
                               public VectorIndexMetaReader,
                               public VectorNullableReader,
                               public VectorRefineReader,
                               public EmbeddingListReader {
 public:
    // `search_beamwidth` is DiskANN's own load-time knob
    // (`VectorDiskIndex.h:298`), read out of the load config by
    // `VectorDiskLoader` and injected here — a per-family construction argument,
    // not a shared parameter bag (§11.2 rule 4).
    VectorDiskReader(KnowhereEngine engine,
                     VectorValidData valid,
                     uint32_t search_beamwidth)
        : engine_(std::move(engine)),
          valid_(std::move(valid)),
          search_beamwidth_(search_beamwidth) {
    }

    ~VectorDiskReader() override = default;

    // --- storage::LoadedArtifact -------------------------------------------

    // §12.3 hole, same as the memory family — and worse here: for a mmap /
    // on-disk artifact the "memory vs file" split of `ResourceUsage` is the whole
    // question, and today it is decided by an `enable_mmap` branch in the
    // translator rather than by the family.
    milvus::ResourceUsage
    CellByteSize() const override;

    // --- IndexReaderBase ----------------------------------------------------

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

    // --- VectorSearchReader -------------------------------------------------

    void
    Search(const DatasetPtr& dataset,
           const VectorSearchParams& params,
           const BitsetView& bitset,
           milvus::OpContext* op_ctx,
           SearchResult& result) const override;

    knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
    Iterators(const DatasetPtr& dataset,
              const knowhere::Json& json,
              const BitsetView& bitset,
              milvus::OpContext* op_ctx) const override;

    bool
    RefineEnabled() const override;

    // --- VectorValueReader --------------------------------------------------

    bool
    HasRawData() const override;

    std::vector<uint8_t>
    GetVector(const DatasetPtr& dataset) const override;

    // See the contract-gap note at the top of this file.
    std::unique_ptr<const knowhere::sparse::SparseRow<SparseValueType>[]>
    GetSparseVector(const DatasetPtr& dataset) const override;

    // --- VectorIndexMetaReader ---------------------------------------------

    MetricType
    Metric() const override;

    IndexType
    KnowhereIndexType() const override;

    int64_t
    Dim() const override;

    knowhere::Json
    PrepareSearchParams(const VectorSearchParams& params) const override;

    // --- VectorNullableReader ----------------------------------------------

    bool
    HasValidData() const override;

    int64_t
    ValidCount() const override;

    bool
    IsRowValid(int64_t logical_offset) const override;

    int64_t
    PhysicalOffset(int64_t logical_offset) const override;

    int64_t
    LogicalOffset(int64_t physical_offset) const override;

    const milvus::OffsetMapping&
    OffsetMapping() const override;

    // --- VectorRefineReader -------------------------------------------------

    knowhere::expected<knowhere::DataSetPtr>
    CalcDistByIDs(const knowhere::DataSetPtr& query_dataset,
                  const BitsetView& bitset,
                  const int64_t* labels,
                  size_t labels_len,
                  bool is_cosine,
                  milvus::OpContext* op_ctx) const override;

    // --- EmbeddingListReader ------------------------------------------------

    std::pair<std::vector<uint8_t>, std::vector<size_t>>
    GetEmbListByIds(const DatasetPtr& dataset,
                    const std::string& metric_type) const override;

 private:
    KnowhereEngine engine_;
    VectorValidData valid_;
    uint32_t search_beamwidth_{8};
};

}  // namespace milvus::index
