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

// READER INTERFACE — knowhere in-memory index families (HNSW, IVF*, FLAT,
// SCANN, sparse WAND/inverted, DataView/interim, ...).
//
// See core_refactor/01-scalar-index.md §11.3. THE QUERY HALF of today's
// `VectorMemIndex<T>` (`index/VectorMemIndex.h:46`) lands here; its build half
// goes to VectorMemBuilder.h, its serialize half to VectorMemArtifact.h and its
// load half to VectorMemLoader.h. That is the whole of the re-homing: four
// call-site groups that today share one class and one 30-method surface now get
// four objects with four lifetimes (§3 principle 1).
//
// WHAT DISAPPEARS RATHER THAN MOVES — every one of these is a `ThrowInfo` shell
// on today's class, i.e. exactly the Liskov violation §2.2 catalogues:
//   `BuildWithRawDataForUT`  (VectorIndex.h:58-64,  Unsupported)
//   `AddWithDataset`         (VectorIndex.h:66-69,  Unsupported on the base class;
//                             the real one is the appender's, see
//                             index/growing/KnowhereGrowingVectorIndex.h)
//   `VectorIterators`        (VectorIndex.h:78-87,  NotImplemented on the base class)
//   `GetEmbListByIds`        (VectorIndex.h:118-123, NotImplemented on the base class)
//   `Serialize`/`Load`x2/`Upload`/`Build`/`BuildWithDataset` — the whole
//                             lifecycle half of `IndexBase` (§4.2's list).
//
// MULTIPLE INHERITANCE, NON-VIRTUALLY: the base class plus N pure-mixin
// interfaces (§4's note). No interface derives from `IndexReaderBase`, so
// `IndexReaderBase` -> reader interface is one sibling cast at pin time, once
// per expression node (§4.3), not per batch.
//
// !! Line references point at the tree before refactor phase 1 (master
// e255009e01); those files are deleted by this change. `git show
// e255009e01:internal/core/src/index/...`

namespace milvus::index {

template <typename T>
class VectorMemReader final : public IndexReaderBase,
                              public VectorSearchReader,
                              public VectorValueReader,
                              public VectorIndexMetaReader,
                              public VectorNullableReader,
                              public VectorRefineReader,
                              public EmbeddingListReader {
 public:
    // Immutable after construction (§5: "every reader is immutable — produced by
    // `Seal()` or `Loader::Open()`, thread-safe, lock-free concurrent reads").
    // Both entrances hand over a fully built engine: `VectorMemLoader::OpenIndex`
    // from persisted bytes, `VectorMemArtifact::OpenReader` from a just-sealed
    // build (§6.2).
    VectorMemReader(KnowhereEngine engine, VectorValidData valid)
        : engine_(std::move(engine)), valid_(std::move(valid)) {
    }

    ~VectorMemReader() override = default;

    // --- storage::LoadedArtifact -------------------------------------------

    // !! THE YARDSTICK OF THIS NUMBER IS UNDEFINED — §12.3. Today most families
    // are charged `SetCellSize({index_load_info_.index_size, 0})` (the index FILE
    // size before compression, `SealedIndexTranslator.cpp:199`) while a couple
    // are charged their measured resident footprint. §12.3 requires the
    // definition to be settled BEFORE the artifact pipeline sinks to L1. This
    // skeleton does not pick one.
    milvus::ResourceUsage
    CellByteSize() const override;

    // --- IndexReaderBase (§4.2) --------------------------------------------

    ReaderCaps
    Caps() const override;

    // Row for an ordinary vector index. FOR AN EMBEDDING-LIST (VECTOR_ARRAY)
    // INDEX THE HONEST ANSWER IS Element — the index numbers embeddings, not
    // rows. Note the mismatch with today's code: element-ness is decided PER
    // QUERY by exec (`search_info.array_offsets_ != nullptr`, which sets
    // `search_result.element_level_`, `query/SearchOnSealed.cpp:94-96`), not
    // self-described by the index, and `SearchInfo::array_offsets_` is one of the
    // fields §12.1(a) proved `index/` never reads. §4.2/§5.8 want the reader to
    // answer it. Recorded, not resolved: see the report.
    Domain
    CoordDomain() const override;

    int64_t
    Count() const override;

    DataType
    ValueType() const override;

    int64_t
    MemoryUsage() const override;

    // --- VectorSearchReader (contracts/VectorReaders.h, §11.3) ---------------

    void
    Search(const DatasetPtr& dataset,
           const VectorSearchParams& params,
           const BitsetView& bitset,
           milvus::OpContext* op_ctx,
           SearchResult& result) const override;

    // §12.1(b): the raw `IteratorPtr` shape is KEPT AS IS, deliberately. See
    // the full ruling in contracts/VectorReaders.h — the pin-lifetime question
    // is made entirely of existing code, is not amplified by this refactor, and
    // paying contract complexity for an unconfirmed defect would be wrong.
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

    std::unique_ptr<const knowhere::sparse::SparseRow<SparseValueType>[]>
    GetSparseVector(const DatasetPtr& dataset) const override;

    // --- VectorIndexMetaReader (family-local, see VectorFamilyReaders.h) -----

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
    // COMPOSED, NOT INHERITED (§3 principle 2, §10 rule 3).
    KnowhereEngine engine_;
    VectorValidData valid_;
};

}  // namespace milvus::index
