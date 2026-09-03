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
#include <string_view>

#include "common/Types.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/SpatialReader.h"
#include "index/growing/GrowingAppenderBase.h"
#include "index/growing/GrowingSpatialIndexInterface.h"
#include "index/scalar/spatial/RTreeEngine.h"

// The growing GEOMETRY appender — TODAY'S ONLY WORKING GROWING SCALAR INDEX.
//
// See core_refactor/01-scalar-index.md §7, §7.1, §5.6, and the contract-gap
// note in GrowingSpatialIndexInterface.h (this family fits none of the three
// appender classes the contract declares).
//
// WHAT IT REPLACES: the geometry branch threaded through
// `ScalarFieldIndexing<std::string>` — `recreate_index` building an
// `index::RTreeIndex<std::string>` behind a hand-assembled `FileManagerContext`
// (`segcore/FieldIndexing.cpp:614-660`), both `AppendSegmentIndex` overloads
// dispatching on `get_data_type() == DataType::GEOMETRY` and throwing
// `Unsupported` otherwise (`:674-751`), and `process_geometry_data`
// (`:753-818`).
//
// !! THIS FAMILY HAS NO SNAPSHOT TODAY, AND THAT IS THE REAL WORK.
// `process_geometry_data` calls `rtree_index->AddGeometry(wkb, global_offset)`
// row by row INTO THE LIVE INDEX and then sets `sync_with_index_ = true`
// (`FieldIndexing.cpp:790-811`); readers get that same mutable object back from
// `get_segment_indexing()` (`FieldIndexing.h:227-237`). There is no generation,
// no commit, no immutable object — so queries read an index that is being
// mutated underneath them, and the "watermark" is an `index_cur_` counter nobody
// reads. §7's model (immutable snapshot + published watermark) is therefore a
// REAL BEHAVIOURAL REQUIREMENT here, not a re-description of existing code.
//
// AND THE ENGINE SPLIT MAKES THE COST EXPLICIT. The spatial family's engine is
// now two classes (`index/scalar/spatial/RTreeEngine.h`): `RTreeBuildEngine`
// accumulates and `Finish()`es BY WRITING `<index_path>.bgi` +
// `<index_path>.meta.json`, and `RTreeQueryEngine` becomes readable only after
// `Load()` FROM THAT PATH. So "publish a snapshot" via the engines as they stand
// is a DISK ROUND TRIP PER COMMIT. Three options, none free:
//   (a) copy the in-memory `bgi::rtree` at commit into a query engine — needs a
//       memory hand-off the engine pair does not currently expose (both classes
//       hold their own `rtree_`/`values_`, joined only by a path);
//   (b) a two-generation scheme: the snapshot holds a frozen tree, the appender
//       fills the next one, and the trees are merged at commit;
//   (c) keep the live tree and take a read lock for the duration of a query,
//       giving up §5's lock-free-concurrent-read promise for this family.
// Not decided here. See the report.
//
// Two further construction facts that must not be re-homed as they are:
//   - The index is built with `InitForBuildIndex(true)` ON FIRST ROW, with no
//     threshold (`FieldIndexing.cpp:772-788`), i.e. a growing threshold of 0 —
//     consistent with §7's "an empty snapshot means none yet".
//   - The `FileManagerContext` assembled at `FieldIndexing.cpp:614-660` (chunk
//     manager, arrow filesystem, a fake `IndexMeta` with `build_id = 0`,
//     `segment_id = 0`, `key = "rtree_index"`) exists ONLY because
//     `RTreeIndex`'s constructor demands one. A growing appender writes nothing
//     to storage — §10 rule 2 says that context may not appear on an appender at
//     all, and the fake metadata is evidence of the same thing.

namespace milvus::index {

class RTreeGrowingSnapshot final : public IndexReaderBase,
                                   public SpatialReader {
 public:
    RTreeGrowingSnapshot(int64_t covered_rows);

    ~RTreeGrowingSnapshot() override = default;

    milvus::ResourceUsage
    CellByteSize() const override;

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

    // --- SpatialReader (§5.6) -----------------------------------------------
    //
    // Candidate family: the result is a SUPERSET (MBR coarse filter,
    // `caps.exact = false`, `caps.spatial = true`) and exec refines it against
    // the raw values — the pattern §5.4 tells ngram to copy.
    //
    // Two contract-level changes relative to today's
    // `RTreeIndex::QueryCandidates(GISOp, Geometry, std::vector<int64_t>&)`:
    // the operator is the contract's NATIVE `SpatialOp` rather than
    // `proto::plan::GISFunctionFilterExpr_GISOp` (README §5 rule 2 — pb never on
    // a contract signature), and the output is a `TargetBitmap` rather than a
    // sparse vector (§5's single output shape).
    TargetBitmap
    Candidates(SpatialOp op, const Geometry& query_geom) const override;

 private:
    int64_t covered_rows_{0};
    // The frozen generation this snapshot reads. `RTreeQueryEngine` is exactly
    // the right shape — "immutable after `Load`, concurrently readable" — the
    // open question is how it gets its data without a disk round trip per
    // commit (see the header).
    std::unique_ptr<RTreeQueryEngine> engine_;
};

class RTreeGrowingSpatialIndex final : public GrowingSpatialIndex,
                                       public GrowingAppenderBase {
 public:
    RTreeGrowingSpatialIndex();

    ~RTreeGrowingSpatialIndex() override = default;

    // --- GrowingSpatialIndex ------------------------------------------------

    void
    Append(int64_t reserved_offset,
           size_t n,
           const std::string_view* wkb_values,
           const bool* valid) override;

    std::shared_ptr<const SpatialReader>
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

 private:
    mutable std::mutex mtx_;
    std::shared_ptr<const RTreeGrowingSnapshot> snapshot_;
    int64_t appended_rows_{0};

    // COMPOSED, not inherited (§3 principle 2), and it is the SPATIAL FAMILY'S
    // OWN ENGINE (`index/scalar/spatial/RTreeEngine.h`), not a second copy: the
    // growing appender and the sealed builder feed the same boost::geometry
    // structure. What it is NOT is an `index::RTreeIndex` — that family class
    // carries a file manager, a build path and an upload path an appender must
    // never have (§3 principle 6, §10 rule 2).
    std::unique_ptr<RTreeBuildEngine> engine_;
};

}  // namespace milvus::index
