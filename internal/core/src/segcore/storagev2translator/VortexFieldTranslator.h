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

#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "common/FieldMeta.h"
#include "common/GroupChunk.h"
#include "common/Types.h"
#include "common/VortexFileHandle.h"
#include "pb/common.pb.h"
#include "segcore/storagev2translator/GroupCTMeta.h"

namespace milvus::segcore::storagev2translator {

/// Per-field Translator for Vortex format with cell-level lazy load.
///
/// Each field in a vortex column group gets its own VortexFieldTranslator
/// and its own CacheSlot.  All VortexFieldTranslators for the same CG
/// share a VortexFileHandle (memfd, mmap, Reader).
///
/// On get_cells(): creates VortexChunk handles + VortexCellGuard.
/// The VortexCellGuard will PUNCH_HOLE the cell's segments when
/// the GroupChunk is destroyed (cache eviction).
class VortexFieldTranslator
    : public milvus::cachinglayer::Translator<milvus::GroupChunk> {
 public:
    /// Per-cell precomputed metadata.
    struct CellMeta {
        int64_t row_start;
        int64_t row_end;
        int64_t compressed_bytes;
        std::vector<int64_t> chunk_indices;  // row group indices
        // Byte ranges for PUNCH_HOLE (populated when segment info available)
        std::vector<std::pair<off_t, size_t>> segment_ranges;
    };

    /// @param cell_segment_ranges  Pre-extracted per-cell (offset, length) pairs
    ///                             for PUNCH_HOLE. Pass empty to disable PUNCH_HOLE.
    VortexFieldTranslator(
        int64_t segment_id,
        int64_t column_group_index,
        FieldId field_id,
        const FieldMeta& field_meta,
        int column_in_batch,
        std::shared_ptr<VortexFileHandle> file_handle,
        milvus::proto::common::LoadPriority load_priority,
        bool eager_load,
        const std::string& warmup_policy,
        std::vector<std::vector<std::pair<off_t, size_t>>>
            cell_segment_ranges = {});

    ~VortexFieldTranslator() override = default;

    size_t
    num_cells() const override;

    milvus::cachinglayer::cid_t
    cell_id_of(milvus::cachinglayer::uid_t uid) const override;

    std::pair<milvus::cachinglayer::ResourceUsage,
              milvus::cachinglayer::ResourceUsage>
    estimated_byte_size_of_cell(
        milvus::cachinglayer::cid_t cid) const override;

    const std::string&
    key() const override;

    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::GroupChunk>>>
    get_cells(milvus::OpContext* ctx,
              const std::vector<milvus::cachinglayer::cid_t>& cids) override;

    milvus::cachinglayer::Meta*
    meta() override {
        return &meta_;
    }

    int64_t
    cells_storage_bytes(
        const std::vector<milvus::cachinglayer::cid_t>& cids) const override;

 private:
    void
    PrecomputeCellMeta(
        std::vector<std::vector<std::pair<off_t, size_t>>>
            cell_segment_ranges);

    int64_t segment_id_;
    int64_t column_group_index_;
    std::string key_;
    FieldId field_id_;
    FieldMeta field_meta_;
    int column_in_batch_;

    std::shared_ptr<VortexFileHandle> file_handle_;

    GroupCTMeta meta_;
    std::vector<CellMeta> cells_;
    // Tracks whether a cell has been loaded before. First get_cells()
    // skips download (data from full download is still in memfd);
    // subsequent calls re-download from S3 (data was PUNCH_HOLEd).
    std::vector<bool> cell_was_loaded_;
};

}  // namespace milvus::segcore::storagev2translator
