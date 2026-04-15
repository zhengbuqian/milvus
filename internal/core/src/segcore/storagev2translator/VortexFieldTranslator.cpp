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

#include "segcore/storagev2translator/VortexFieldTranslator.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cachinglayer/Utils.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/VortexCellGuard.h"
#include "common/VortexChunk.h"
#include "fmt/core.h"
#include "log/Log.h"
#include "segcore/Utils.h"

namespace milvus::segcore::storagev2translator {

VortexFieldTranslator::VortexFieldTranslator(
    int64_t segment_id,
    int64_t column_group_index,
    FieldId field_id,
    const FieldMeta& field_meta,
    int column_in_batch,
    std::shared_ptr<VortexFileHandle> file_handle,
    milvus::proto::common::LoadPriority load_priority,
    bool eager_load,
    const std::string& warmup_policy,
    std::vector<std::vector<std::pair<off_t, size_t>>> cell_segment_ranges)
    : segment_id_(segment_id),
      column_group_index_(column_group_index),
      key_(fmt::format(
          "seg_{}_cg_{}_f_{}", segment_id, column_group_index, field_id.get())),
      field_id_(field_id),
      field_meta_(field_meta),
      column_in_batch_(column_in_batch),
      file_handle_(std::move(file_handle)),
      meta_(/* num_fields */ 1,
            milvus::cachinglayer::StorageType::MEMORY,
            milvus::cachinglayer::CellIdMappingMode::IDENTICAL,
            milvus::segcore::getCellDataType(
                IsVectorDataType(field_meta.get_data_type()),
                /* is_index */ false),
            milvus::segcore::getCacheWarmupPolicy(
                warmup_policy,
                IsVectorDataType(field_meta.get_data_type()),
                /* is_index */ false,
                /* in_load_list */ eager_load),
            /* support_eviction */ true) {
    PrecomputeCellMeta(std::move(cell_segment_ranges));

    LOG_INFO(
        "[StorageV2] VortexFieldTranslator {} created: {} cells, "
        "field_id={}, data_type={}",
        key_,
        cells_.size(),
        field_id_.get(),
        GetDataTypeName(field_meta_.get_data_type()));
}

void
VortexFieldTranslator::PrecomputeCellMeta(
    std::vector<std::vector<std::pair<off_t, size_t>>> cell_segment_ranges) {
    // Use Reader → ChunkReader for row count metadata.
    // ChunkReader's logical chunks may differ from physical chunks,
    // but cell boundaries are determined by cell_segment_ranges
    // (physical FieldChunkOffsets). We use ChunkReader only for row counts.
    auto reader = file_handle_->reader();
    auto chunk_reader_result =
        reader->get_chunk_reader(file_handle_->column_group_index());
    AssertInfo(chunk_reader_result.ok(),
               "VortexFieldTranslator {}: get_chunk_reader failed: {}",
               key_,
               chunk_reader_result.status().ToString());
    auto chunk_reader = std::move(chunk_reader_result).ValueOrDie();

    auto rows_result = chunk_reader->get_chunk_rows();
    AssertInfo(rows_result.ok(),
               "VortexFieldTranslator {}: get_chunk_rows failed",
               key_);
    const auto& row_group_rows = rows_result.ValueOrDie();

    auto sizes_result = chunk_reader->get_chunk_size();
    AssertInfo(sizes_result.ok(),
               "VortexFieldTranslator {}: get_chunk_size failed",
               key_);
    const auto& row_group_sizes = sizes_result.ValueOrDie();

    size_t total_row_groups = row_group_rows.size();
    meta_.total_row_groups_ = total_row_groups;
    size_t num_cells =
        (total_row_groups + kRowGroupsPerCell - 1) / kRowGroupsPerCell;

    meta_.cell_row_group_ranges_.reserve(num_cells);
    meta_.num_rows_until_chunk_.reserve(num_cells + 1);
    meta_.num_rows_until_chunk_.push_back(0);
    meta_.chunk_memory_size_.reserve(num_cells);
    cells_.reserve(num_cells);

    int64_t cumulative_rows = 0;
    for (size_t cid = 0; cid < num_cells; ++cid) {
        size_t rg_start = cid * kRowGroupsPerCell;
        size_t rg_end =
            std::min(rg_start + kRowGroupsPerCell, total_row_groups);
        meta_.cell_row_group_ranges_.push_back({rg_start, rg_end});

        CellMeta cell;
        cell.row_start = cumulative_rows;
        cell.compressed_bytes = 0;

        for (size_t i = rg_start; i < rg_end; ++i) {
            cumulative_rows += static_cast<int64_t>(row_group_rows[i]);
            cell.compressed_bytes += static_cast<int64_t>(row_group_sizes[i]);
        }
        cell.row_end = cumulative_rows;

        meta_.num_rows_until_chunk_.push_back(cumulative_rows);
        meta_.chunk_memory_size_.push_back(cell.compressed_bytes);

        cells_.push_back(std::move(cell));
    }

    // In lazy mode, data is not in memfd (only footer was downloaded),
    // so first get_cells() must also download from S3.
    cell_was_loaded_.resize(cells_.size(), file_handle_->is_lazy());

    // Apply pre-extracted segment ranges for PUNCH_HOLE
    for (size_t cid = 0;
         cid < std::min(cells_.size(), cell_segment_ranges.size());
         ++cid) {
        cells_[cid].segment_ranges = std::move(cell_segment_ranges[cid]);

        // Update compressed_bytes to per-field actual size
        if (!cells_[cid].segment_ranges.empty()) {
            int64_t field_bytes = 0;
            for (const auto& [off, len] : cells_[cid].segment_ranges) {
                field_bytes += static_cast<int64_t>(len);
            }
            cells_[cid].compressed_bytes = field_bytes;
            meta_.chunk_memory_size_[cid] = field_bytes;
        }
    }
}

void
VortexFieldTranslator::CreateFormatReader() {
    if (format_reader_) {
        return;
    }

    auto cg_index = file_handle_->column_group_index();
    auto column_groups = file_handle_->column_groups();
    AssertInfo(column_groups && cg_index >= 0 &&
                   static_cast<size_t>(cg_index) < column_groups->size(),
               "VortexFieldTranslator {}: invalid column_group_index {}",
               key_,
               cg_index);
    auto& cg = column_groups->at(cg_index);
    AssertInfo(!cg->files.empty(),
               "VortexFieldTranslator {}: no files in column group",
               key_);

    // Per-field projection: only read this field's column.
    // Must pass per-field read_schema so VortexFormatReader calls
    // WithOutputSchema to produce flat (non-struct) RecordBatch output.
    std::string field_name = std::to_string(field_id_.get());
    std::vector<std::string> needed_columns = {field_name};

    // Extract per-field schema from the CG's arrow schema (same as
    // ColumnGroupLazyReader does in Reader::take path).
    auto cg_schema = file_handle_->arrow_schema();
    std::shared_ptr<arrow::Schema> field_schema;
    if (cg_schema) {
        auto f = cg_schema->GetFieldByName(field_name);
        if (f) {
            field_schema = arrow::schema({f});
        }
    }

    auto result = milvus_storage::FormatReader::create(
        field_schema,
        cg->format,
        cg->files[0],
        *file_handle_->properties(),
        needed_columns,
        nullptr);
    AssertInfo(result.ok(),
               "VortexFieldTranslator {}: FormatReader::create failed: {}",
               key_,
               result.status().ToString());
    format_reader_ = std::move(result).ValueOrDie();

    auto open_status = format_reader_->open();
    AssertInfo(open_status.ok(),
               "VortexFieldTranslator {}: FormatReader::open failed: {}",
               key_,
               open_status.ToString());

    auto file_schema = format_reader_->get_schema();
    LOG_INFO(
        "[StorageV2] VortexFieldTranslator {} FormatReader: "
        "field={} file_schema={}",
        key_,
        field_name,
        file_schema ? file_schema->ToString() : "null");
}

size_t
VortexFieldTranslator::num_cells() const {
    return cells_.size();
}

milvus::cachinglayer::cid_t
VortexFieldTranslator::cell_id_of(milvus::cachinglayer::uid_t uid) const {
    return uid;
}

std::pair<milvus::cachinglayer::ResourceUsage,
          milvus::cachinglayer::ResourceUsage>
VortexFieldTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    AssertInfo(cid < cells_.size(),
               "VortexFieldTranslator {}: cid {} out of range ({})",
               key_,
               cid,
               cells_.size());
    auto cell_sz = cells_[cid].compressed_bytes;
    return {{cell_sz, 0}, {2 * cell_sz, 0}};
}

const std::string&
VortexFieldTranslator::key() const {
    return key_;
}

int64_t
VortexFieldTranslator::cells_storage_bytes(
    const std::vector<milvus::cachinglayer::cid_t>& cids) const {
    constexpr int64_t MIN_STORAGE_BYTES = 1 * 1024 * 1024;
    int64_t total = 0;
    for (auto cid : cids) {
        AssertInfo(cid < cells_.size(),
                   "VortexFieldTranslator {}: cid {} out of range ({})",
                   key_,
                   cid,
                   cells_.size());
        total += std::max(cells_[cid].compressed_bytes, MIN_STORAGE_BYTES);
    }
    return total;
}

std::vector<
    std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::GroupChunk>>>
VortexFieldTranslator::get_cells(
    milvus::OpContext* ctx,
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    CheckCancellation(ctx, segment_id_, "VortexFieldTranslator::get_cells()");

    // Lazily create per-field FormatReader on first cell access.
    CreateFormatReader();

    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::GroupChunk>>>
        result;
    result.reserve(cids.size());

    for (const auto cid : cids) {
        AssertInfo(cid < cells_.size(),
                   "[StorageV2] VortexFieldTranslator {} cid {} out of range "
                   "(total: {})",
                   key_,
                   cid,
                   cells_.size());

        const auto& cell = cells_[cid];

        // Re-download from S3 if cell was previously evicted (PUNCH_HOLEd).
        // First load skips this — data is already in memfd from full download.
        if (cell_was_loaded_[cid] && !cell.segment_ranges.empty()) {
            auto s3_fs = file_handle_->s3_fs();
            auto input_result = s3_fs->OpenInputFile(file_handle_->s3_path());
            AssertInfo(input_result.ok(),
                       "VortexFieldTranslator {}: OpenInputFile failed for "
                       "S3 re-read: {}",
                       key_,
                       input_result.status().ToString());
            auto input = input_result.ValueOrDie();

            for (const auto& [offset, length] : cell.segment_ranges) {
                auto buf_result = input->ReadAt(offset, length);
                AssertInfo(buf_result.ok(),
                           "VortexFieldTranslator {}: S3 ReadAt failed at "
                           "offset={} len={}: {}",
                           key_,
                           offset,
                           length,
                           buf_result.status().ToString());
                auto buf = buf_result.ValueOrDie();

                size_t written = 0;
                while (written < length) {
                    ssize_t n = pwrite(file_handle_->memfd(),
                                       buf->data() + written,
                                       length - written,
                                       offset + written);
                    AssertInfo(n > 0,
                               "VortexFieldTranslator {}: pwrite failed at "
                               "offset {}: {}",
                               key_,
                               offset + written,
                               strerror(errno));
                    written += n;
                }
            }

            LOG_INFO(
                "[StorageV2] VortexFieldTranslator {}: re-loaded cell "
                "{} from S3 ({} ranges)",
                key_,
                cid,
                cell.segment_ranges.size());
        }
        cell_was_loaded_[cid] = true;

        int64_t row_count = cell.row_end - cell.row_start;

        // Create VortexChunk handle with per-field FormatReader
        auto vortex_chunk =
            std::make_shared<VortexChunk>(field_id_,
                                          field_meta_,
                                          row_count,
                                          cell.compressed_bytes,
                                          format_reader_,
                                          column_in_batch_,
                                          cell.row_start);

        std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
        chunks[field_id_] = std::move(vortex_chunk);

        auto group_chunk = std::make_unique<GroupChunk>(chunks);

        // Attach VortexCellGuard for PUNCH_HOLE on eviction
        if (!cell.segment_ranges.empty()) {
            group_chunk->SetVortexGuard(std::make_unique<VortexCellGuard>(
                file_handle_->memfd(), cell.segment_ranges));
        }

        result.emplace_back(cid, std::move(group_chunk));
    }

    return result;
}

}  // namespace milvus::segcore::storagev2translator
