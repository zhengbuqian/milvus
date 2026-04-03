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

#include <folly/io/IOBuf.h>
#include <sys/mman.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>
#include <cmath>

#include <arrow/array.h>
#include <arrow/type_traits.h>
#include "common/VortexChunk.h"

#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Manager.h"
#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"

#include "common/Chunk.h"
#include "common/ChunkDataView.h"
#include "common/GroupChunk.h"
#include "common/EasyAssert.h"
#include "common/OpContext.h"
#include "mmap/ChunkedColumnInterface.h"
#include "segcore/storagev2translator/GroupCTMeta.h"

namespace milvus {

using GroupChunkVector = std::vector<std::shared_ptr<GroupChunk>>;

using namespace milvus::cachinglayer;

// ChunkedColumnGroup represents a collection of group chunks
class ChunkedColumnGroup {
 public:
    explicit ChunkedColumnGroup(
        std::unique_ptr<Translator<GroupChunk>> translator)
        : slot_(Manager::GetInstance().CreateCacheSlot(std::move(translator))) {
        num_chunks_ = slot_->num_cells();
        num_rows_ = GetNumRowsUntilChunk().back();
    }

    virtual ~ChunkedColumnGroup() {
        slot_->CancelWarmup();
    }

    void
    ManualEvictCache() const {
        slot_->ManualEvictAll();
    }

    void
    CancelWarmup() {
        slot_->CancelWarmup();
    }

    // Get the number of group chunks
    size_t
    num_chunks() const {
        return num_chunks_;
    }

    PinWrapper<GroupChunk*>
    GetGroupChunk(milvus::OpContext* op_ctx, int64_t chunk_id) const {
        auto ca = SemiInlineGet(slot_->PinCells(op_ctx, {chunk_id}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<GroupChunk*>(ca, chunk);
    }

    std::shared_ptr<CellAccessor<GroupChunk>>
    GetGroupChunks(milvus::OpContext* op_ctx,
                   const std::vector<int64_t>& chunk_ids) {
        return SemiInlineGet(slot_->PinCells(op_ctx, chunk_ids));
    }

    std::vector<PinWrapper<GroupChunk*>>
    GetAllGroupChunks(milvus::OpContext* op_ctx) {
        auto ca = SemiInlineGet(slot_->PinAllCells(op_ctx));
        std::vector<PinWrapper<GroupChunk*>> ret;
        ret.reserve(num_chunks_);
        for (size_t i = 0; i < num_chunks_; i++) {
            auto chunk = ca->get_cell_of(i);
            ret.emplace_back(ca, chunk);
        }
        return ret;
    }

    int64_t
    NumRows() const {
        return num_rows_;
    }

    int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const {
        AssertInfo(
            chunk_id >= 0 && chunk_id <= num_chunks_,
            "[StorageV2] chunk_id out of range: " + std::to_string(chunk_id));
        return GetNumRowsUntilChunk()[chunk_id];
    }

    const std::vector<int64_t>&
    GetNumRowsUntilChunk() const {
        auto meta =
            static_cast<milvus::segcore::storagev2translator::GroupCTMeta*>(
                slot_->meta());
        return meta->num_rows_until_chunk_;
    }

    std::pair<size_t, size_t>
    GetChunkIDByOffset(int64_t offset) const {
        const auto& num_rows_until_chunk = GetNumRowsUntilChunk();
        auto iter = std::lower_bound(num_rows_until_chunk.begin(),
                                     num_rows_until_chunk.end(),
                                     offset + 1);
        size_t chunk_idx =
            std::distance(num_rows_until_chunk.begin(), iter) - 1;
        size_t offset_in_chunk = offset - num_rows_until_chunk[chunk_idx];
        return {chunk_idx, offset_in_chunk};
    }

    std::pair<std::vector<milvus::cachinglayer::cid_t>, std::vector<int64_t>>
    GetChunkIDsByOffsets(const int64_t* offsets, int64_t count) {
        const auto& num_rows_until_chunk = GetNumRowsUntilChunk();
        std::vector<milvus::cachinglayer::cid_t> cids(count, 1);
        std::vector<int64_t> offsets_in_chunk(count);
        int64_t len = num_rows_until_chunk.size() - 1;
        while (len > 1) {
            const int64_t half = len / 2;
            len -= half;
            for (size_t i = 0; i < count; ++i) {
                const bool cmp =
                    num_rows_until_chunk[cids[i] + half - 1] < offsets[i] + 1;
                cids[i] += static_cast<int64_t>(cmp) * half;
            }
        }

        for (size_t i = 0; i < count; ++i) {
            offsets_in_chunk[i] = offsets[i] - num_rows_until_chunk[--cids[i]];
        }

        return std::make_pair(std::move(cids), std::move(offsets_in_chunk));
    }

    size_t
    NumFieldsInGroup() const {
        auto meta =
            static_cast<milvus::segcore::storagev2translator::GroupCTMeta*>(
                slot_->meta());
        return meta->num_fields_;
    }

    size_t
    memory_size() const {
        auto meta =
            static_cast<milvus::segcore::storagev2translator::GroupCTMeta*>(
                slot_->meta());
        size_t memory_size = 0;
        for (auto& size : meta->chunk_memory_size_) {
            memory_size += size;
        }
        return memory_size;
    }

 protected:
    mutable std::shared_ptr<CacheSlot<GroupChunk>> slot_;
    size_t num_chunks_{0};
    size_t num_rows_{0};
};

class ProxyChunkColumn : public ChunkedColumnInterface {
 public:
    explicit ProxyChunkColumn(std::shared_ptr<ChunkedColumnGroup> group,
                              FieldId field_id,
                              const FieldMeta& field_meta)
        : group_(group),
          field_id_(field_id),
          field_meta_(field_meta),
          data_type_(field_meta.get_data_type()) {
    }

    ~ProxyChunkColumn() override {
        CancelWarmup();
    }

    bool
    IsInMultiFieldColumnGroup() const override {
        return group_->NumFieldsInGroup() > 1;
    }

    void
    ManualEvictCache() const override {
        if (group_->NumFieldsInGroup() == 1) {
            group_->ManualEvictCache();
        }
    }

    void
    CancelWarmup() override {
        if (group_->NumFieldsInGroup() == 1) {
            group_->CancelWarmup();
        }
    }

    PinWrapper<const char*>
    DataOfChunk(milvus::OpContext* op_ctx, int chunk_id) const override {
        auto group_chunk = group_->GetGroupChunk(op_ctx, chunk_id);
        auto chunk = group_chunk.get()->GetChunk(field_id_);
        // Data() returns raw contiguous pointer; VortexChunk does not
        // support this — callers should use ChunkDataView instead.
        return PinWrapper<const char*>(group_chunk, chunk->Data());
    }

    bool
    IsValid(milvus::OpContext* op_ctx, size_t offset) const override {
        auto [chunk_id, offset_in_chunk] = group_->GetChunkIDByOffset(offset);
        auto group_chunk = group_->GetGroupChunk(op_ctx, chunk_id);
        auto chunk = group_chunk.get()->GetChunk(field_id_);
        return chunk->IsValid(offset_in_chunk);
    }

    void
    BulkIsValid(milvus::OpContext* op_ctx,
                std::function<void(bool, size_t)> fn,
                const int64_t* offsets = nullptr,
                int64_t count = 0) const override {
        if (!field_meta_.is_nullable()) {
            if (offsets == nullptr) {
                for (int64_t i = 0; i < group_->NumRows(); i++) {
                    fn(true, i);
                }
            } else {
                for (int64_t i = 0; i < count; i++) {
                    fn(true, i);
                }
            }
        }
        // nullable:
        if (count == 0) {
            return;
        }
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
        auto ca = group_->GetGroupChunks(op_ctx, cids);
        for (int64_t i = 0; i < count; i++) {
            auto* group_chunk = ca->get_cell_of(cids[i]);
            auto chunk = group_chunk->GetChunk(field_id_);
            auto valid = chunk->IsValid(offsets_in_chunk[i]);
            fn(valid, i);
        }
    }

    bool
    IsNullable() const override {
        return field_meta_.is_nullable();
    }

    size_t
    NumRows() const override {
        return group_->NumRows();
    }

    int64_t
    num_chunks() const override {
        return group_->num_chunks();
    }

    size_t
    DataByteSize() const override {
        return group_->memory_size();
    }

    int64_t
    chunk_row_nums(int64_t chunk_id) const override {
        return group_->GetNumRowsUntilChunk(chunk_id + 1) -
               group_->GetNumRowsUntilChunk(chunk_id);
    }

    // TODO(tiered storage): make it async
    void
    PrefetchChunks(milvus::OpContext* op_ctx,
                   const std::vector<int64_t>& chunk_ids) const override {
        group_->GetGroupChunks(op_ctx, chunk_ids);
    }

    PinWrapper<AnyDataView>
    ChunkDataView(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        auto chunk_wrapper = group_->GetGroupChunk(op_ctx, chunk_id);
        auto chunk = chunk_wrapper.get()->GetChunk(field_id_);
        return PinWrapper<AnyDataView>(chunk_wrapper, chunk->GetAnyDataView());
    }

    PinWrapper<AnyDataView>
    ChunkDataViewByRange(milvus::OpContext* op_ctx,
                         int64_t chunk_id,
                         int64_t start_offset,
                         int64_t length) const override {
        auto chunk_wrapper = group_->GetGroupChunk(op_ctx, chunk_id);
        auto chunk = chunk_wrapper.get()->GetChunk(field_id_);
        return PinWrapper<AnyDataView>(
            chunk_wrapper, chunk->GetAnyDataView(start_offset, length));
    }

    PinWrapper<AnyDataView>
    ChunkDataViewByOffsets(milvus::OpContext* op_ctx,
                           int64_t chunk_id,
                           const FixedVector<int32_t>& offsets) const override {
        auto chunk_wrapper = group_->GetGroupChunk(op_ctx, chunk_id);
        auto chunk = chunk_wrapper.get()->GetChunk(field_id_);
        return PinWrapper<AnyDataView>(chunk_wrapper,
                                       chunk->GetAnyDataView(offsets));
    }

    std::pair<size_t, size_t>
    GetChunkIDByOffset(int64_t offset) const override {
        return group_->GetChunkIDByOffset(offset);
    }

    std::pair<std::vector<milvus::cachinglayer::cid_t>, std::vector<int64_t>>
    GetChunkIDsByOffsets(const int64_t* offsets, int64_t count) const override {
        return group_->GetChunkIDsByOffsets(offsets, count);
    }

    PinWrapper<Chunk*>
    GetChunk(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        auto group_chunk = group_->GetGroupChunk(op_ctx, chunk_id);
        auto chunk = group_chunk.get()->GetChunk(field_id_);
        return PinWrapper<Chunk*>(group_chunk, chunk.get());
    }

    std::vector<PinWrapper<Chunk*>>
    GetAllChunks(milvus::OpContext* op_ctx) const override {
        std::vector<PinWrapper<Chunk*>> ret;
        auto group_chunks = group_->GetAllGroupChunks(op_ctx);
        ret.reserve(group_chunks.size());

        for (auto& group_chunk : group_chunks) {
            auto chunk = group_chunk.get()->GetChunk(field_id_);
            ret.emplace_back(group_chunk, chunk.get());
        }
        return ret;
    }

    int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const override {
        return group_->GetNumRowsUntilChunk(chunk_id);
    }

    const std::vector<int64_t>&
    GetNumRowsUntilChunk() const override {
        return group_->GetNumRowsUntilChunk();
    }

    const std::vector<int64_t>&
    GetNumValidRowsUntilChunk() const override {
        // For nullable columns, return the cumulative valid row counts
        // For non-nullable columns, this equals num_rows_until_chunk_
        if (!num_valid_rows_until_chunk_.empty()) {
            return num_valid_rows_until_chunk_;
        }
        return GetNumRowsUntilChunk();
    }

    void
    BulkValueAt(milvus::OpContext* op_ctx,
                std::function<void(const char*, size_t)> fn,
                const int64_t* offsets,
                int64_t count) override {
        auto [cids, offsets_in_chunk] =
            field_meta_.is_nullable()
                ? ToChunkIdAndOffsetByPhysical(offsets, count)
                : ToChunkIdAndOffset(offsets, count);
        auto ca = group_->GetGroupChunks(op_ctx, cids);
        for (int64_t i = 0; i < count; i++) {
            auto* group_chunk = ca->get_cell_of(cids[i]);
            auto chunk = group_chunk->GetChunk(field_id_);
            fn(chunk->ValueAt(offsets_in_chunk[i]), i);
        }
    }

    template <typename S, typename T>
    void
    BulkPrimitiveValueAtImpl(milvus::OpContext* op_ctx,
                             void* dst,
                             const int64_t* offsets,
                             int64_t count) {
        static_assert(std::is_fundamental_v<S> && std::is_fundamental_v<T>);
        if (count == 0) {
            return;
        }
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
        auto ca = group_->GetGroupChunks(op_ctx, cids);
        auto typed_dst = static_cast<T*>(dst);

        // Vortex fast path: all referenced cells are pinned above.
        // All cells in a CG share one Reader, so pick any cell's Reader
        // and issue a single take() at segment level for all offsets.
        if (auto* vchunk = dynamic_cast<VortexChunk*>(
                ca->get_cell_of(cids[0])->GetChunk(field_id_).get())) {
            std::vector<int64_t> sorted(offsets, offsets + count);
            std::sort(sorted.begin(), sorted.end());
            sorted.erase(
                std::unique(sorted.begin(), sorted.end()), sorted.end());

            auto result = vchunk->GetReader()->take(sorted);
            AssertInfo(result.ok(),
                       "Vortex batched take failed: {}",
                       result.status().ToString());
            auto table = *result;

            int col_idx =
                table->schema()->GetFieldIndex(vchunk->GetFieldName());
            AssertInfo(col_idx >= 0,
                       "BulkVortex: field '{}' not found in take result",
                       vchunk->GetFieldName());
            auto col = table->column(col_idx);

            std::vector<S> flat_vals;
            flat_vals.reserve(col->length());
            for (int i = 0; i < col->num_chunks(); ++i) {
                using ArrayType = typename arrow::CTypeTraits<S>::ArrayType;
                auto typed_arr =
                    std::static_pointer_cast<ArrayType>(col->chunk(i));
                for (int64_t j = 0; j < typed_arr->length(); ++j) {
                    flat_vals.push_back(typed_arr->Value(j));
                }
            }

            for (int64_t i = 0; i < count; ++i) {
                auto it = std::lower_bound(
                    sorted.begin(), sorted.end(), offsets[i]);
                typed_dst[i] =
                    static_cast<T>(flat_vals[it - sorted.begin()]);
            }
            return;
        }

        // Process per-chunk segments using batch DataByOffsets.
        int64_t seg_start = 0;
        while (seg_start < count) {
            auto cur_cid = cids[seg_start];
            int64_t seg_end = seg_start + 1;
            while (seg_end < count && cids[seg_end] == cur_cid) {
                ++seg_end;
            }
            int64_t seg_count = seg_end - seg_start;

            auto* group_chunk = ca->get_cell_of(cur_cid);
            auto chunk = group_chunk->GetChunk(field_id_);
            auto data_view =
                chunk->GetAnyDataView().template as<S>();

            std::vector<int64_t> local_offsets(seg_count);
            for (int64_t j = 0; j < seg_count; ++j) {
                local_offsets[j] =
                    static_cast<int64_t>(offsets_in_chunk[seg_start + j]);
            }

            const S* batch_data =
                data_view->DataByOffsets(local_offsets.data(), seg_count);
            for (int64_t j = 0; j < seg_count; ++j) {
                typed_dst[seg_start + j] = static_cast<T>(batch_data[j]);
            }

            seg_start = seg_end;
        }
    }

    void
    BulkPrimitiveValueAt(milvus::OpContext* op_ctx,
                         void* dst,
                         const int64_t* offsets,
                         int64_t count,
                         bool small_int_raw_type) override {
        switch (data_type_) {
            case DataType::INT8: {
                if (small_int_raw_type) {
                    BulkPrimitiveValueAtImpl<int8_t, int8_t>(
                        op_ctx, dst, offsets, count);
                } else {
                    BulkPrimitiveValueAtImpl<int8_t, int32_t>(
                        op_ctx, dst, offsets, count);
                }
                break;
            }
            case DataType::INT16: {
                if (small_int_raw_type) {
                    BulkPrimitiveValueAtImpl<int16_t, int16_t>(
                        op_ctx, dst, offsets, count);
                } else {
                    BulkPrimitiveValueAtImpl<int16_t, int32_t>(
                        op_ctx, dst, offsets, count);
                }
                break;
            }
            case DataType::INT32: {
                BulkPrimitiveValueAtImpl<int32_t, int32_t>(
                    op_ctx, dst, offsets, count);
                break;
            }
            case DataType::INT64: {
                BulkPrimitiveValueAtImpl<int64_t, int64_t>(
                    op_ctx, dst, offsets, count);
                break;
            }
            case DataType::TIMESTAMPTZ: {
                BulkPrimitiveValueAtImpl<int64_t, int64_t>(
                    op_ctx, dst, offsets, count);
                break;
            }
            case DataType::FLOAT: {
                BulkPrimitiveValueAtImpl<float, float>(
                    op_ctx, dst, offsets, count);
                break;
            }
            case DataType::DOUBLE: {
                BulkPrimitiveValueAtImpl<double, double>(
                    op_ctx, dst, offsets, count);
                break;
            }
            case DataType::BOOL: {
                BulkPrimitiveValueAtImpl<bool, bool>(
                    op_ctx, dst, offsets, count);
                break;
            }
            default: {
                ThrowInfo(ErrorCode::Unsupported,
                          "[StorageV2] BulkScalarValueAt is not supported for "
                          "unknown scalar "
                          "data type: {}",
                          data_type_);
            }
        }
    }

    void
    BulkVectorValueAt(milvus::OpContext* op_ctx,
                      void* dst,
                      const int64_t* offsets,
                      int64_t element_sizeof,
                      int64_t count) override {
        auto [cids, offsets_in_chunk] =
            field_meta_.is_nullable()
                ? ToChunkIdAndOffsetByPhysical(offsets, count)
                : ToChunkIdAndOffset(offsets, count);
        auto ca = group_->GetGroupChunks(op_ctx, cids);
        auto dst_vec = reinterpret_cast<char*>(dst);
        for (int64_t i = 0; i < count; i++) {
            auto* group_chunk = ca->get_cell_of(cids[i]);
            auto chunk = group_chunk->GetChunk(field_id_);
            auto value = chunk->ValueAt(offsets_in_chunk[i]);
            memcpy(dst_vec + i * element_sizeof, value, element_sizeof);
        }
    }

    void
    BulkRawStringAt(milvus::OpContext* op_ctx,
                    std::function<void(std::string&&, size_t, bool)> fn,
                    const int64_t* offsets = nullptr,
                    int64_t count = 0) const override {
        if (!IsChunkedVariableColumnDataType(data_type_) ||
            data_type_ == DataType::JSON) {
            ThrowInfo(ErrorCode::Unsupported,
                      "[StorageV2] BulkRawStringAt only supported for "
                      "ProxyChunkColumn of "
                      "variable length type(except Json)");
        }
        if (offsets == nullptr) {
            int64_t current_offset = 0;
            for (cid_t cid = 0; cid < num_chunks(); ++cid) {
                auto group_chunk = group_->GetGroupChunk(op_ctx, cid);
                auto chunk = group_chunk.get()->GetChunk(field_id_);
                auto dv = chunk->GetAnyDataView()
                              .template as<std::string_view>();
                auto chunk_rows = chunk->RowNums();
                const auto* data = dv->Data();
                for (int64_t i = 0; i < chunk_rows; ++i) {
                    auto valid = chunk->IsValid(i);
                    fn(std::string(data[i]), current_offset + i, valid);
                }
                current_offset += chunk_rows;
            }
        } else if (count > 0) {
            auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
            auto ca = group_->GetGroupChunks(op_ctx, cids);

            // Vortex fast path: single batched take() across all cells.
            std::vector<int64_t> sorted;
            if (auto vortex_result =
                    TryVortexStringTake(ca, cids, offsets, count, sorted)) {
                auto& [table, flat_views] = *vortex_result;
                for (int64_t i = 0; i < count; ++i) {
                    auto it = std::lower_bound(
                        sorted.begin(), sorted.end(), offsets[i]);
                    fn(std::string(flat_views[it - sorted.begin()]), i, true);
                }
                return;
            }

            int64_t seg_start = 0;
            while (seg_start < count) {
                auto cur_cid = cids[seg_start];
                int64_t seg_end = seg_start + 1;
                while (seg_end < count && cids[seg_end] == cur_cid) {
                    ++seg_end;
                }
                int64_t seg_count = seg_end - seg_start;
                auto* group_chunk = ca->get_cell_of(cur_cid);
                auto chunk = group_chunk->GetChunk(field_id_);

                std::vector<int64_t> local_offsets(seg_count);
                for (int64_t j = 0; j < seg_count; ++j) {
                    local_offsets[j] = static_cast<int64_t>(
                        offsets_in_chunk[seg_start + j]);
                }
                auto owned = chunk->BulkOwnData(
                    local_offsets.data(), seg_count);
                for (int64_t j = 0; j < seg_count; ++j) {
                    auto valid =
                        chunk->IsValid(offsets_in_chunk[seg_start + j]);
                    fn(std::move(owned[j]), seg_start + j, valid);
                }
                seg_start = seg_end;
            }
        }
    }

    void
    BulkRawJsonAt(milvus::OpContext* op_ctx,
                  std::function<void(Json&&, size_t, bool)> fn,
                  const int64_t* offsets,
                  int64_t count) const override {
        if (data_type_ != DataType::JSON) {
            ThrowInfo(ErrorCode::Unsupported,
                      "[StorageV2] RawJsonAt only supported for "
                      "ProxyChunkColumn of Json type");
        }
        if (count == 0) {
            return;
        }
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
        auto ca = group_->GetGroupChunks(op_ctx, cids);

        // Vortex fast path: single batched take() across all cells.
        // Constructs owning Json (simdjson::padded_string) so callers can move.
        std::vector<int64_t> sorted;
        if (auto vortex_result =
                TryVortexStringTake(ca, cids, offsets, count, sorted)) {
            auto& [table, flat_views] = *vortex_result;
            for (int64_t i = 0; i < count; ++i) {
                auto it = std::lower_bound(
                    sorted.begin(), sorted.end(), offsets[i]);
                auto sv = flat_views[it - sorted.begin()];
                fn(Json(simdjson::padded_string(sv.data(), sv.size())),
                   i,
                   true);
            }
            return;
        }

        auto make_dv = [&](cachinglayer::cid_t cid) {
            auto* group_chunk = ca->get_cell_of(cid);
            auto chunk = group_chunk->GetChunk(field_id_);
            return std::make_pair(
                chunk->GetAnyDataView()
                    .template as<std::string_view>(),
                chunk);
        };

        int64_t seg_start = 0;
        while (seg_start < count) {
            auto cur_cid = cids[seg_start];
            int64_t seg_end = seg_start + 1;
            while (seg_end < count && cids[seg_end] == cur_cid) {
                ++seg_end;
            }
            int64_t seg_count = seg_end - seg_start;
            auto cached = make_dv(cur_cid);

            std::vector<int64_t> local_offsets(seg_count);
            for (int64_t j = 0; j < seg_count; ++j) {
                local_offsets[j] = static_cast<int64_t>(
                    offsets_in_chunk[seg_start + j]);
            }
            const auto* batch_data =
                cached.first->DataByOffsets(
                    local_offsets.data(), seg_count);
            for (int64_t j = 0; j < seg_count; ++j) {
                auto valid = cached.second->IsValid(
                    offsets_in_chunk[seg_start + j]);
                auto sv = batch_data[j];
                // Non-owning Json: points into DataView buffer (valid during call).
                fn(Json(sv.data(), sv.size()), seg_start + j, valid);
            }
            seg_start = seg_end;
        }
    }

    void
    BulkRawBsonAt(milvus::OpContext* op_ctx,
                  std::function<void(BsonView, uint32_t, uint32_t)> fn,
                  const uint32_t* row_offsets,
                  const uint32_t* value_offsets,
                  int64_t count) const override {
        if (data_type_ != DataType::STRING) {
            ThrowInfo(ErrorCode::Unsupported,
                      "BulkRawBsonAt only supported for ProxyChunkColumn of "
                      "Bson type");
        }
        if (count == 0) {
            return;
        }

        AssertInfo(row_offsets != nullptr, "row_offsets is nullptr");
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(row_offsets, count);
        auto ca = group_->GetGroupChunks(op_ctx, cids);

        auto make_dv = [&](cachinglayer::cid_t cid) {
            auto* group_chunk = ca->get_cell_of(cid);
            auto chunk = group_chunk->GetChunk(field_id_);
            return chunk->GetAnyDataView()
                .template as<std::string_view>();
        };

        int64_t seg_start = 0;
        while (seg_start < count) {
            auto cur_cid = cids[seg_start];
            int64_t seg_end = seg_start + 1;
            while (seg_end < count && cids[seg_end] == cur_cid) {
                ++seg_end;
            }
            int64_t seg_count = seg_end - seg_start;
            auto dv = make_dv(cur_cid);

            std::vector<int64_t> local_offsets(seg_count);
            for (int64_t j = 0; j < seg_count; ++j) {
                local_offsets[j] = static_cast<int64_t>(
                    offsets_in_chunk[seg_start + j]);
            }
            const auto* batch_data =
                dv->DataByOffsets(local_offsets.data(), seg_count);
            for (int64_t j = 0; j < seg_count; ++j) {
                auto sv = batch_data[j];
                fn(BsonView(reinterpret_cast<const uint8_t*>(sv.data()),
                            sv.size()),
                   row_offsets[seg_start + j],
                   value_offsets[seg_start + j]);
            }
            seg_start = seg_end;
        }
    }

    void
    BulkArrayAt(milvus::OpContext* op_ctx,
                std::function<void(ScalarFieldProto&&, size_t)> fn,
                const int64_t* offsets,
                int64_t count) const override {
        if (!IsChunkedArrayColumnDataType(data_type_)) {
            ThrowInfo(ErrorCode::Unsupported,
                      "[StorageV2] BulkArrayAt only supported for "
                      "ChunkedArrayColumn");
        }
        if (count == 0) {
            return;
        }
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
        auto ca = group_->GetGroupChunks(op_ctx, cids);

        // Vortex fast path: single batched take() across all cells.
        // ARRAY is stored as proto-serialized binary; parse directly to
        // ScalarFieldProto, bypassing the Array/ArrayView construction entirely.
        std::vector<int64_t> sorted;
        if (auto vortex_result =
                TryVortexStringTake(ca, cids, offsets, count, sorted)) {
            auto& [table, flat_views] = *vortex_result;
            for (int64_t i = 0; i < count; ++i) {
                auto it = std::lower_bound(
                    sorted.begin(), sorted.end(), offsets[i]);
                auto sv = flat_views[it - sorted.begin()];
                ScalarFieldProto proto;
                proto.ParseFromArray(sv.data(), sv.size());
                fn(std::move(proto), i);
            }
            return;
        }

        int64_t seg_start = 0;
        while (seg_start < count) {
            auto cur_cid = cids[seg_start];
            int64_t seg_end = seg_start + 1;
            while (seg_end < count && cids[seg_end] == cur_cid) {
                ++seg_end;
            }
            int64_t seg_count = seg_end - seg_start;
            auto* group_chunk = ca->get_cell_of(cur_cid);
            auto chunk = group_chunk->GetChunk(field_id_);

            std::vector<int64_t> local_offsets(seg_count);
            for (int64_t j = 0; j < seg_count; ++j) {
                local_offsets[j] = static_cast<int64_t>(
                    offsets_in_chunk[seg_start + j]);
            }
            auto owned = chunk->BulkOwnData(local_offsets.data(), seg_count);
            for (int64_t j = 0; j < seg_count; ++j) {
                ScalarFieldProto proto;
                proto.ParseFromString(owned[j]);
                fn(std::move(proto), seg_start + j);
            }
            seg_start = seg_end;
        }
    }

    // TODO: static_cast<VectorArrayChunk*> is not compatible with VortexChunk.
    // When vortex supports VectorArray type, this needs DataView migration.
    void
    BulkVectorArrayAt(milvus::OpContext* op_ctx,
                      std::function<void(VectorFieldProto&&, size_t)> fn,
                      const int64_t* offsets,
                      int64_t count) const override {
        if (!IsChunkedVectorArrayColumnDataType(data_type_)) {
            ThrowInfo(ErrorCode::Unsupported,
                      "[StorageV2] BulkVectorArrayAt only supported for "
                      "ChunkedVectorArrayColumn");
        }
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
        auto ca = group_->GetGroupChunks(op_ctx, cids);
        for (int64_t i = 0; i < count; i++) {
            auto* group_chunk = ca->get_cell_of(cids[i]);
            auto chunk = group_chunk->GetChunk(field_id_);
            auto array = static_cast<VectorArrayChunk*>(chunk.get())
                             ->View(offsets_in_chunk[i])
                             .output_data();
            fn(std::move(array), i);
        }
    }

    void
    BuildValidRowIds(milvus::OpContext* op_ctx) override {
        if (!field_meta_.is_nullable()) {
            return;
        }
        auto total_rows = NumRows();
        auto total_chunks = num_chunks();
        valid_data_.resize(total_rows);
        valid_count_per_chunk_.resize(total_chunks);

        int64_t logical_offset = 0;
        for (int64_t i = 0; i < total_chunks; i++) {
            auto group_chunk = group_->GetGroupChunk(op_ctx, i);
            auto chunk = group_chunk.get()->GetChunk(field_id_);
            auto rows = chunk->RowNums();
            int64_t valid_count = 0;
            for (int64_t j = 0; j < rows; j++) {
                if (chunk->IsValid(j)) {
                    valid_data_[logical_offset + j] = true;
                    valid_count++;
                } else {
                    valid_data_[logical_offset + j] = false;
                }
            }
            valid_count_per_chunk_[i] = valid_count;
            logical_offset += rows;
        }

        num_valid_rows_until_chunk_.clear();
        num_valid_rows_until_chunk_.reserve(total_chunks + 1);
        num_valid_rows_until_chunk_.push_back(0);
        for (int64_t i = 0; i < total_chunks; i++) {
            num_valid_rows_until_chunk_.push_back(
                num_valid_rows_until_chunk_.back() + valid_count_per_chunk_[i]);
        }

        BuildOffsetMapping();
    }

 private:
    // Vortex fast path for string/JSON bulk take.
    // Returns {table, flat_string_views} if the column is backed by VortexChunk;
    // returns nullopt otherwise.  The caller MUST keep `table` alive while using
    // the string_views (they point into Arrow column buffers).
    std::optional<std::pair<std::shared_ptr<arrow::Table>,
                            std::vector<std::string_view>>>
    TryVortexStringTake(
        const std::shared_ptr<CellAccessor<GroupChunk>>& ca,
        const std::vector<cachinglayer::cid_t>& cids,
        const int64_t* offsets,
        int64_t count,
        std::vector<int64_t>& sorted_out) const {
        auto* vchunk = dynamic_cast<VortexChunk*>(
            ca->get_cell_of(cids[0])->GetChunk(field_id_).get());
        if (!vchunk) {
            return std::nullopt;
        }

        // Build sorted, deduped segment-level offsets for a single take().
        sorted_out.assign(offsets, offsets + count);
        std::sort(sorted_out.begin(), sorted_out.end());
        sorted_out.erase(
            std::unique(sorted_out.begin(), sorted_out.end()), sorted_out.end());

        auto result = vchunk->GetReader()->take(sorted_out);
        AssertInfo(result.ok(),
                   "Vortex batched string take failed: {}",
                   result.status().ToString());
        auto table = *result;

        int col_idx =
            table->schema()->GetFieldIndex(vchunk->GetFieldName());
        AssertInfo(col_idx >= 0,
                   "BulkVortex: field '{}' not found in take result",
                   vchunk->GetFieldName());
        auto col = table->column(col_idx);

        std::vector<std::string_view> flat_views;
        flat_views.reserve(col->length());
        for (int i = 0; i < col->num_chunks(); ++i) {
            auto arr = col->chunk(i);
            if (auto sa =
                    std::dynamic_pointer_cast<arrow::StringArray>(arr)) {
                for (int64_t j = 0; j < sa->length(); ++j) {
                    auto sv = sa->GetView(j);
                    flat_views.emplace_back(sv.data(), sv.size());
                }
            } else if (auto la =
                           std::dynamic_pointer_cast<arrow::LargeStringArray>(
                               arr)) {
                for (int64_t j = 0; j < la->length(); ++j) {
                    auto sv = la->GetView(j);
                    flat_views.emplace_back(sv.data(), sv.size());
                }
            } else {
                // Fallback: BinaryArray / LargeBinaryArray
                if (auto ba =
                        std::dynamic_pointer_cast<arrow::BinaryArray>(arr)) {
                    for (int64_t j = 0; j < ba->length(); ++j) {
                        auto sv = ba->GetView(j);
                        flat_views.emplace_back(sv.data(), sv.size());
                    }
                } else if (auto lba =
                               std::dynamic_pointer_cast<
                                   arrow::LargeBinaryArray>(arr)) {
                    for (int64_t j = 0; j < lba->length(); ++j) {
                        auto sv = lba->GetView(j);
                        flat_views.emplace_back(sv.data(), sv.size());
                    }
                }
            }
        }
        return std::make_pair(std::move(table), std::move(flat_views));
    }

    std::shared_ptr<ChunkedColumnGroup> group_;
    FieldId field_id_;
    const FieldMeta field_meta_;
    DataType data_type_;
};

}  // namespace milvus