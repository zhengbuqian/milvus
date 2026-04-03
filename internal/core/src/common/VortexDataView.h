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

#include <chrono>
#include <cstdio>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "arrow/api.h"
#include "common/ChunkDataView.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Json.h"
#include "common/Types.h"
#include "common/VectorTrait.h"
#include "milvus-storage/reader.h"

namespace milvus {

/// Get a string_view from an Arrow array element, handling both
/// BinaryArray/StringArray and BinaryViewArray/StringViewArray.
/// Vortex may return either type depending on data size and version.
inline std::string_view
GetBinaryView(const std::shared_ptr<arrow::Array>& arr, int64_t idx) {
    switch (arr->type_id()) {
        case arrow::Type::BINARY:
        case arrow::Type::STRING: {
            auto typed =
                std::static_pointer_cast<arrow::BinaryArray>(arr);
            auto sv = typed->GetView(idx);
            return {sv.data(), sv.size()};
        }
        case arrow::Type::LARGE_BINARY:
        case arrow::Type::LARGE_STRING: {
            auto typed =
                std::static_pointer_cast<arrow::LargeBinaryArray>(arr);
            auto sv = typed->GetView(idx);
            return {sv.data(), sv.size()};
        }
        case arrow::Type::BINARY_VIEW:
        case arrow::Type::STRING_VIEW: {
            auto typed =
                std::static_pointer_cast<arrow::BinaryViewArray>(arr);
            auto sv = typed->GetView(idx);
            return {sv.data(), sv.size()};
        }
        default:
            AssertInfo(false,
                       "GetBinaryView: unsupported array type {}",
                       arr->type()->ToString());
            return {};
    }
}

/// Shared state for all VortexDataView types.
///
/// Provides point query (via Reader::take) and bulk query
/// (via ChunkReader::get_chunks) primitives.
struct VortexDataViewCore {
    std::shared_ptr<milvus_storage::api::Reader> reader;
    std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader;
    std::vector<int64_t> chunk_indices;
    int column_in_batch;
    std::string field_name;
    int64_t row_start;
    int64_t total_rows;
    bool nullable;

    /// Point query: read a single row via Reader::take().
    /// Returns the Arrow Table (caller extracts the needed column).
    std::shared_ptr<arrow::Table>
    DoPointQuery(int64_t local_idx) const {
        std::vector<int64_t> indices = {row_start + local_idx};
        auto result = reader->take(indices);
        AssertInfo(result.ok(),
                   "VortexDataView: take() failed: {}",
                   result.status().ToString());
        return result.ValueOrDie();
    }

    /// Batch point query: read multiple rows via a single Reader::take().
    /// @param local_offsets  Local offsets within this chunk/cell (may be unsorted).
    /// @param count          Number of offsets.
    /// @param reorder_map    Output: maps sorted result index → original index.
    ///                       Empty if input was already sorted+unique.
    /// Returns the Arrow Table with rows in sorted order.
    std::shared_ptr<arrow::Table>
    DoBatchPointQuery(const int64_t* local_offsets,
                      int64_t count,
                      std::vector<int64_t>& reorder_map) const {
        using Clock = std::chrono::steady_clock;
        using Us = std::chrono::microseconds;
        auto t_total0 = Clock::now();

        // Build (global_index, original_position) pairs
        std::vector<std::pair<int64_t, int64_t>> idx_pairs;
        idx_pairs.reserve(count);
        for (int64_t i = 0; i < count; ++i) {
            idx_pairs.emplace_back(row_start + local_offsets[i], i);
        }

        // Sort by global index (required by Reader::take)
        auto t_sort0 = Clock::now();
        std::sort(idx_pairs.begin(), idx_pairs.end());
        auto t_sort1 = Clock::now();

        // Deduplicate and build take indices
        std::vector<int64_t> sorted_indices;
        sorted_indices.reserve(count);
        // Map from deduped position → list of original positions
        std::vector<std::vector<int64_t>> dedup_to_orig;
        dedup_to_orig.reserve(count);

        for (size_t i = 0; i < idx_pairs.size(); ++i) {
            if (sorted_indices.empty() ||
                sorted_indices.back() != idx_pairs[i].first) {
                sorted_indices.push_back(idx_pairs[i].first);
                dedup_to_orig.push_back({idx_pairs[i].second});
            } else {
                dedup_to_orig.back().push_back(idx_pairs[i].second);
            }
        }

        auto t_take0 = Clock::now();
        auto result = reader->take(sorted_indices);
        auto t_take1 = Clock::now();
        AssertInfo(result.ok(),
                   "VortexDataView: batch take() failed: {}",
                   result.status().ToString());

        // Build reorder map: result[i] in sorted_indices order
        // → should be placed at reorder_map[i] in original order
        bool needs_reorder = (sorted_indices.size() != static_cast<size_t>(count));
        if (!needs_reorder) {
            for (int64_t i = 0; i < count; ++i) {
                if (sorted_indices[i] !=
                    row_start + local_offsets[i]) {
                    needs_reorder = true;
                    break;
                }
            }
        }

        if (needs_reorder) {
            reorder_map.resize(count);
            for (size_t di = 0; di < dedup_to_orig.size(); ++di) {
                for (auto orig_pos : dedup_to_orig[di]) {
                    reorder_map[orig_pos] = static_cast<int64_t>(di);
                }
            }
        } else {
            reorder_map.clear();
        }

        auto t_total1 = Clock::now();
        fprintf(stderr,
                "[VortexPerf] DoBatchPointQuery n=%ld dedup=%zu sort=%ldus take=%ldus total=%ldus\n",
                count,
                sorted_indices.size(),
                std::chrono::duration_cast<Us>(t_sort1 - t_sort0).count(),
                std::chrono::duration_cast<Us>(t_take1 - t_take0).count(),
                std::chrono::duration_cast<Us>(t_total1 - t_total0).count());

        return result.ValueOrDie();
    }

    /// Find the column index for our field in a Table returned by take().
    int
    FindFieldColumn(const std::shared_ptr<arrow::Table>& table) const {
        auto idx = table->schema()->GetFieldIndex(field_name);
        AssertInfo(idx >= 0,
                   "VortexDataView: field '{}' not found in take() result",
                   field_name);
        return idx;
    }

    /// Range query: read [row_start, row_start+total_rows) via Reader::take().
    /// More efficient than bulk query when only a subset of the chunk is needed.
    /// Returns the Arrow Table with only the requested rows.
    std::shared_ptr<arrow::Table>
    DoRangeQuery() const {
        std::vector<int64_t> indices;
        indices.reserve(total_rows);
        for (int64_t i = 0; i < total_rows; ++i) {
            indices.push_back(row_start + i);
        }
        auto result = reader->take(indices);
        AssertInfo(result.ok(),
                   "VortexDataView: take() failed for range [{}, {}): {}",
                   row_start,
                   row_start + total_rows,
                   result.status().ToString());
        return result.ValueOrDie();
    }

    /// Bulk query: read all row groups via ChunkReader::get_chunks().
    /// Returns RecordBatches (caller extracts column at column_in_batch).
    std::vector<std::shared_ptr<arrow::RecordBatch>>
    DoBulkQuery() const {
        auto result = chunk_reader->get_chunks(chunk_indices);
        AssertInfo(result.ok(),
                   "VortexDataView: get_chunks() failed: {}",
                   result.status().ToString());
        return result.ValueOrDie();
    }

    /// Extract field column arrays — uses range query when row range
    /// is a subset of this cell's row groups, otherwise bulk query.
    std::vector<std::shared_ptr<arrow::Array>>
    ExtractFieldArrays() const {
        // Compute the expected rows from this cell's chunk_indices,
        // not from the entire chunk_reader (which covers all cells).
        auto rows_result = chunk_reader->get_chunk_rows();
        int64_t cell_rows = 0;
        if (rows_result.ok()) {
            const auto& all_rows = rows_result.ValueOrDie();
            for (auto idx : chunk_indices) {
                if (idx >= 0 &&
                    static_cast<size_t>(idx) < all_rows.size()) {
                    cell_rows += all_rows[idx];
                }
            }
        }

        if (cell_rows > 0 && total_rows < cell_rows) {
            // Sub-range: use take() for partial decompression
            auto table = DoRangeQuery();
            auto col_idx = FindFieldColumn(table);
            auto chunked = table->column(col_idx);
            std::vector<std::shared_ptr<arrow::Array>> arrays;
            arrays.reserve(chunked->num_chunks());
            for (int i = 0; i < chunked->num_chunks(); ++i) {
                arrays.push_back(chunked->chunk(i));
            }
            return arrays;
        }

        // Full chunk: use get_chunks() for bulk decompression
        auto batches = DoBulkQuery();
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        arrays.reserve(batches.size());
        for (const auto& batch : batches) {
            AssertInfo(column_in_batch < batch->num_columns(),
                       "VortexDataView: column {} out of range ({})",
                       column_in_batch,
                       batch->num_columns());
            arrays.push_back(batch->column(column_in_batch));
        }
        return arrays;
    }

    /// Extract validity bitmap from Arrow arrays.
    void
    ExtractValidity(const std::vector<std::shared_ptr<arrow::Array>>& arrays,
                    FixedVector<bool>& valid_out) const {
        if (!nullable) {
            return;
        }
        valid_out.reserve(total_rows);
        for (const auto& arr : arrays) {
            for (int64_t i = 0; i < arr->length(); ++i) {
                valid_out.push_back(arr->IsValid(i));
            }
        }
    }
};

// ============================================================================
// VortexNumericDataView<T> — for int8/16/32/64, float, double
// ============================================================================

template <typename T>
class VortexNumericDataView : public ChunkDataView<T> {
 public:
    VortexNumericDataView(VortexDataViewCore core)
        : core_(std::move(core)) {
    }

    const T&
    operator[](int64_t idx) const override {
        AssertInfo(idx >= 0 && idx < core_.total_rows,
                   "VortexNumericDataView: index {} out of range ({})",
                   idx,
                   core_.total_rows);
        cached_table_ = core_.DoPointQuery(idx);
        auto col_idx = core_.FindFieldColumn(cached_table_);
        auto chunked = cached_table_->column(col_idx);
        auto arr = std::static_pointer_cast<
            typename arrow::CTypeTraits<T>::ArrayType>(chunked->chunk(0));
        cached_value_ = arr->Value(0);
        return cached_value_;
    }

    const T*
    Data() const override {
        EnsureLoaded();
        return data_buf_.data();
    }

    const T*
    DataByOffsets(const int64_t* offsets, int64_t count) const override {
        // Single batch take() with sort+dedup handling.
        std::vector<int64_t> reorder_map;
        cached_table_ =
            core_.DoBatchPointQuery(offsets, count, reorder_map);
        auto col_idx = core_.FindFieldColumn(cached_table_);
        auto chunked = cached_table_->column(col_idx);

        // Flatten sorted result into contiguous buffer
        std::vector<T> sorted_buf;
        sorted_buf.reserve(chunked->length());
        for (int i = 0; i < chunked->num_chunks(); ++i) {
            auto typed = std::static_pointer_cast<
                typename arrow::CTypeTraits<T>::ArrayType>(
                chunked->chunk(i));
            auto old_size = sorted_buf.size();
            sorted_buf.resize(old_size + typed->length());
            std::memcpy(sorted_buf.data() + old_size,
                        typed->raw_values(),
                        typed->length() * sizeof(T));
        }

        if (reorder_map.empty()) {
            // Already in correct order
            data_buf_ = std::move(sorted_buf);
        } else {
            // Reorder to match original offsets
            data_buf_.resize(count);
            for (int64_t i = 0; i < count; ++i) {
                data_buf_[i] = sorted_buf[reorder_map[i]];
            }
        }
        return data_buf_.data();
    }

    const bool*
    ValidData() const override {
        EnsureLoaded();
        return valid_data_.empty() ? nullptr : valid_data_.data();
    }

    int64_t
    RowCount() const override {
        return core_.total_rows;
    }

    void
    SetValidData(const bool* valid) override {
        override_valid_ = valid;
    }

 private:
    void
    EnsureLoaded() const {
        if (data_loaded_) {
            return;
        }
        auto arrays = core_.ExtractFieldArrays();
        data_buf_.reserve(core_.total_rows);
        for (const auto& arr : arrays) {
            auto typed = std::static_pointer_cast<
                typename arrow::CTypeTraits<T>::ArrayType>(arr);
            auto len = typed->length();
            auto old_size = data_buf_.size();
            data_buf_.resize(old_size + len);
            std::memcpy(
                data_buf_.data() + old_size, typed->raw_values(),
                len * sizeof(T));
        }
        core_.ExtractValidity(arrays, valid_data_);
        data_loaded_ = true;
    }

    VortexDataViewCore core_;
    mutable T cached_value_{};
    mutable std::shared_ptr<arrow::Table> cached_table_;
    mutable std::vector<T> data_buf_;
    mutable FixedVector<bool> valid_data_;
    mutable bool data_loaded_ = false;
    const bool* override_valid_ = nullptr;
};

// ============================================================================
// VortexBoolDataView — bool needs bit unpacking from Arrow BooleanArray
// ============================================================================

class VortexBoolDataView : public ChunkDataView<bool> {
 public:
    VortexBoolDataView(VortexDataViewCore core) : core_(std::move(core)) {
    }

    const bool&
    operator[](int64_t idx) const override {
        AssertInfo(idx >= 0 && idx < core_.total_rows,
                   "VortexBoolDataView: index {} out of range ({})",
                   idx,
                   core_.total_rows);
        cached_table_ = core_.DoPointQuery(idx);
        auto col_idx = core_.FindFieldColumn(cached_table_);
        auto chunked = cached_table_->column(col_idx);
        auto arr =
            std::static_pointer_cast<arrow::BooleanArray>(chunked->chunk(0));
        cached_value_ = arr->Value(0);
        return cached_value_;
    }

    const bool*
    Data() const override {
        EnsureLoaded();
        return data_buf_.data();
    }

    const bool*
    ValidData() const override {
        EnsureLoaded();
        return valid_data_.empty() ? nullptr : valid_data_.data();
    }

    int64_t
    RowCount() const override {
        return core_.total_rows;
    }

    void
    SetValidData(const bool* valid) override {
        override_valid_ = valid;
    }

 private:
    void
    EnsureLoaded() const {
        if (data_loaded_) {
            return;
        }
        auto arrays = core_.ExtractFieldArrays();
        data_buf_.reserve(core_.total_rows);
        for (const auto& arr : arrays) {
            auto bool_arr =
                std::static_pointer_cast<arrow::BooleanArray>(arr);
            for (int64_t i = 0; i < bool_arr->length(); ++i) {
                data_buf_.push_back(bool_arr->Value(i));
            }
        }
        core_.ExtractValidity(arrays, valid_data_);
        data_loaded_ = true;
    }

    VortexDataViewCore core_;
    mutable bool cached_value_ = false;
    mutable std::shared_ptr<arrow::Table> cached_table_;
    mutable FixedVector<bool> data_buf_;  // Not std::vector<bool> (bitset)
    mutable FixedVector<bool> valid_data_;
    mutable bool data_loaded_ = false;
    const bool* override_valid_ = nullptr;
};

// ============================================================================
// VortexStringDataView — string_view pointing into Arrow buffers
// ============================================================================

class VortexStringDataView : public ChunkDataView<std::string_view> {
 public:
    VortexStringDataView(VortexDataViewCore core) : core_(std::move(core)) {
    }

    const std::string_view&
    operator[](int64_t idx) const override {
        AssertInfo(idx >= 0 && idx < core_.total_rows,
                   "VortexStringDataView: index {} out of range ({})",
                   idx,
                   core_.total_rows);
        cached_table_ = core_.DoPointQuery(idx);
        auto col_idx = core_.FindFieldColumn(cached_table_);
        auto chunked = cached_table_->column(col_idx);
        cached_value_ = GetBinaryView(chunked->chunk(0), 0);
        return cached_value_;
    }

    const std::string_view*
    Data() const override {
        EnsureLoaded();
        return views_.data();
    }

    const std::string_view*
    DataByOffsets(const int64_t* offsets, int64_t count) const override {
        // Single batch take() with sort+dedup handling.
        std::vector<int64_t> reorder_map;
        cached_table_ =
            core_.DoBatchPointQuery(offsets, count, reorder_map);
        auto col_idx = core_.FindFieldColumn(cached_table_);
        auto chunked = cached_table_->column(col_idx);

        // Build sorted string_views
        cached_arrays_.clear();
        std::vector<std::string_view> sorted_views;
        sorted_views.reserve(chunked->length());
        for (int i = 0; i < chunked->num_chunks(); ++i) {
            auto chunk = chunked->chunk(i);
            cached_arrays_.push_back(chunk);
            for (int64_t j = 0; j < chunk->length(); ++j) {
                sorted_views.push_back(GetBinaryView(chunk, j));
            }
        }

        if (reorder_map.empty()) {
            views_ = std::move(sorted_views);
        } else {
            views_.resize(count);
            for (int64_t i = 0; i < count; ++i) {
                views_[i] = sorted_views[reorder_map[i]];
            }
        }
        return views_.data();
    }

    const bool*
    ValidData() const override {
        EnsureLoaded();
        return valid_data_.empty() ? nullptr : valid_data_.data();
    }

    int64_t
    RowCount() const override {
        return core_.total_rows;
    }

    void
    SetValidData(const bool* valid) override {
        override_valid_ = valid;
    }

 private:
    void
    EnsureLoaded() const {
        if (data_loaded_) {
            return;
        }
        auto arrays = core_.ExtractFieldArrays();
        // Keep Arrow arrays alive so string_views remain valid
        cached_arrays_ = arrays;
        views_.reserve(core_.total_rows);
        for (const auto& arr : arrays) {
            for (int64_t i = 0; i < arr->length(); ++i) {
                views_.push_back(GetBinaryView(arr, i));
            }
        }
        core_.ExtractValidity(arrays, valid_data_);
        data_loaded_ = true;
    }

    VortexDataViewCore core_;
    mutable std::string_view cached_value_;
    mutable std::shared_ptr<arrow::Table> cached_table_;
    mutable std::vector<std::shared_ptr<arrow::Array>> cached_arrays_;
    mutable std::vector<std::string_view> views_;
    mutable FixedVector<bool> valid_data_;
    mutable bool data_loaded_ = false;
    const bool* override_valid_ = nullptr;
};

// ============================================================================
// VortexJsonDataView — JSON stored as binary strings in Arrow
// ============================================================================

class VortexJsonDataView : public ChunkDataView<Json> {
 public:
    VortexJsonDataView(VortexDataViewCore core) : core_(std::move(core)) {
    }

    const Json&
    operator[](int64_t idx) const override {
        AssertInfo(idx >= 0 && idx < core_.total_rows,
                   "VortexJsonDataView: index {} out of range ({})",
                   idx,
                   core_.total_rows);
        cached_table_ = core_.DoPointQuery(idx);
        auto col_idx = core_.FindFieldColumn(cached_table_);
        auto chunked = cached_table_->column(col_idx);
        auto arr =
            std::static_pointer_cast<arrow::BinaryArray>(chunked->chunk(0));
        auto sv = arr->GetView(0);
        cached_value_ = Json(sv.data(), sv.size());
        return cached_value_;
    }

    const Json*
    Data() const override {
        EnsureLoaded();
        return data_buf_.data();
    }

    const bool*
    ValidData() const override {
        EnsureLoaded();
        return valid_data_.empty() ? nullptr : valid_data_.data();
    }

    int64_t
    RowCount() const override {
        return core_.total_rows;
    }

    void
    SetValidData(const bool* valid) override {
        override_valid_ = valid;
    }

 private:
    void
    EnsureLoaded() const {
        if (data_loaded_) {
            return;
        }
        auto arrays = core_.ExtractFieldArrays();
        // Keep Arrow arrays alive for underlying string data
        cached_arrays_ = arrays;
        data_buf_.reserve(core_.total_rows);
        for (const auto& arr : arrays) {
            auto str_arr =
                std::static_pointer_cast<arrow::BinaryArray>(arr);
            for (int64_t i = 0; i < str_arr->length(); ++i) {
                auto sv = str_arr->GetView(i);
                data_buf_.emplace_back(sv.data(), sv.size());
            }
        }
        core_.ExtractValidity(arrays, valid_data_);
        data_loaded_ = true;
    }

    VortexDataViewCore core_;
    mutable Json cached_value_;
    mutable std::shared_ptr<arrow::Table> cached_table_;
    mutable std::vector<std::shared_ptr<arrow::Array>> cached_arrays_;
    mutable std::vector<Json> data_buf_;
    mutable FixedVector<bool> valid_data_;
    mutable bool data_loaded_ = false;
    const bool* override_valid_ = nullptr;
};

// ============================================================================
// VortexVectorDataView<VectorType> — dense vector types
// ============================================================================

template <typename VectorType>
class VortexVectorDataView
    : public ChunkDataView<
          VectorType,
          std::enable_if_t<std::is_base_of_v<VectorTrait, VectorType> &&
                           !std::is_same_v<VectorType, VectorArray>>> {
 public:
    using ValueType = typename VectorType::embedded_type;

    VortexVectorDataView(VortexDataViewCore core, int64_t dim)
        : core_(std::move(core)), dim_(dim) {
    }

    const ValueType&
    operator[](int64_t idx) const override {
        AssertInfo(idx >= 0 && idx < core_.total_rows,
                   "VortexVectorDataView: index {} out of range ({})",
                   idx,
                   core_.total_rows);
        cached_table_ = core_.DoPointQuery(idx);
        auto col_idx = core_.FindFieldColumn(cached_table_);
        auto chunked = cached_table_->column(col_idx);
        auto arr = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(
            chunked->chunk(0));
        auto* ptr =
            reinterpret_cast<const ValueType*>(arr->GetValue(0));
        // Cache the first element (consistent with ContiguousDataView
        // vector specialization which returns data_[idx * dim_])
        cached_value_ = *ptr;
        return cached_value_;
    }

    const ValueType*
    Data() const override {
        EnsureLoaded();
        return reinterpret_cast<const ValueType*>(data_buf_->data());
    }

    const bool*
    ValidData() const override {
        EnsureLoaded();
        return valid_data_.empty() ? nullptr : valid_data_.data();
    }

    int64_t
    RowCount() const override {
        return core_.total_rows;
    }

    void
    SetValidData(const bool* valid) override {
        override_valid_ = valid;
    }

 private:
    void
    EnsureLoaded() const {
        if (data_loaded_) {
            return;
        }
        auto arrays = core_.ExtractFieldArrays();
        int64_t total_bytes = core_.total_rows * dim_ * sizeof(ValueType);
        data_buf_ = arrow::AllocateBuffer(total_bytes).ValueOrDie();
        auto* dst = data_buf_->mutable_data();
        int64_t offset = 0;
        for (const auto& arr : arrays) {
            auto fixed_arr =
                std::static_pointer_cast<arrow::FixedSizeBinaryArray>(arr);
            auto len = fixed_arr->length();
            auto byte_width = fixed_arr->byte_width();
            std::memcpy(dst + offset, fixed_arr->raw_values(),
                        len * byte_width);
            offset += len * byte_width;
        }
        core_.ExtractValidity(arrays, valid_data_);
        data_loaded_ = true;
    }

    VortexDataViewCore core_;
    int64_t dim_;
    mutable ValueType cached_value_{};
    mutable std::shared_ptr<arrow::Table> cached_table_;
    mutable std::shared_ptr<arrow::Buffer> data_buf_;
    mutable FixedVector<bool> valid_data_;
    mutable bool data_loaded_ = false;
    const bool* override_valid_ = nullptr;
};

}  // namespace milvus
