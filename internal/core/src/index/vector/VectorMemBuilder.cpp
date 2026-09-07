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

#include "index/vector/VectorMemBuilder.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <string_view>
#include <type_traits>
#include <utility>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "index/vector/VectorMemArtifact.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/segcore_error_code.h"

namespace milvus::index {
namespace {

int64_t
CheckedAdd(int64_t left, size_t right, std::string_view label) {
    AssertInfo(
        right <= static_cast<size_t>(std::numeric_limits<int64_t>::max()),
        "{} increment exceeds int64",
        label);
    const auto increment = static_cast<int64_t>(right);
    AssertInfo(left <= std::numeric_limits<int64_t>::max() - increment,
               "{} count overflows int64",
               label);
    return left + increment;
}

size_t
CheckedBytes(size_t rows, size_t row_bytes, std::string_view label) {
    AssertInfo(row_bytes == 0 ||
                   rows <= std::numeric_limits<size_t>::max() / row_bytes,
               "{} byte size overflows size_t",
               label);
    return rows * row_bytes;
}

size_t
CountValid(size_t n, const bool* valid) {
    if (valid == nullptr) {
        return n;
    }
    size_t result = 0;
    for (size_t i = 0; i < n; ++i) {
        result += valid[i] ? 1 : 0;
    }
    return result;
}

}  // namespace

template <typename T>
VectorMemBuilder<T>::VectorMemBuilder(DataType elem_type,
                                      IndexType index_type,
                                      MetricType metric_type,
                                      IndexVersion version,
                                      int64_t dim,
                                      knowhere::Json build_params,
                                      bool use_knowhere_build_pool)
    : engine_(PhysicalVectorDataType<T>(),
              elem_type,
              std::move(index_type),
              std::move(metric_type),
              version,
              use_knowhere_build_pool),
      build_params_(std::move(build_params)) {
    constexpr auto physical_type = PhysicalVectorDataType<T>();
    AssertInfo(dim >= 0 && (dim != 0 ||
                            physical_type == DataType::VECTOR_SPARSE_U32_F32),
               "memory vector dimension {} is invalid for type {}",
               dim,
               physical_type);
    if constexpr (physical_type == DataType::VECTOR_BINARY) {
        AssertInfo(dim % 8 == 0,
                   "binary vector dimension {} is not byte aligned",
                   dim);
    }
    engine_.SetDim(dim);

    expected_rows_ =
        GetValueFromConfig<int64_t>(build_params_, INDEX_NUM_ROWS_KEY);
    if (expected_rows_.has_value()) {
        AssertInfo(*expected_rows_ >= 0,
                   "memory vector expected row count is negative");
        const auto nullable =
            GetValueFromConfig<bool>(build_params_, "nullable").value_or(false);
        if (!nullable && elem_type == DataType::NONE) {
            const auto reserve_rows = static_cast<size_t>(*expected_rows_);
            if constexpr (std::is_same_v<T, sparse_u32_f32>) {
                sparse_rows_.reserve(reserve_rows);
            } else {
                const auto row_bytes =
                    vector_bytes_per_element(physical_type, engine_.Dim());
                buffer_.reserve(
                    CheckedBytes(reserve_rows, row_bytes, "memory vector"));
            }
        }
    }
}

template <typename T>
void
VectorMemBuilder<T>::EnsureOpen(const char* operation) const {
    AssertInfo(!failed_, "cannot {} a failed memory vector builder", operation);
    AssertInfo(!sealed_, "cannot {} a sealed memory vector builder", operation);
}

template <typename T>
void
VectorMemBuilder<T>::EnsureValidityCapacity(size_t required) {
    if (required <= validity_capacity_) {
        return;
    }
    size_t capacity = validity_capacity_ == 0 ? 64 : validity_capacity_;
    if (expected_rows_.has_value()) {
        capacity = std::max(capacity, static_cast<size_t>(*expected_rows_));
    }
    while (capacity < required) {
        AssertInfo(capacity <= std::numeric_limits<size_t>::max() / 2,
                   "memory vector validity capacity overflows size_t");
        capacity *= 2;
    }
    auto next = std::make_unique<bool[]>(capacity);
    if (validity_ != nullptr) {
        std::copy_n(validity_.get(), static_cast<size_t>(rows_), next.get());
    }
    validity_ = std::move(next);
    validity_capacity_ = capacity;
}

template <typename T>
void
VectorMemBuilder<T>::AppendValidity(size_t logical_rows,
                                    const bool* valid,
                                    int64_t next_rows) {
    const auto old_rows = static_cast<size_t>(rows_);
    if (valid != nullptr && !saw_validity_) {
        EnsureValidityCapacity(static_cast<size_t>(next_rows));
        std::fill_n(validity_.get(), old_rows, true);
        saw_validity_ = true;
    } else if (saw_validity_) {
        EnsureValidityCapacity(static_cast<size_t>(next_rows));
    }
    if (saw_validity_) {
        for (size_t i = 0; i < logical_rows; ++i) {
            validity_[old_rows + i] = valid == nullptr ? true : valid[i];
        }
    }
}

template <typename T>
void
VectorMemBuilder<T>::ReleaseStaging() noexcept {
    std::vector<uint8_t>().swap(buffer_);
    std::vector<SparseRow>().swap(sparse_rows_);
    validity_.reset();
    validity_capacity_ = 0;
    decltype(scalar_info_)().swap(scalar_info_);
    std::vector<size_t>().swap(emb_list_offsets_);
}

template <typename T>
BuilderInputSpec
VectorMemBuilder<T>::InputSpec() const {
    BuilderInputSpec spec;
    spec.form = BuilderInputSpec::Contiguous;
    spec.needs_second_pass = false;

    const auto opt_fields =
        GetValueFromConfig<OptFieldT>(build_params_, VEC_OPT_FIELDS);
    const auto partition_isolation =
        GetValueFromConfig<bool>(build_params_, PARTITION_KEY_ISOLATION_KEY)
            .value_or(false);
    if (opt_fields.has_value() &&
        engine_.Raw().IsAdditionalScalarSupported(partition_isolation)) {
        spec.side_inputs.reserve(opt_fields->size());
        for (const auto& [field_id, _] : *opt_fields) {
            spec.side_inputs.emplace_back(field_id);
        }
        std::sort(spec.side_inputs.begin(), spec.side_inputs.end());
    }
    return spec;
}

template <typename T>
void
VectorMemBuilder<T>::Add(size_t n, const T* values, const bool* valid) {
    try {
        EnsureOpen("add to");
        AssertInfo(engine_.ElemType() == DataType::NONE,
                   "VECTOR_ARRAY input must use AddEmbeddingBatch");
        const auto next_rows =
            CheckedAdd(rows_, n, "memory vector logical row");
        if (expected_rows_.has_value()) {
            AssertInfo(next_rows <= *expected_rows_,
                       "memory vector input exceeds expected row count {}",
                       *expected_rows_);
        }

        const auto valid_parents = CountValid(n, valid);
        const auto vectors = valid_parents;
        AssertInfo(values != nullptr || vectors == 0,
                   "memory vector input has null compact values for {} "
                   "valid rows",
                   vectors);

        AppendValidity(n, valid, next_rows);

        if constexpr (std::is_same_v<T, sparse_u32_f32>) {
            const auto old_size = sparse_rows_.size();
            AssertInfo(vectors <= std::numeric_limits<size_t>::max() - old_size,
                       "sparse vector row count overflows size_t");
            sparse_rows_.resize(old_size + vectors);
            if (vectors != 0) {
                const auto* sparse_values =
                    reinterpret_cast<const SparseRow*>(values);
                std::copy_n(
                    sparse_values, vectors, sparse_rows_.data() + old_size);
                for (size_t i = 0; i < vectors; ++i) {
                    sparse_dim_ =
                        std::max(sparse_dim_,
                                 static_cast<int64_t>(sparse_values[i].dim()));
                }
            }
        } else {
            const auto row_bytes = vector_bytes_per_element(
                PhysicalVectorDataType<T>(), engine_.Dim());
            const auto bytes =
                CheckedBytes(vectors, row_bytes, "memory vector input");
            AssertInfo(
                bytes <= std::numeric_limits<size_t>::max() - buffer_.size(),
                "memory vector buffer size overflows size_t");
            const auto old_size = buffer_.size();
            buffer_.resize(old_size + bytes);
            if (bytes != 0) {
                std::memcpy(buffer_.data() + old_size, values, bytes);
            }
        }

        rows_ = next_rows;
        valid_parent_rows_ =
            CheckedAdd(valid_parent_rows_, valid_parents, "valid parent row");
        physical_vectors_ =
            CheckedAdd(physical_vectors_, vectors, "physical vector");
        add_called_ = true;
    } catch (...) {
        failed_ = true;
        throw;
    }
}

template <typename T>
void
VectorMemBuilder<T>::AddEmbeddingBatch(
    size_t logical_rows,
    const EmbeddingListValueView* compact_values,
    size_t compact_count,
    const bool* valid) {
    try {
        EnsureOpen("add embedding-list batch to");
        AssertInfo(engine_.ElemType() != DataType::NONE,
                   "ordinary vector input must use Add");
        const auto next_rows =
            CheckedAdd(rows_, logical_rows, "embedding-list parent row");
        if (expected_rows_.has_value()) {
            AssertInfo(next_rows <= *expected_rows_,
                       "embedding-list input exceeds expected row count {}",
                       *expected_rows_);
        }

        const auto valid_parents = CountValid(logical_rows, valid);
        AssertInfo(compact_count == valid_parents,
                   "embedding-list compact parent count {} disagrees with {} "
                   "valid logical parents",
                   compact_count,
                   valid_parents);
        AssertInfo(compact_values != nullptr || compact_count == 0,
                   "embedding-list compact values are null for {} parents",
                   compact_count);

        const auto row_bytes = vector_bytes_per_element(
            PhysicalVectorDataType<T>(), engine_.Dim());
        AssertInfo(row_bytes > 0,
                   "embedding-list vector row byte size is zero");
        size_t batch_vectors = 0;
        size_t batch_bytes = 0;
        for (size_t i = 0; i < compact_count; ++i) {
            const auto& value = compact_values[i];
            const auto expected_bytes = CheckedBytes(
                value.vector_count, row_bytes, "embedding-list value");
            AssertInfo(value.byte_size == expected_bytes,
                       "embedding-list value {} has {} bytes, expected {}",
                       i,
                       value.byte_size,
                       expected_bytes);
            AssertInfo(value.data != nullptr || value.byte_size == 0,
                       "embedding-list value {} has null data for {} bytes",
                       i,
                       value.byte_size);
            AssertInfo(value.vector_count <=
                           std::numeric_limits<size_t>::max() - batch_vectors,
                       "embedding-list vector count overflows size_t");
            batch_vectors += value.vector_count;
            AssertInfo(value.byte_size <=
                           std::numeric_limits<size_t>::max() - batch_bytes,
                       "embedding-list byte count overflows size_t");
            batch_bytes += value.byte_size;
        }
        const auto next_valid_parents = CheckedAdd(
            valid_parent_rows_, valid_parents, "valid embedding-list parent");
        const auto next_vectors = CheckedAdd(
            physical_vectors_, batch_vectors, "embedding-list vector");
        AssertInfo(
            batch_bytes <= std::numeric_limits<size_t>::max() - buffer_.size(),
            "embedding-list build buffer size overflows size_t");
        const auto initial_offset = emb_list_offsets_.empty() ? size_t{1} : 0;
        AssertInfo(initial_offset <=
                           std::numeric_limits<size_t>::max() - valid_parents &&
                       valid_parents + initial_offset <=
                           std::numeric_limits<size_t>::max() -
                               emb_list_offsets_.size(),
                   "embedding-list offset count overflows size_t");

        AppendValidity(logical_rows, valid, next_rows);
        const auto old_bytes = buffer_.size();
        buffer_.resize(old_bytes + batch_bytes);
        if (emb_list_offsets_.empty()) {
            emb_list_offsets_.push_back(0);
        }
        size_t byte_offset = old_bytes;
        size_t vector_offset = emb_list_offsets_.back();
        for (size_t i = 0; i < compact_count; ++i) {
            const auto& value = compact_values[i];
            if (value.byte_size != 0) {
                std::memcpy(
                    buffer_.data() + byte_offset, value.data, value.byte_size);
            }
            byte_offset += value.byte_size;
            vector_offset += value.vector_count;
            emb_list_offsets_.push_back(vector_offset);
        }
        AssertInfo(byte_offset == buffer_.size() &&
                       vector_offset == static_cast<size_t>(next_vectors),
                   "embedding-list batch accounting changed during append");

        rows_ = next_rows;
        valid_parent_rows_ = next_valid_parents;
        physical_vectors_ = next_vectors;
        add_called_ = true;
    } catch (...) {
        failed_ = true;
        throw;
    }
}

template <typename T>
void
VectorMemBuilder<T>::SetSideInput(
    std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>>
        scalar_info) {
    try {
        EnsureOpen("set side input on");
        AssertInfo(!side_input_set_,
                   "memory vector side input was already set");
        scalar_info_ = std::move(scalar_info);
        side_input_set_ = true;
    } catch (...) {
        failed_ = true;
        throw;
    }
}

template <typename T>
storage::ArtifactPtr
VectorMemBuilder<T>::Seal() && {
    EnsureOpen("seal");
    sealed_ = true;
    try {
        if (rows_ == 0) {
            ThrowInfo(DataIsEmpty, "cannot build an empty memory vector index");
        }
        AssertInfo(add_called_, "memory vector build has no input batch");
        if (expected_rows_.has_value()) {
            AssertInfo(rows_ == *expected_rows_,
                       "memory vector input row count {} disagrees with "
                       "expected {}",
                       rows_,
                       *expected_rows_);
        }
        const auto required_side_inputs = InputSpec().side_inputs;
        AssertInfo(required_side_inputs.empty() || side_input_set_,
                   "memory vector build requires declared side input delivery");

        if (saw_validity_) {
            auto options = GetOffsetMappingMmapOptions(build_params_);
            if (options.enable_mmap_i2o_map || options.enable_mmap_o2i_map) {
                const auto local_dir =
                    GetValueFromConfig<std::string>(build_params_, "local_dir");
                AssertInfo(local_dir.has_value() && !local_dir->empty(),
                           "nullable memory vector mmap requires local_dir");
                options.mmap_dir_path = *local_dir;
            }
            valid_data_.Build(validity_.get(), rows_, options);
            AssertInfo(valid_data_.ValidCount() == valid_parent_rows_,
                       "memory vector validity count changed during seal");
        } else {
            AssertInfo(valid_parent_rows_ == rows_,
                       "non-nullable memory vector has compacted rows");
        }

        const bool all_null = saw_validity_ && valid_parent_rows_ == 0;
        const bool embedding_list = engine_.ElemType() != DataType::NONE;
        const bool empty_embedding_list =
            embedding_list && !all_null && physical_vectors_ == 0;

        if (all_null) {
            AssertInfo(physical_vectors_ == 0 && buffer_.empty() &&
                           sparse_rows_.empty(),
                       "all-null memory vector build retained physical data");
            emb_list_offsets_ = {};
            auto artifact = std::make_unique<VectorMemArtifact<T>>(
                std::move(engine_), std::move(valid_data_));
            ReleaseStaging();
            return artifact;
        }

        if (empty_embedding_list) {
            AssertInfo(!emb_list_offsets_.empty(),
                       "empty embedding-list build has no offsets");
            AssertInfo(emb_list_offsets_.back() == 0,
                       "empty embedding-list offsets contain vectors");
            auto artifact = std::make_unique<VectorMemArtifact<T>>(
                std::move(engine_),
                std::move(valid_data_),
                std::move(emb_list_offsets_));
            ReleaseStaging();
            return artifact;
        }

        AssertInfo(physical_vectors_ > 0,
                   "non-empty memory vector build has no physical vectors");
        const void* data = nullptr;
        if constexpr (std::is_same_v<T, sparse_u32_f32>) {
            AssertInfo(
                static_cast<uint64_t>(physical_vectors_) == sparse_rows_.size(),
                "sparse vector count disagrees with owned rows");
            engine_.SetDim(std::max(engine_.Dim(), sparse_dim_));
            data = sparse_rows_.data();
        } else {
            const auto expected_bytes =
                CheckedBytes(static_cast<size_t>(physical_vectors_),
                             vector_bytes_per_element(
                                 PhysicalVectorDataType<T>(), engine_.Dim()),
                             "memory vector payload");
            AssertInfo(buffer_.size() == expected_bytes,
                       "memory vector byte count {} disagrees with expected "
                       "{}",
                       buffer_.size(),
                       expected_bytes);
            data = buffer_.data();
        }

        auto dataset =
            knowhere::GenDataSet(physical_vectors_, engine_.Dim(), data);
        if constexpr (std::is_same_v<T, sparse_u32_f32>) {
            dataset->SetIsSparse(true);
        }
        if (!scalar_info_.empty()) {
            dataset->Set(knowhere::meta::SCALAR_INFO, std::move(scalar_info_));
        }
        if (embedding_list) {
            AssertInfo(!emb_list_offsets_.empty() &&
                           emb_list_offsets_.back() ==
                               static_cast<size_t>(physical_vectors_),
                       "embedding-list offsets disagree with vector count");
            dataset->Set(knowhere::meta::EMB_LIST_OFFSET,
                         const_cast<const size_t*>(emb_list_offsets_.data()));
        }

        auto config = build_params_;
        config.erase(INSERT_FILES_KEY);
        config.erase(VEC_OPT_FIELDS);
        config[EMB_LIST] = embedding_list;
        const auto status =
            engine_.Raw().Build(dataset, config, engine_.UseBuildPool());
        if (status != knowhere::Status::success) {
            ThrowInfo(knowhere::ToSegcoreErrorCode(status),
                      "failed to build memory vector index: status {} ({})",
                      static_cast<int>(status),
                      knowhere::Status2String(status));
        }
        engine_.SetDim(engine_.Raw().Dim());
        emb_list_offsets_ = {};
        auto artifact = std::make_unique<VectorMemArtifact<T>>(
            std::move(engine_), std::move(valid_data_));
        ReleaseStaging();
        return artifact;
    } catch (...) {
        failed_ = true;
        throw;
    }
}

template class VectorMemBuilder<float>;
template class VectorMemBuilder<bin1>;
template class VectorMemBuilder<float16>;
template class VectorMemBuilder<bfloat16>;
template class VectorMemBuilder<int8>;
template class VectorMemBuilder<sparse_u32_f32>;

}  // namespace milvus::index
