// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "indexbuilder/VectorBuildDriver.h"

#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/VectorArray.h"
#include "index/Families.h"
#include "index/Utils.h"
#include "index/vector/VectorDiskBuilder.h"
#include "index/vector/VectorMemBuilder.h"

namespace milvus::indexbuilder {
namespace {

class VectorSideInputDriver {
 public:
    virtual ~VectorSideInputDriver() = default;

    virtual const VectorPrimaryLayout&
    PrimaryLayout() const = 0;

    virtual void
    SetScalarInfo(VectorScalarInfo scalar_info) = 0;
};

class VectorDiskInputDriver {
 public:
    virtual ~VectorDiskInputDriver() = default;

    virtual void
    SetInputs(VectorDiskBuildInputs inputs) = 0;
};

size_t
CheckedSize(int64_t value, const char* label) {
    AssertInfo(value >= 0, "{} is negative: {}", label, value);
    AssertInfo(static_cast<uint64_t>(value) <=
                   static_cast<uint64_t>(std::numeric_limits<size_t>::max()),
               "{} exceeds size_t: {}",
               label,
               value);
    return static_cast<size_t>(value);
}

template <typename T>
std::unique_ptr<index::IndexBuilder<T>>
CreateVectorBuilder(const index::IndexFamily& family,
                    const index::BuildParams& params,
                    DataType value_type) {
    auto builder = index::BuilderRegistry<T>::Instance().Create(family, params);
    if (builder == nullptr) {
        ThrowInfo(Unsupported,
                  "index family {} has no vector builder for value type {}",
                  family,
                  static_cast<int>(value_type));
    }
    return builder;
}

template <typename T>
class VectorValueBuildDriver final : public TypedBuildDriver<T>,
                                     public VectorSideInputDriver {
 public:
    VectorValueBuildDriver(std::unique_ptr<index::IndexBuilder<T>> builder,
                           index::VectorMemBuilder<T>* mem_builder,
                           DataType source_type,
                           int64_t expected_dim)
        : TypedBuildDriver<T>(std::move(builder), source_type),
          mem_builder_(mem_builder),
          source_type_(source_type),
          expected_dim_(expected_dim) {
        AssertInfo(mem_builder_ != nullptr,
                   "vector_mem driver cannot own an incompatible builder");
        if (!this->InputSpec().side_inputs.empty()) {
            primary_layout_.emplace();
        }
    }

    FeedControl
    Feed(const FieldDataPtr& batch) override {
        try {
            this->ValidateFeed(batch);
            const auto logical_rows = batch->Length();
            const auto physical_rows =
                CheckedSize(batch->get_valid_rows(), "valid vector row count");
            AssertInfo(physical_rows <= logical_rows,
                       "valid vector row count {} exceeds logical count {}",
                       physical_rows,
                       logical_rows);
            AssertInfo(batch->Data() != nullptr || physical_rows == 0,
                       "vector field-data has null physical data for {} rows",
                       physical_rows);
            const auto* valid = this->UnpackValidity(batch);
            size_t bitmap_valid_rows = logical_rows;
            if (valid != nullptr) {
                bitmap_valid_rows = 0;
                for (size_t i = 0; i < logical_rows; ++i) {
                    bitmap_valid_rows += valid[i] ? 1 : 0;
                }
            }
            AssertInfo(bitmap_valid_rows == physical_rows,
                       "vector validity has {} rows, field-data has {} "
                       "physical rows",
                       bitmap_valid_rows,
                       physical_rows);

            if constexpr (std::is_same_v<T, sparse_u32_f32>) {
                auto* sparse =
                    dynamic_cast<FieldData<SparseFloatVector>*>(batch.get());
                AssertInfo(sparse != nullptr,
                           "sparse vector field-data has the wrong layout");
                AssertInfo(sparse->Dim() >= 0,
                           "sparse vector field-data has negative dimension");
            } else {
                AssertInfo(batch->get_dim() == expected_dim_,
                           "vector field-data dimension {} disagrees with {}",
                           batch->get_dim(),
                           expected_dim_);
                const auto row_bytes =
                    vector_bytes_per_element(source_type_, expected_dim_);
                AssertInfo(physical_rows == 0 ||
                               row_bytes <= std::numeric_limits<size_t>::max() /
                                                physical_rows,
                           "vector field-data byte size overflows size_t");
                const auto expected_bytes = physical_rows * row_bytes;
                AssertInfo(batch->DataSize() >= 0 &&
                               static_cast<uint64_t>(batch->DataSize()) ==
                                   expected_bytes,
                           "vector field-data has {} bytes, expected {}",
                           batch->DataSize(),
                           expected_bytes);
            }
            if (primary_layout_.has_value()) {
                primary_layout_->Append(logical_rows, valid);
            }
            return this->AddProjected(
                logical_rows, static_cast<const T*>(batch->Data()), valid);
        } catch (...) {
            this->MarkFailed();
            throw;
        }
    }

    const VectorPrimaryLayout&
    PrimaryLayout() const override {
        this->AssertOpen("read primary layout from");
        AssertInfo(primary_layout_.has_value(),
                   "vector driver did not declare side inputs");
        return *primary_layout_;
    }

    void
    SetScalarInfo(VectorScalarInfo scalar_info) override {
        this->AssertOpen("deliver scalar info to");
        AssertInfo(primary_layout_.has_value() && mem_builder_ != nullptr,
                   "vector driver cannot accept scalar info");
        try {
            mem_builder_->SetSideInput(std::move(scalar_info));
        } catch (...) {
            this->MarkFailed();
            throw;
        }
    }

 private:
    // Borrowed from TypedBuildDriver's builder_. It is dereferenced only after
    // the base-class Open-state guard succeeds.
    index::VectorMemBuilder<T>* mem_builder_{nullptr};
    std::optional<VectorPrimaryLayout> primary_layout_;
    DataType source_type_{DataType::NONE};
    int64_t expected_dim_{0};
};

template <typename T>
class VectorDiskValueBuildDriver final : public TypedBuildDriver<T>,
                                         public VectorDiskInputDriver {
 public:
    VectorDiskValueBuildDriver(std::unique_ptr<index::IndexBuilder<T>> builder,
                               index::VectorDiskBuilder<T>* disk_builder,
                               DataType source_type)
        : TypedBuildDriver<T>(std::move(builder), source_type),
          disk_builder_(disk_builder) {
        AssertInfo(disk_builder_ != nullptr,
                   "vector_disk driver cannot own an incompatible builder");
    }

    void
    SetInputs(VectorDiskBuildInputs inputs) override {
        this->AssertOpen("deliver materialized inputs to");
        AssertInfo(disk_builder_ != nullptr,
                   "vector_disk driver has no concrete builder");
        try {
            disk_builder_->SetMaterializedInputs(
                std::move(inputs.owner),
                std::move(inputs.raw_path),
                std::move(inputs.valid_path),
                std::move(inputs.offsets_path),
                std::move(inputs.scalar_info_path));
            this->MarkSourceSet();
        } catch (...) {
            this->MarkFailed();
            throw;
        }
    }

 private:
    // Borrowed from TypedBuildDriver's builder_ and accessed only after the
    // base Open-state guard succeeds.
    index::VectorDiskBuilder<T>* disk_builder_{nullptr};
};

template <typename T>
class VectorArrayBuildDriver final : public BuildDriver {
 public:
    VectorArrayBuildDriver(std::unique_ptr<index::VectorMemBuilder<T>> builder,
                           DataType element_type,
                           int64_t expected_dim)
        : builder_(std::move(builder)),
          element_type_(element_type),
          expected_dim_(expected_dim) {
        AssertInfo(builder_ != nullptr,
                   "VECTOR_ARRAY build driver cannot own a null builder");
        spec_ = builder_->InputSpec();
    }

    const index::BuilderInputSpec&
    InputSpec() const override {
        return spec_;
    }

    FeedControl
    Feed(const FieldDataPtr& batch) override {
        try {
            AssertOpen("feed");
            AssertInfo(batch != nullptr,
                       "VECTOR_ARRAY build driver received a null batch");
            AssertInfo(batch->get_data_type() == DataType::VECTOR_ARRAY,
                       "VECTOR_ARRAY build driver received field type {}",
                       batch->get_data_type());
            auto* arrays = dynamic_cast<FieldData<VectorArray>*>(batch.get());
            AssertInfo(arrays != nullptr,
                       "VECTOR_ARRAY field-data has the wrong layout");
            AssertInfo(arrays->get_element_type() == element_type_,
                       "VECTOR_ARRAY element type {} disagrees with {}",
                       arrays->get_element_type(),
                       element_type_);
            AssertInfo(arrays->get_dim() == expected_dim_,
                       "VECTOR_ARRAY dimension {} disagrees with {}",
                       arrays->get_dim(),
                       expected_dim_);

            const auto logical_rows = batch->Length();
            const auto compact_count = CheckedSize(
                arrays->get_valid_rows(), "valid VECTOR_ARRAY parent count");
            AssertInfo(compact_count <= logical_rows,
                       "valid VECTOR_ARRAY parent count {} exceeds {}",
                       compact_count,
                       logical_rows);

            const bool nullable = batch->IsNullable();
            const auto* packed_validity =
                nullable ? batch->ValidData() : nullptr;
            AssertInfo(
                !nullable || logical_rows == 0 || packed_validity != nullptr,
                "nullable VECTOR_ARRAY batch has no validity bitmap");
            if (nullable && logical_rows > validity_capacity_) {
                validity_ = std::make_unique<bool[]>(logical_rows);
                validity_capacity_ = logical_rows;
            }
            size_t counted_valid = 0;
            for (size_t i = 0; i < logical_rows; ++i) {
                const bool row_valid =
                    !nullable ||
                    ((packed_validity[i >> 3] >> static_cast<unsigned>(i & 7)) &
                     1U) != 0;
                if (nullable) {
                    validity_[i] = row_valid;
                }
                counted_valid += row_valid ? 1 : 0;
            }
            AssertInfo(counted_valid == compact_count,
                       "VECTOR_ARRAY validity has {} parents, field-data has "
                       "{}",
                       counted_valid,
                       compact_count);

            const auto* compact_values =
                static_cast<const VectorArray*>(batch->Data());
            AssertInfo(compact_values != nullptr || compact_count == 0,
                       "VECTOR_ARRAY batch has null compact values");
            views_.resize(compact_count);
            size_t total_bytes = 0;
            for (size_t i = 0; i < compact_count; ++i) {
                const auto& value = compact_values[i];
                AssertInfo(value.get_element_type() == element_type_,
                           "VECTOR_ARRAY value {} has element type {}, "
                           "expected {}",
                           i,
                           value.get_element_type(),
                           element_type_);
                AssertInfo(value.dim() == expected_dim_,
                           "VECTOR_ARRAY value {} has dimension {}, expected "
                           "{}",
                           i,
                           value.dim(),
                           expected_dim_);
                AssertInfo(value.length() >= 0,
                           "VECTOR_ARRAY value {} has negative length",
                           i);
                AssertInfo(value.byte_size() <=
                               std::numeric_limits<size_t>::max() - total_bytes,
                           "VECTOR_ARRAY batch byte size overflows size_t");
                total_bytes += value.byte_size();
                views_[i] = {
                    .data = value.data(),
                    .byte_size = value.byte_size(),
                    .vector_count = static_cast<size_t>(value.length())};
            }
            AssertInfo(
                batch->DataSize() >= 0 &&
                    static_cast<uint64_t>(batch->DataSize()) == total_bytes,
                "VECTOR_ARRAY field-data has {} bytes, expected {}",
                batch->DataSize(),
                total_bytes);

            builder_->AddEmbeddingBatch(
                logical_rows,
                views_.empty() ? nullptr : views_.data(),
                views_.size(),
                nullable ? validity_.get() : nullptr);
            return builder_->CurrentPassComplete() ? FeedControl::PassComplete
                                                   : FeedControl::Continue;
        } catch (...) {
            state_ = State::Failed;
            throw;
        }
    }

    void
    FinishPass() override {
        AssertOpen("finish pass on");
        AssertInfo(spec_.needs_second_pass,
                   "FinishPass called on one-pass VECTOR_ARRAY builder");
    }

    void
    SetSourceFile(const std::string&) override {
        AssertOpen("set source file on");
        state_ = State::Failed;
        ThrowInfo(UnexpectedError,
                  "memory VECTOR_ARRAY builder does not accept a source file");
    }

    storage::ArtifactPtr
        Seal() &&
        override {
        AssertOpen("seal");
        state_ = State::Consumed;
        auto builder = std::move(builder_);
        return std::move(*builder).Seal();
    }

 private:
    enum class State { Open, Failed, Consumed };

    void
    AssertOpen(const char* operation) const {
        AssertInfo(state_ == State::Open,
                   "cannot {} a {} VECTOR_ARRAY build driver",
                   operation,
                   state_ == State::Failed ? "failed" : "consumed");
    }

    std::unique_ptr<index::VectorMemBuilder<T>> builder_;
    index::BuilderInputSpec spec_;
    DataType element_type_{DataType::NONE};
    int64_t expected_dim_{0};
    std::vector<index::EmbeddingListValueView> views_;
    std::unique_ptr<bool[]> validity_;
    size_t validity_capacity_{0};
    State state_{State::Open};
};

template <typename T>
BuildDriverPtr
MakeTypedVectorDriver(DataType field_type,
                      DataType value_type,
                      const index::IndexFamily& family,
                      const index::BuildParams& params,
                      int64_t dim) {
    auto builder = CreateVectorBuilder<T>(family, params, value_type);
    if (family == index::families::kVectorDisk) {
        auto* concrete =
            dynamic_cast<index::VectorDiskBuilder<T>*>(builder.get());
        return std::make_unique<VectorDiskValueBuildDriver<T>>(
            std::move(builder), concrete, field_type);
    }
    if (field_type != DataType::VECTOR_ARRAY) {
        auto* concrete =
            dynamic_cast<index::VectorMemBuilder<T>*>(builder.get());
        return std::make_unique<VectorValueBuildDriver<T>>(
            std::move(builder), concrete, field_type, dim);
    }

    if (!builder->InputSpec().side_inputs.empty()) {
        ThrowInfo(Unsupported,
                  "VECTOR_ARRAY optional scalar input is not migrated");
    }

    auto* concrete = dynamic_cast<index::VectorMemBuilder<T>*>(builder.get());
    AssertInfo(concrete != nullptr,
               "vector_mem registry returned an incompatible VECTOR_ARRAY "
               "builder");
    static_cast<void>(builder.release());
    return std::make_unique<VectorArrayBuildDriver<T>>(
        std::unique_ptr<index::VectorMemBuilder<T>>(concrete), value_type, dim);
}

}  // namespace

const VectorPrimaryLayout&
GetVectorPrimaryLayout(const BuildDriver& driver) {
    const auto* side_driver =
        dynamic_cast<const VectorSideInputDriver*>(&driver);
    AssertInfo(side_driver != nullptr,
               "build driver cannot expose vector primary layout");
    return side_driver->PrimaryLayout();
}

void
DeliverVectorScalarInfo(BuildDriver& driver, VectorScalarInfo scalar_info) {
    auto* side_driver = dynamic_cast<VectorSideInputDriver*>(&driver);
    AssertInfo(side_driver != nullptr,
               "build driver cannot accept vector scalar info");
    side_driver->SetScalarInfo(std::move(scalar_info));
}

void
DeliverVectorDiskInputs(BuildDriver& driver, VectorDiskBuildInputs inputs) {
    auto* disk_driver = dynamic_cast<VectorDiskInputDriver*>(&driver);
    AssertInfo(disk_driver != nullptr,
               "build driver cannot accept vector disk inputs");
    disk_driver->SetInputs(std::move(inputs));
}

void
ValidateVectorDiskInputDriver(const BuildDriver& driver) {
    AssertInfo(dynamic_cast<const VectorDiskInputDriver*>(&driver) != nullptr,
               "build driver cannot accept vector disk inputs");
}

BuildDriverPtr
MakeVectorBuildDriver(DataType field_type,
                      DataType value_type,
                      const index::IndexFamily& family,
                      const index::BuildParams& params) {
    AssertInfo(IsVectorDataType(field_type),
               "vector build driver received non-vector field type {}",
               field_type);
    AssertInfo(family == index::families::kVectorMem ||
                   family == index::families::kVectorDisk,
               "vector build resolved to unexpected family {}",
               family);
    const auto dim = index::GetValueFromConfig<int64_t>(params, DIM_KEY);
    AssertInfo(dim.has_value(), "vector build driver requires dimension");

    switch (value_type) {
        case DataType::VECTOR_FLOAT:
            return MakeTypedVectorDriver<float>(
                field_type, value_type, family, params, *dim);
        case DataType::VECTOR_BINARY:
            return MakeTypedVectorDriver<bin1>(
                field_type, value_type, family, params, *dim);
        case DataType::VECTOR_FLOAT16:
            return MakeTypedVectorDriver<float16>(
                field_type, value_type, family, params, *dim);
        case DataType::VECTOR_BFLOAT16:
            return MakeTypedVectorDriver<bfloat16>(
                field_type, value_type, family, params, *dim);
        case DataType::VECTOR_INT8:
            return MakeTypedVectorDriver<int8>(
                field_type, value_type, family, params, *dim);
        case DataType::VECTOR_SPARSE_U32_F32:
            AssertInfo(field_type != DataType::VECTOR_ARRAY,
                       "VECTOR_ARRAY does not support sparse elements");
            return MakeTypedVectorDriver<sparse_u32_f32>(
                field_type, value_type, family, params, *dim);
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported vector build value type {}",
                      value_type);
    }
}

}  // namespace milvus::indexbuilder
