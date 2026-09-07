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

#include "indexbuilder/VectorBuildMaterializer.h"

#include <limits>
#include <string>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus::indexbuilder {

bool
VectorPrimaryLayout::IsValid(size_t logical_row) const {
    AssertInfo(logical_row < logical_rows_,
               "vector primary layout row {} is out of range {}",
               logical_row,
               logical_rows_);
    return ((validity_[logical_row >> 3] >>
             static_cast<unsigned>(logical_row & 7)) &
            1U) != 0;
}

bool
VectorPrimaryLayout::Cursor::Done() const {
    AssertInfo(layout_ != nullptr, "vector primary layout cursor is unbound");
    const bool done = logical_row_ == layout_->logical_rows_;
    if (done) {
        AssertInfo(physical_row_ == layout_->physical_rows_,
                   "vector primary layout cursor consumed {} of {} physical "
                   "rows",
                   physical_row_,
                   layout_->physical_rows_);
    }
    return done;
}

VectorPrimaryLayout::Row
VectorPrimaryLayout::Cursor::Next() {
    AssertInfo(!Done(), "vector primary layout cursor is exhausted");
    const bool valid = layout_->IsValid(logical_row_++);
    if (!valid) {
        return {.valid = false, .physical_id = 0};
    }
    AssertInfo(physical_row_ < layout_->physical_rows_,
               "vector primary layout physical cursor exceeds {}",
               layout_->physical_rows_);
    return {.valid = true, .physical_id = physical_row_++};
}

void
VectorPrimaryLayout::Append(size_t logical_rows, const bool* valid) {
    AssertInfo(
        logical_rows <= std::numeric_limits<size_t>::max() - logical_rows_,
        "vector primary logical row count overflows size_t");
    const auto next_rows = logical_rows_ + logical_rows;
    AssertInfo(next_rows <= std::numeric_limits<size_t>::max() - 7,
               "vector primary validity byte count overflows size_t");
    validity_.resize((next_rows + 7) / 8, 0);
    size_t added_physical = 0;
    for (size_t row = 0; row < logical_rows; ++row) {
        if (valid == nullptr || valid[row]) {
            const auto bit = logical_rows_ + row;
            validity_[bit >> 3] |=
                static_cast<uint8_t>(1U << static_cast<unsigned>(bit & 7));
            ++added_physical;
        }
    }
    AssertInfo(
        added_physical <= std::numeric_limits<size_t>::max() - physical_rows_,
        "vector primary physical row count overflows size_t");
    logical_rows_ = next_rows;
    physical_rows_ += added_physical;
}

bool
IsSupportedVectorScalarInfoType(DataType type) {
    switch (type) {
        case DataType::BOOL:
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
        case DataType::FLOAT:
        case DataType::DOUBLE:
        case DataType::STRING:
        case DataType::VARCHAR:
            return true;
        default:
            return false;
    }
}

struct VectorScalarInfoAccumulator::Impl {
    virtual ~Impl() = default;

    virtual void
    Add(const FieldDataPtr& batch) = 0;

    virtual VectorScalarInfo
    Finish() = 0;
};

namespace {

bool
CompatibleScalarType(DataType actual, DataType expected) {
    return actual == expected ||
           ((actual == DataType::STRING || actual == DataType::VARCHAR) &&
            (expected == DataType::STRING || expected == DataType::VARCHAR)) ||
           ((actual == DataType::INT64 || actual == DataType::TIMESTAMPTZ) &&
            (expected == DataType::INT64 || expected == DataType::TIMESTAMPTZ));
}

template <typename T>
class TypedScalarInfoAccumulator final
    : public VectorScalarInfoAccumulator::Impl {
 public:
    TypedScalarInfoAccumulator(FieldId field_id,
                               DataType field_type,
                               const VectorPrimaryLayout& layout)
        : field_id_(field_id),
          field_type_(field_type),
          layout_(layout),
          cursor_(layout.NewCursor()) {
    }

    void
    Add(const FieldDataPtr& batch) override {
        AssertInfo(!finished_,
                   "cannot add to a finished vector scalar-info accumulator");
        if (batch == nullptr) {
            ThrowInfo(DataFormatBroken,
                      "optional scalar source produced a null field-data "
                      "batch");
        }
        if (!CompatibleScalarType(batch->get_data_type(), field_type_)) {
            ThrowInfo(DataFormatBroken,
                      "optional scalar field {} produced type {}, expected {}",
                      field_id_.get(),
                      batch->get_data_type(),
                      field_type_);
        }

        auto* typed_batch = dynamic_cast<FieldData<T>*>(batch.get());
        AssertInfo(typed_batch != nullptr,
                   "optional scalar field {} has an incompatible field-data "
                   "layout",
                   field_id_.get());

        const auto rows = batch->Length();
        if (rows_seen_ > layout_.LogicalRows() ||
            rows > layout_.LogicalRows() - rows_seen_) {
            ThrowInfo(DataFormatBroken,
                      "optional scalar field {} exceeds primary row count {}",
                      field_id_.get(),
                      layout_.LogicalRows());
        }
        const auto* values = static_cast<const T*>(typed_batch->Data());
        AssertInfo(values != nullptr || rows == 0,
                   "optional scalar field {} has null data for {} rows",
                   field_id_.get(),
                   rows);
        const bool nullable = batch->IsNullable();
        const auto* valid = nullable ? batch->ValidData() : nullptr;
        AssertInfo(!nullable || rows == 0 || valid != nullptr,
                   "nullable optional scalar field {} has no validity bitmap",
                   field_id_.get());
        for (size_t row = 0; row < rows; ++row) {
            if (nullable &&
                ((valid[row >> 3] >> static_cast<unsigned>(row & 7)) & 1U) ==
                    0) {
                ThrowInfo(DataFormatBroken,
                          "optional scalar field {} contains NULL at row {}",
                          field_id_.get(),
                          rows_seen_ + row);
            }
            const auto primary = cursor_.Next();
            if (!primary.valid) {
                continue;
            }
            if (primary.physical_id > std::numeric_limits<uint32_t>::max()) {
                ThrowInfo(Unsupported,
                          "vector scalar-info physical row {} exceeds uint32",
                          primary.physical_id);
            }
            categories_[values[row]].push_back(
                static_cast<uint32_t>(primary.physical_id));
        }
        rows_seen_ += rows;
    }

    VectorScalarInfo
    Finish() override {
        AssertInfo(!finished_,
                   "vector scalar-info accumulator was already finished");
        finished_ = true;
        if (rows_seen_ != layout_.LogicalRows()) {
            ThrowInfo(DataFormatBroken,
                      "optional scalar field {} produced {} rows, expected {}",
                      field_id_.get(),
                      rows_seen_,
                      layout_.LogicalRows());
        }
        AssertInfo(cursor_.Done(),
                   "vector scalar-info cursor did not consume its layout");
        if (categories_.size() <= 1) {
            VectorScalarInfo result;
            result.emplace(field_id_.get(),
                           std::vector<std::vector<uint32_t>>{});
            return result;
        }
        if (categories_.size() >
            static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
            ThrowInfo(Unsupported,
                      "vector scalar-info category count exceeds uint32");
        }
        std::vector<std::vector<uint32_t>> groups;
        groups.reserve(categories_.size());
        for (auto& [_, rows] : categories_) {
            groups.push_back(std::move(rows));
        }
        VectorScalarInfo result;
        result.emplace(field_id_.get(), std::move(groups));
        return result;
    }

 private:
    FieldId field_id_;
    DataType field_type_{DataType::NONE};
    const VectorPrimaryLayout& layout_;
    VectorPrimaryLayout::Cursor cursor_;
    std::unordered_map<T, std::vector<uint32_t>> categories_;
    size_t rows_seen_{0};
    bool finished_{false};
};

template <typename T>
std::unique_ptr<VectorScalarInfoAccumulator::Impl>
MakeAccumulator(FieldId field_id,
                DataType field_type,
                const VectorPrimaryLayout& layout) {
    return std::make_unique<TypedScalarInfoAccumulator<T>>(
        field_id, field_type, layout);
}

}  // namespace

VectorScalarInfoAccumulator::VectorScalarInfoAccumulator(
    FieldId field_id, DataType field_type, const VectorPrimaryLayout& layout) {
    switch (field_type) {
        case DataType::BOOL:
            impl_ = MakeAccumulator<bool>(field_id, field_type, layout);
            break;
        case DataType::INT8:
            impl_ = MakeAccumulator<int8_t>(field_id, field_type, layout);
            break;
        case DataType::INT16:
            impl_ = MakeAccumulator<int16_t>(field_id, field_type, layout);
            break;
        case DataType::INT32:
            impl_ = MakeAccumulator<int32_t>(field_id, field_type, layout);
            break;
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            impl_ = MakeAccumulator<int64_t>(field_id, field_type, layout);
            break;
        case DataType::FLOAT:
            impl_ = MakeAccumulator<float>(field_id, field_type, layout);
            break;
        case DataType::DOUBLE:
            impl_ = MakeAccumulator<double>(field_id, field_type, layout);
            break;
        case DataType::STRING:
        case DataType::VARCHAR:
            impl_ = MakeAccumulator<std::string>(field_id, field_type, layout);
            break;
        default:
            ThrowInfo(Unsupported,
                      "optional scalar field {} has unsupported type {}",
                      field_id.get(),
                      field_type);
    }
}

VectorScalarInfoAccumulator::~VectorScalarInfoAccumulator() = default;
VectorScalarInfoAccumulator::VectorScalarInfoAccumulator(
    VectorScalarInfoAccumulator&&) noexcept = default;
VectorScalarInfoAccumulator&
VectorScalarInfoAccumulator::operator=(VectorScalarInfoAccumulator&&) noexcept =
    default;

void
VectorScalarInfoAccumulator::Add(const FieldDataPtr& batch) {
    AssertInfo(impl_ != nullptr,
               "vector scalar-info accumulator has no implementation");
    impl_->Add(batch);
}

VectorScalarInfo
VectorScalarInfoAccumulator::Finish() && {
    AssertInfo(impl_ != nullptr,
               "vector scalar-info accumulator has no implementation");
    return impl_->Finish();
}

}  // namespace milvus::indexbuilder
