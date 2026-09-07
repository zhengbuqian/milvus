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

#include "index/scalar/sort/SortedIndexArtifact.h"

#include <cstring>
#include <limits>
#include <type_traits>
#include <utility>

#include "common/EasyAssert.h"
#include "index/scalar/sort/SortedIndexFormat.h"
#include "index/scalar/sort/SortedIndexReader.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

void
CheckedAdd(size_t& total, size_t value) {
    if (value > std::numeric_limits<size_t>::max() - total) {
        ThrowInfo(DataTypeInvalid,
                  "sorted string serialized size overflows size_t");
    }
    total += value;
}

size_t
CheckedMultiply(size_t left, size_t right) {
    if (right != 0 && left > std::numeric_limits<size_t>::max() / right) {
        ThrowInfo(DataTypeInvalid,
                  "sorted string serialized size multiplication overflows");
    }
    return left * right;
}

uint32_t
ToUint32(size_t value, std::string_view label) {
    if (value > std::numeric_limits<uint32_t>::max()) {
        ThrowInfo(DataTypeInvalid,
                  "sorted string {} {} exceeds uint32 domain",
                  label,
                  value);
    }
    return static_cast<uint32_t>(value);
}

template <typename T>
void
WritePod(std::vector<uint8_t>& output, size_t& offset, const T& value) {
    AssertInfo(offset <= output.size() && output.size() - offset >= sizeof(T),
               "sorted string serializer exceeded allocated buffer");
    std::memcpy(output.data() + offset, &value, sizeof(T));
    offset += sizeof(T);
}

std::vector<uint8_t>
SerializeStrings(const SortedStringIndexReader::Layout& layout) {
    const auto unique_count = ToUint32(layout.UniqueCount(), "unique count");

    size_t total = sizeof(uint32_t);
    CheckedAdd(total, CheckedMultiply(layout.UniqueCount(), sizeof(uint32_t)));
    for (size_t i = 0; i < layout.UniqueCount(); ++i) {
        const auto value = layout.Value(i);
        (void)ToUint32(value.size(), "value length");
        CheckedAdd(total, sizeof(uint32_t));
        CheckedAdd(total, value.size());
    }
    CheckedAdd(total, CheckedMultiply(layout.UniqueCount(), sizeof(uint32_t)));
    for (size_t i = 0; i < layout.UniqueCount(); ++i) {
        const auto posting = layout.Posting(i);
        (void)ToUint32(posting.size, "posting length");
        CheckedAdd(total, sizeof(uint32_t));
        CheckedAdd(total, CheckedMultiply(posting.size, sizeof(uint32_t)));
    }
    CheckedAdd(total, sizeof(uint64_t));

    std::vector<uint8_t> output(total);
    size_t offset = 0;
    WritePod(output, offset, unique_count);

    const auto string_offsets_start = offset;
    offset += layout.UniqueCount() * sizeof(uint32_t);
    const auto string_data_start = offset;
    for (size_t i = 0; i < layout.UniqueCount(); ++i) {
        const auto value = layout.Value(i);
        const auto relative =
            ToUint32(offset - string_data_start, "string offset");
        std::memcpy(output.data() + string_offsets_start + i * sizeof(uint32_t),
                    &relative,
                    sizeof(relative));
        const auto length = ToUint32(value.size(), "value length");
        WritePod(output, offset, length);
        if (!value.empty()) {
            std::memcpy(output.data() + offset, value.data(), value.size());
            offset += value.size();
        }
    }

    const auto posting_offsets_start = offset;
    offset += layout.UniqueCount() * sizeof(uint32_t);
    const auto posting_data_start = offset;
    for (size_t i = 0; i < layout.UniqueCount(); ++i) {
        const auto posting = layout.Posting(i);
        const auto relative =
            ToUint32(offset - posting_data_start, "posting offset");
        std::memcpy(
            output.data() + posting_offsets_start + i * sizeof(uint32_t),
            &relative,
            sizeof(relative));
        const auto length = ToUint32(posting.size, "posting length");
        WritePod(output, offset, length);
        for (size_t row = 0; row < posting.size; ++row) {
            WritePod(output, offset, posting.At(row));
        }
    }

    WritePod(output, offset, sort_format::kStringMagic);
    AssertInfo(offset == output.size(),
               "sorted string serialized size mismatch");
    return output;
}

std::vector<uint8_t>
SerializePackedValidity(const TargetBitmap& validity, size_t count) {
    std::vector<uint8_t> output((count + 7) / 8, 0);
    for (size_t i = 0; i < count; ++i) {
        if (validity[i]) {
            output[i / 8] |= static_cast<uint8_t>(1U << (i % 8));
        }
    }
    return output;
}

template <typename T>
typename SortedIndexReader<T>::OpenArgs
MakeNumericState(std::vector<IndexStructure<T>> data,
                 TargetBitmap valid_bitset,
                 std::vector<int32_t> idx_to_offsets,
                 size_t total_num_rows,
                 DataType value_type,
                 bool nested,
                 bool value_lookup) {
    auto data_owner =
        std::make_shared<std::vector<IndexStructure<T>>>(std::move(data));
    auto offsets_owner =
        std::make_shared<std::vector<int32_t>>(std::move(idx_to_offsets));
    typename SortedIndexReader<T>::OpenArgs state;
    state.data = data_owner->data();
    state.size = data_owner->size();
    state.data_owner = data_owner;
    state.data_heap_bytes = data_owner->capacity() * sizeof(IndexStructure<T>);
    state.idx_to_offsets = offsets_owner->data();
    state.idx_to_offsets_size = offsets_owner->size();
    state.idx_to_offsets_owner = offsets_owner;
    state.idx_to_offsets_heap_bytes =
        offsets_owner->capacity() * sizeof(int32_t);
    state.valid_bitset =
        std::make_shared<const TargetBitmap>(std::move(valid_bitset));
    state.total_num_rows = total_num_rows;
    state.value_type = value_type;
    state.nested = nested;
    state.value_lookup = value_lookup;
    return state;
}

SortedStringIndexReader::OpenArgs
MakeStringState(std::vector<std::string> unique_values,
                std::vector<std::vector<uint32_t>> posting_lists,
                TargetBitmap valid_bitset,
                std::vector<int32_t> idx_to_offsets,
                size_t total_num_rows,
                DataType value_type,
                bool nested,
                bool value_lookup) {
    auto offsets_owner =
        std::make_shared<std::vector<int32_t>>(std::move(idx_to_offsets));
    SortedStringIndexReader::OpenArgs state;
    state.layout = SortedStringIndexReader::Layout::FromHeap(
        std::move(unique_values), std::move(posting_lists), total_num_rows);
    state.valid_bitset =
        std::make_shared<const TargetBitmap>(std::move(valid_bitset));
    state.idx_to_offsets = offsets_owner->data();
    state.idx_to_offsets_size = offsets_owner->size();
    state.idx_to_offsets_owner = offsets_owner;
    state.idx_to_offsets_heap_bytes =
        offsets_owner->capacity() * sizeof(int32_t);
    state.total_num_rows = total_num_rows;
    state.value_type = value_type;
    state.nested = nested;
    state.value_lookup = value_lookup;
    return state;
}

}  // namespace

template <typename T>
SortedIndexArtifact<T>::SortedIndexArtifact(std::vector<IndexStructure<T>> data,
                                            TargetBitmap valid_bitset,
                                            std::vector<int32_t> idx_to_offsets,
                                            size_t total_num_rows,
                                            DataType value_type,
                                            bool nested,
                                            bool value_lookup)
    : SortedIndexArtifact(MakeNumericState(std::move(data),
                                           std::move(valid_bitset),
                                           std::move(idx_to_offsets),
                                           total_num_rows,
                                           value_type,
                                           nested,
                                           value_lookup)) {
}

template <typename T>
SortedIndexArtifact<T>::SortedIndexArtifact(
    typename SortedIndexReader<T>::OpenArgs state)
    : state_(std::move(state)) {
    static_assert(std::is_trivially_copyable_v<IndexStructure<T>>);
    AssertInfo(state_.data_owner != nullptr,
               "sorted artifact data owner is null");
    AssertInfo(state_.idx_to_offsets_owner != nullptr,
               "sorted artifact offset owner is null");
    AssertInfo(state_.valid_bitset != nullptr,
               "sorted artifact validity owner is null");
}

template <typename T>
SortedIndexArtifact<T>::~SortedIndexArtifact() = default;

template <typename T>
std::shared_ptr<storage::LoadedArtifact>
SortedIndexArtifact<T>::OpenReader() const {
    return std::make_shared<SortedIndexReader<T>>(state_);
}

template <typename T>
void
SortedIndexArtifact<T>::Serialize(storage::FileSink& sink) const {
    const auto data_bytes = state_.size * sizeof(IndexStructure<T>);
    if (sink.Gen() == storage::Generation::V1V2) {
        const auto index_length = state_.size;
        sink.WriteEntry(sort_format::kIndexData, state_.data, data_bytes);
        sink.WriteEntry(
            sort_format::kIndexLength, &index_length, sizeof(index_length));
        sink.WriteEntry(sort_format::kLegacyNumRows,
                        &state_.total_num_rows,
                        sizeof(state_.total_num_rows));
        sink.WriteEntry(
            sort_format::kLegacyNested, &state_.nested, sizeof(state_.nested));
        return;
    }

    sink.PutMeta(sort_format::kIndexLength, nlohmann::json(state_.size));
    sink.PutMeta(sort_format::kNumRows, nlohmann::json(state_.total_num_rows));
    sink.PutMeta(sort_format::kNested, nlohmann::json(state_.nested));
    sink.WriteEntry(sort_format::kIndexData, state_.data, data_bytes);
    sink.WriteEntry(sort_format::kIdxToOffsets,
                    state_.idx_to_offsets,
                    state_.idx_to_offsets_size * sizeof(int32_t));
    sink.WriteEntry(sort_format::kValidBitset,
                    state_.valid_bitset->data(),
                    state_.valid_bitset->size_in_bytes());
}

SortedStringIndexArtifact::SortedStringIndexArtifact(
    std::vector<std::string> unique_values,
    std::vector<std::vector<uint32_t>> posting_lists,
    TargetBitmap valid_bitset,
    std::vector<int32_t> idx_to_offsets,
    size_t total_num_rows,
    DataType value_type,
    bool nested,
    bool value_lookup)
    : SortedStringIndexArtifact(MakeStringState(std::move(unique_values),
                                                std::move(posting_lists),
                                                std::move(valid_bitset),
                                                std::move(idx_to_offsets),
                                                total_num_rows,
                                                value_type,
                                                nested,
                                                value_lookup)) {
}

SortedStringIndexArtifact::SortedStringIndexArtifact(
    SortedStringIndexReader::OpenArgs state)
    : state_(std::move(state)) {
    AssertInfo(state_.layout != nullptr,
               "sorted string artifact layout is null");
    AssertInfo(state_.idx_to_offsets_owner != nullptr,
               "sorted string artifact offset owner is null");
    AssertInfo(state_.valid_bitset != nullptr,
               "sorted string artifact validity owner is null");
}

SortedStringIndexArtifact::~SortedStringIndexArtifact() = default;

std::shared_ptr<storage::LoadedArtifact>
SortedStringIndexArtifact::OpenReader() const {
    return std::make_shared<SortedStringIndexReader>(state_);
}

void
SortedStringIndexArtifact::Serialize(storage::FileSink& sink) const {
    const auto packed = SerializeStrings(*state_.layout);
    const auto validity =
        SerializePackedValidity(*state_.valid_bitset, state_.total_num_rows);
    if (sink.Gen() == storage::Generation::V1V2) {
        const auto version = sort_format::kStringVersion;
        sink.WriteEntry(sort_format::kVersion, &version, sizeof(version));
        sink.WriteEntry(sort_format::kIndexData, packed.data(), packed.size());
        sink.WriteEntry(sort_format::kLegacyNumRows,
                        &state_.total_num_rows,
                        sizeof(state_.total_num_rows));
        sink.WriteEntry(
            sort_format::kValidBitset, validity.data(), validity.size());
        sink.WriteEntry(
            sort_format::kLegacyNested, &state_.nested, sizeof(state_.nested));
        return;
    }

    sink.PutMeta(sort_format::kVersion,
                 nlohmann::json(sort_format::kStringVersion));
    sink.PutMeta(sort_format::kNumRows, nlohmann::json(state_.total_num_rows));
    sink.PutMeta(sort_format::kNested, nlohmann::json(state_.nested));
    sink.WriteEntry(sort_format::kIndexData, packed.data(), packed.size());
    sink.WriteEntry(
        sort_format::kValidBitset, validity.data(), validity.size());
    sink.WriteEntry(sort_format::kIdxToOffsets,
                    state_.idx_to_offsets,
                    state_.idx_to_offsets_size * sizeof(int32_t));
}

#define INSTANTIATE_SORTED_ARTIFACT(T) template class SortedIndexArtifact<T>;
INSTANTIATE_SORTED_ARTIFACT(bool)
INSTANTIATE_SORTED_ARTIFACT(int8_t)
INSTANTIATE_SORTED_ARTIFACT(int16_t)
INSTANTIATE_SORTED_ARTIFACT(int32_t)
INSTANTIATE_SORTED_ARTIFACT(int64_t)
INSTANTIATE_SORTED_ARTIFACT(float)
INSTANTIATE_SORTED_ARTIFACT(double)
#undef INSTANTIATE_SORTED_ARTIFACT

}  // namespace milvus::index
