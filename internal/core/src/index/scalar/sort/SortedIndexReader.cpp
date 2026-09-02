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

#include "index/scalar/sort/SortedIndexReader.h"

#include <utility>

namespace milvus::index {

template <typename T>
SortedIndexReader<T>::SortedIndexReader(OpenArgs args)
    : data_(std::move(args)) {
}

template <typename T>
SortedIndexReader<T>::~SortedIndexReader() = default;

template <typename T>
ReaderCaps
SortedIndexReader<T>::Caps() const {
    // `value_lookup` false for nested and for ARRAY fields — today's
    // `HasRawData()` is `!is_nested_index_ && !is_array_field_`
    // (ScalarIndexSort.h:167-170). Reverse lookup is O(1) through
    // `idx_to_offsets`, hence `cheap_value_lookup`.
    return ReaderCaps{.predicate = true,
                      .nested = data_.nested,
                      .value_lookup = !data_.nested,
                      .cheap_value_lookup = true};
}

template <typename T>
Domain
SortedIndexReader<T>::CoordDomain() const {
    return data_.nested ? Domain::Element : Domain::Row;
}

template <typename T>
int64_t
SortedIndexReader<T>::Count() const {
    return static_cast<int64_t>(data_.total_num_rows);
}

template <typename T>
DataType
SortedIndexReader<T>::ValueType() const {
    // TODO: derive from T.
}

template <typename T>
int64_t
SortedIndexReader<T>::MemoryUsage() const {
    // TODO: move existing logic here (see ScalarIndexSort.h:138-162).
    //
    // NAMING NOTE while moving: today's `Size()` (ScalarIndexSort.h:128-131)
    // returns the ELEMENT COUNT of the sorted array, `BitmapIndex::Size()`
    // returns the ROW COUNT, and `StringIndexSort::Size()` / `Marisa::Size()`
    // return a byte count. Three meanings, one name. `Count()` (coordinate
    // cardinality) and `MemoryUsage()` (bytes) split it apart; note that
    // `ScalarIndexSort.cpp:414` and `:890` currently RELY on `Size()` meaning
    // element count, so those call sites must move to the internal accessor.
}

template <typename T>
ResourceUsage
SortedIndexReader<T>::CellByteSize() const {
    // See §12.3.
}

template <typename T>
TargetBitmap
SortedIndexReader<T>::In(size_t n, const T* values) const {
    // TODO: move existing logic here (see ScalarIndexSort.cpp:458-479).
}

template <typename T>
TargetBitmap
SortedIndexReader<T>::NotIn(size_t n, const T* values) const {
    // TODO: move existing logic here (see ScalarIndexSort.cpp:481-504).
}

template <typename T>
TargetBitmap
SortedIndexReader<T>::Range(const T& value, CompareOp op) const {
    // TODO: move existing logic here (see ScalarIndexSort.cpp:525-576,
    // including the "more than half the rows hit" inversion at :556-575).
}

template <typename T>
TargetBitmap
SortedIndexReader<T>::Range(const T& lo, bool lo_inc, const T& hi,
                           bool hi_inc) const {
    // TODO: move existing logic here (see ScalarIndexSort.cpp:578-635).
}

template <typename T>
std::optional<T>
SortedIndexReader<T>::Lookup(int64_t offset) const {
    // TODO: move existing logic here (see ScalarIndexSort.cpp:637-648).
}

template <typename T>
void
SortedIndexReader<T>::Gather(
    const int64_t* offsets,
    int64_t count,
    const std::function<void(int64_t i, const T*, bool valid)>& out) const {
    // NEW (§5.5). Replaces the per-row loop in `ReverseDataFromIndex`
    // (segcore/Utils.h:151); a sorted index can order the offsets first and
    // walk its array once.
}

template <typename T>
TargetBitmap
SortedIndexReader<T>::IsNull() const {
    // TODO: move existing logic here (see ScalarIndexSort.cpp:506-514).
}

template <typename T>
TargetBitmap
SortedIndexReader<T>::IsNotNull() const {
    // TODO: move existing logic here (see ScalarIndexSort.cpp:516-523).
}

template <typename T>
bool
SortedIndexReader<T>::ShouldSkip(const T& lower, const T& upper,
                                 CompareOp op) const {
    // TODO: move existing logic here (see ScalarIndexSort.cpp:650-690).
}

SortedStringIndexReader::SortedStringIndexReader(
    std::unique_ptr<Layout> layout,
    TargetBitmap valid_bitset,
    std::vector<int32_t> idx_to_offsets,
    size_t total_num_rows,
    bool nested)
    : layout_(std::move(layout)),
      valid_bitset_(std::move(valid_bitset)),
      idx_to_offsets_(std::move(idx_to_offsets)),
      total_num_rows_(total_num_rows),
      nested_(nested) {
}

SortedStringIndexReader::~SortedStringIndexReader() = default;

ReaderCaps
SortedStringIndexReader::Caps() const {
    return ReaderCaps{.predicate = true,
                      .pattern_match = true,
                      .nested = nested_,
                      .value_lookup = !nested_,
                      .cheap_value_lookup = true};
}

Domain
SortedStringIndexReader::CoordDomain() const {
    return nested_ ? Domain::Element : Domain::Row;
}

int64_t
SortedStringIndexReader::Count() const {
    return static_cast<int64_t>(total_num_rows_);
}

DataType
SortedStringIndexReader::ValueType() const {
    return DataType::VARCHAR;
}

int64_t
SortedStringIndexReader::MemoryUsage() const {
    // TODO: move existing logic here (see StringIndexSort.cpp:553-574
    // ComputeByteSize and :538-551 CalculateTotalSize; the memory layout's own
    // accounting is at :1362-1390, the mapped one's at :1854-1858).
}

ResourceUsage
SortedStringIndexReader::CellByteSize() const {
    // See §12.3.
}

TargetBitmap
SortedStringIndexReader::In(size_t n, const std::string_view* values) const {
    // TODO: move existing logic here (see StringIndexSort.cpp:1051-1068 memory
    // layout / :1585-1602 mapped layout; the facade at :449-453 only forwarded).
}

TargetBitmap
SortedStringIndexReader::NotIn(size_t n, const std::string_view* values) const {
    // TODO: see StringIndexSort.cpp:1070-1086 / :1604-1619.
}

TargetBitmap
SortedStringIndexReader::Range(const std::string_view& value,
                               CompareOp op) const {
    // TODO: see StringIndexSort.cpp:1101-1150 / :1634-1670.
}

TargetBitmap
SortedStringIndexReader::Range(const std::string_view& lo, bool lo_inc,
                               const std::string_view& hi, bool hi_inc) const {
    // TODO: see StringIndexSort.cpp:1152-1185 / :1672-1693.
}

std::optional<std::string>
SortedStringIndexReader::Lookup(int64_t offset) const {
    // TODO: see StringIndexSort.cpp:1325-1343 / :1825-1847.
}

void
SortedStringIndexReader::Gather(
    const int64_t* offsets,
    int64_t count,
    const std::function<void(int64_t i, const std::string_view*, bool valid)>&
        out) const {
    // NEW (§5.5). A view IS allowed here, unlike in `Lookup`: the layout can
    // keep its bytes alive for the duration of the callback.
}

TargetBitmap
SortedStringIndexReader::PatternMatch(std::string_view pattern,
                                      PatternOp op) const {
    // TODO: move existing logic here (see StringIndexSort.cpp:1266-1323 memory
    // / :1764-1823 mapped, plus PrefixMatch :1187-1203 / :1695-1710 and the
    // shared prefix-range helpers :1205-1242 / :1712-1741).
}

TargetBitmap
SortedStringIndexReader::IsNull() const {
    // TODO: see StringIndexSort.cpp:1088-1094 / :1621-1627.
}

TargetBitmap
SortedStringIndexReader::IsNotNull() const {
    // TODO: see StringIndexSort.cpp:1096-1099 / :1629-1632.
}

#define INSTANTIATE_SORTED_READER(T) template class SortedIndexReader<T>;
INSTANTIATE_SORTED_READER(bool)
INSTANTIATE_SORTED_READER(int8_t)
INSTANTIATE_SORTED_READER(int16_t)
INSTANTIATE_SORTED_READER(int32_t)
INSTANTIATE_SORTED_READER(int64_t)
INSTANTIATE_SORTED_READER(float)
INSTANTIATE_SORTED_READER(double)
#undef INSTANTIATE_SORTED_READER

}  // namespace milvus::index
