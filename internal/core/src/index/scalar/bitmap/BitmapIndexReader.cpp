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

#include "index/scalar/bitmap/BitmapIndexReader.h"

#include <utility>

namespace milvus::index {

template <typename T>
BitmapIndexReader<T>::BitmapIndexReader(OpenArgs args)
    : data_(std::move(args)) {
    // TODO: build the offset cache when data_.offset_cache is set (see
    // BitmapIndex.cpp:474-507 BuildOffsetCache).
}

template <typename T>
BitmapIndexReader<T>::~BitmapIndexReader() = default;

template <typename T>
ReaderCaps
BitmapIndexReader<T>::Caps() const {
    // `cheap_value_lookup` mirrors today's `SupportFastReverseLookup()`
    // override (BitmapIndex.h:82-85 -> `use_offset_cache_`): without the cache a
    // per-row reverse lookup is O(cardinality). §5.5: the reader STATES the
    // cost, the consumer decides (exec/expression/BloomFilterExpr.h:398 and
    // RoaringFilterExpr.h:179 are the two that ask).
    //
    // `value_lookup` is false when the indexed field is an ARRAY — today's
    // `HasRawData()` (BitmapIndex.h:196-202).
    return ReaderCaps{.predicate = true,
                      .nested = data_.nested,
                      .value_lookup = true,
                      .cheap_value_lookup = data_.offset_cache};
}

template <typename T>
Domain
BitmapIndexReader<T>::CoordDomain() const {
    return data_.nested ? Domain::Element : Domain::Row;
}

template <typename T>
int64_t
BitmapIndexReader<T>::Count() const {
    return static_cast<int64_t>(data_.total_num_rows);
}

template <typename T>
DataType
BitmapIndexReader<T>::ValueType() const {
    // TODO: derive from T.
}

template <typename T>
int64_t
BitmapIndexReader<T>::MemoryUsage() const {
    // TODO: move existing logic here (see BitmapIndex.h:130-191
    // ComputeByteSize — the roaring branch at :152-164, the bitset branch at
    // :164-180 and the three offset caches at :182-189).
}

template <typename T>
ResourceUsage
BitmapIndexReader<T>::CellByteSize() const {
    // See §12.3: roaring bitmaps are a family whose serialized size and
    // resident size differ a lot, and today the translator charges the FILE
    // size for this family. Named, not fixed here.
}

template <typename T>
TargetBitmap
BitmapIndexReader<T>::In(size_t n, const T* values) const {
    // TODO: move existing logic here (see BitmapIndex.cpp:720-760).
}

template <typename T>
TargetBitmap
BitmapIndexReader<T>::NotIn(size_t n, const T* values) const {
    // TODO: move existing logic here (see BitmapIndex.cpp:762-812).
}

template <typename T>
TargetBitmap
BitmapIndexReader<T>::Range(const T& value, CompareOp op) const {
    // TODO: move existing logic here (see BitmapIndex.cpp:899-910 dispatcher,
    // and the two layout kernels RangeForBitset :837-897 / RangeForRoaring
    // :975-1037). The third kernel, RangeForMmap (:911-973), folds into the
    // roaring one once mmap stops being a mode of this class (see the header).
}

template <typename T>
TargetBitmap
BitmapIndexReader<T>::Range(const T& lo, bool lo_inc, const T& hi,
                            bool hi_inc) const {
    // TODO: move existing logic here (see BitmapIndex.cpp:1098-1115 dispatcher,
    // kernels at :1039-1096 and :1178-1238).
}

template <typename T>
std::optional<owned_t<T>>
BitmapIndexReader<T>::Lookup(int64_t offset) const {
    // TODO: move existing logic here (see BitmapIndex.cpp:1255-1297
    // Reverse_Lookup and :1240-1253 Reverse_Lookup_InCache).
    //
    // ONE BEHAVIOUR CHANGE, and it is the reason §5.5 returns an optional:
    // today "value not found for this offset" is a `ThrowInfo(UnexpectedError)`
    // (BitmapIndex.cpp:1292). Absence is data, not an error.
}

template <typename T>
void
BitmapIndexReader<T>::Gather(
    const int64_t* offsets,
    int64_t count,
    const std::function<void(int64_t i, const T*, bool valid)>& out) const {
    // NEW SHAPE, no direct predecessor: today the batch reverse lookup is a
    // per-row loop on the caller's side (`ReverseDataFromIndex`,
    // segcore/Utils.h:151). §5.5 makes it an interface so the implementation
    // can cluster the offsets by its own layout — for a bitmap that means
    // walking the postings once instead of once per row.
}

template <typename T>
TargetBitmap
BitmapIndexReader<T>::IsNull() const {
    // TODO: move existing logic here (see BitmapIndex.cpp:814-824).
}

template <typename T>
TargetBitmap
BitmapIndexReader<T>::IsNotNull() const {
    // TODO: move existing logic here (see BitmapIndex.cpp:826-835).
}

template <typename T>
bool
BitmapIndexReader<T>::ShouldSkip(const T& lower, const T& upper,
                                 CompareOp op) const {
    // TODO: move existing logic here (see BitmapIndex.cpp:1299-1367).
}

BitmapStringIndexReader::BitmapStringIndexReader(OpenArgs args)
    : data_(std::move(args)) {
}

BitmapStringIndexReader::~BitmapStringIndexReader() = default;

ReaderCaps
BitmapStringIndexReader::Caps() const {
    return ReaderCaps{.predicate = true,
                      .pattern_match = true,
                      .nested = data_.nested,
                      .value_lookup = true,
                      .cheap_value_lookup = data_.offset_cache};
}

Domain
BitmapStringIndexReader::CoordDomain() const {
    return data_.nested ? Domain::Element : Domain::Row;
}

int64_t
BitmapStringIndexReader::Count() const {
    return static_cast<int64_t>(data_.total_num_rows);
}

DataType
BitmapStringIndexReader::ValueType() const {
    return DataType::VARCHAR;
}

int64_t
BitmapStringIndexReader::MemoryUsage() const {
    // TODO: see BitmapIndex.h:130-191.
}

ResourceUsage
BitmapStringIndexReader::CellByteSize() const {
    // See §12.3.
}

TargetBitmap
BitmapStringIndexReader::In(size_t n, const std::string_view* values) const {
    // TODO: see BitmapIndex.cpp:720-760 (string instantiation).
}

TargetBitmap
BitmapStringIndexReader::NotIn(size_t n, const std::string_view* values) const {
    // TODO: see BitmapIndex.cpp:762-812.
}

TargetBitmap
BitmapStringIndexReader::Range(const std::string_view& value,
                               CompareOp op) const {
    // TODO: see BitmapIndex.cpp:899-910 and kernels.
}

TargetBitmap
BitmapStringIndexReader::Range(const std::string_view& lo, bool lo_inc,
                               const std::string_view& hi, bool hi_inc) const {
    // TODO: see BitmapIndex.cpp:1098-1115 and kernels.
}

std::optional<std::string>
BitmapStringIndexReader::Lookup(int64_t offset) const {
    // TODO: see BitmapIndex.cpp:1255-1297.
}

void
BitmapStringIndexReader::Gather(
    const int64_t* offsets,
    int64_t count,
    const std::function<void(int64_t i, const std::string_view*, bool valid)>&
        out) const {
    // TODO: see BitmapIndexReader<T>::Gather.
}

TargetBitmap
BitmapStringIndexReader::PatternMatch(std::string_view pattern,
                                      PatternOp op) const {
    // TODO: move existing logic here (see BitmapIndex.h:223-277) — the
    // five-way dispatch including the regex branch at :237-271.
    // Absorbs `Query(DatasetPtr)`'s string specialization
    // (BitmapIndex.cpp:1375-1415), which unpacked the same operation out of a
    // knowhere dataset.
}

TargetBitmap
BitmapStringIndexReader::PatternQuery(std::string_view pattern) const {
    // TODO: move existing logic here (see BitmapIndex.h:280-316).
}

TargetBitmap
BitmapStringIndexReader::IsNull() const {
    // TODO: see BitmapIndex.cpp:814-824.
}

TargetBitmap
BitmapStringIndexReader::IsNotNull() const {
    // TODO: see BitmapIndex.cpp:826-835.
}

#define INSTANTIATE_BITMAP_READER(T) template class BitmapIndexReader<T>;
INSTANTIATE_BITMAP_READER(bool)
INSTANTIATE_BITMAP_READER(int8_t)
INSTANTIATE_BITMAP_READER(int16_t)
INSTANTIATE_BITMAP_READER(int32_t)
INSTANTIATE_BITMAP_READER(int64_t)
INSTANTIATE_BITMAP_READER(float)
INSTANTIATE_BITMAP_READER(double)
#undef INSTANTIATE_BITMAP_READER

}  // namespace milvus::index
