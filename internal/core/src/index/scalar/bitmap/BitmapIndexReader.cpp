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

#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <type_traits>
#include <unistd.h>
#include <utility>

#include "common/EasyAssert.h"
#include "common/RegexQuery.h"

namespace milvus::index {
namespace {

template <typename T>
constexpr DataType
CppDataType() {
    if constexpr (std::is_same_v<T, bool>) {
        return DataType::BOOL;
    } else if constexpr (std::is_same_v<T, int8_t>) {
        return DataType::INT8;
    } else if constexpr (std::is_same_v<T, int16_t>) {
        return DataType::INT16;
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return DataType::INT32;
    } else if constexpr (std::is_same_v<T, int64_t>) {
        return DataType::INT64;
    } else if constexpr (std::is_same_v<T, float>) {
        return DataType::FLOAT;
    } else if constexpr (std::is_same_v<T, double>) {
        return DataType::DOUBLE;
    } else {
        return DataType::VARCHAR;
    }
}

void
UnionPosting(TargetBitmap& result, const roaring::Roaring& posting) {
    for (auto coordinate : posting) {
        AssertInfo(coordinate < result.size(),
                   "bitmap posting coordinate {} exceeds count {}",
                   coordinate,
                   result.size());
        result.set(coordinate);
    }
}

void
UnionPosting(TargetBitmap& result, const TargetBitmap& posting) {
    result |= posting;
}

template <typename Iterator>
void
UnionRange(TargetBitmap& result, Iterator begin, Iterator end) {
    for (auto it = begin; it != end; ++it) {
        UnionPosting(result, it->second);
    }
}

template <typename Map, typename Key>
TargetBitmap
MapIn(const Map& postings, size_t count, size_t n, const Key* values) {
    TargetBitmap result(count, false);
    for (size_t i = 0; i < n; ++i) {
        auto it = postings.find(values[i]);
        if (it != postings.end()) {
            UnionPosting(result, it->second);
        }
    }
    return result;
}

template <typename Map, typename Key>
TargetBitmap
MapNotIn(const Map& postings,
         const TargetBitmap& validity,
         size_t count,
         size_t n,
         const Key* values) {
    auto excluded = MapIn(postings, count, n, values);
    excluded.flip();
    TargetBitmap result = validity.clone();
    result &= excluded;
    return result;
}

template <typename Map, typename Key>
TargetBitmap
MapRange(const Map& postings, size_t count, const Key& value, CompareOp op) {
    if (op == CompareOp::Equal) {
        return MapIn(postings, count, 1, &value);
    }
    if (op == CompareOp::NotEqual) {
        // The caller applies validity after this helper.
        auto result = MapIn(postings, count, 1, &value);
        result.flip();
        return result;
    }

    TargetBitmap result(count, false);
    auto begin = postings.begin();
    auto end = postings.end();
    switch (op) {
        case CompareOp::LessThan:
            end = postings.lower_bound(value);
            break;
        case CompareOp::LessEqual:
            end = postings.upper_bound(value);
            break;
        case CompareOp::GreaterThan:
            begin = postings.upper_bound(value);
            break;
        case CompareOp::GreaterEqual:
            begin = postings.lower_bound(value);
            break;
        case CompareOp::Equal:
        case CompareOp::NotEqual:
            break;
    }
    UnionRange(result, begin, end);
    return result;
}

template <typename Map, typename Key>
TargetBitmap
MapRange(const Map& postings,
         size_t count,
         const Key& lo,
         bool lo_inc,
         const Key& hi,
         bool hi_inc) {
    TargetBitmap result(count, false);
    if (hi < lo || (lo == hi && !(lo_inc && hi_inc))) {
        return result;
    }
    const auto begin =
        lo_inc ? postings.lower_bound(lo) : postings.upper_bound(lo);
    const auto end =
        hi_inc ? postings.upper_bound(hi) : postings.lower_bound(hi);
    UnionRange(result, begin, end);
    return result;
}

template <typename K, typename V>
int64_t
PostingMapHeapBytes(const std::map<K, V>& postings,
                    bool mapped_roaring_payload) {
    int64_t total = 0;
    for (const auto& [key, posting] : postings) {
        // Approximate libstdc++'s red-black-tree links/color storage. The key
        // and mapped object live in that allocation and are counted
        // separately so frozen Roaring views still charge their heap
        // metadata while their backing bytes are charged as file usage.
        total += 40 + sizeof(K) + sizeof(V);
        if constexpr (std::is_same_v<K, std::string>) {
            total += static_cast<int64_t>(key.capacity() + 1);
        }
        if constexpr (std::is_same_v<V, roaring::Roaring>) {
            if (!mapped_roaring_payload) {
                // CRoaring exposes no allocator footprint. Keep the existing
                // engine-size approximation for heap postings; frozen views
                // skip it because their exact backing-file size is known.
                total += static_cast<int64_t>(posting.getSizeInBytes());
            }
        } else {
            total += static_cast<int64_t>(posting.size_in_bytes());
        }
    }
    return total;
}

template <typename Map>
const typename Map::key_type*
LookupMap(const Map& postings, size_t coordinate) {
    for (const auto& [key, posting] : postings) {
        if constexpr (std::is_same_v<typename Map::mapped_type,
                                     roaring::Roaring>) {
            if (posting.contains(static_cast<uint32_t>(coordinate))) {
                return &key;
            }
        } else if (posting[coordinate]) {
            return &key;
        }
    }
    return nullptr;
}

template <typename Map, typename Cache>
void
BuildOffsetCache(const Map& postings, Cache& cache, size_t count) {
    cache.assign(count, nullptr);
    for (const auto& [key, posting] : postings) {
        if constexpr (std::is_same_v<typename Map::mapped_type,
                                     roaring::Roaring>) {
            for (auto coordinate : posting) {
                AssertInfo(coordinate < count,
                           "bitmap posting coordinate {} exceeds count {}",
                           coordinate,
                           count);
                cache[coordinate] = &key;
            }
        } else {
            for (size_t coordinate = 0; coordinate < count; ++coordinate) {
                if (posting[coordinate]) {
                    cache[coordinate] = &key;
                }
            }
        }
    }
}

template <typename Map>
std::vector<const typename Map::key_type*>
GatherFromMap(const Map& postings,
              const TargetBitmap& validity,
              size_t total_count,
              const int64_t* offsets,
              int64_t count) {
    using Key = typename Map::key_type;
    std::vector<const Key*> result(static_cast<size_t>(count), nullptr);
    std::map<size_t, std::vector<int64_t>> wanted;
    for (int64_t i = 0; i < count; ++i) {
        AssertInfo(
            offsets[i] >= 0 && static_cast<size_t>(offsets[i]) < total_count,
            "bitmap gather offset {} is outside [0, {})",
            offsets[i],
            total_count);
        const auto coordinate = static_cast<size_t>(offsets[i]);
        if (validity[coordinate]) {
            wanted[coordinate].push_back(i);
        }
    }

    for (const auto& [key, posting] : postings) {
        if (wanted.empty()) {
            break;
        }
        if constexpr (std::is_same_v<typename Map::mapped_type,
                                     roaring::Roaring>) {
            for (auto coordinate : posting) {
                auto match = wanted.find(coordinate);
                if (match == wanted.end()) {
                    continue;
                }
                for (auto i : match->second) {
                    result[static_cast<size_t>(i)] = &key;
                }
                wanted.erase(match);
            }
        } else {
            for (auto match = wanted.begin(); match != wanted.end();) {
                if (!posting[match->first]) {
                    ++match;
                    continue;
                }
                for (auto i : match->second) {
                    result[static_cast<size_t>(i)] = &key;
                }
                match = wanted.erase(match);
            }
        }
    }
    return result;
}

}  // namespace

BitmapMmapOwner::BitmapMmapOwner(char* data, size_t size, std::string path)
    : data_(data), size_(size), path_(std::move(path)) {
}

BitmapMmapOwner::~BitmapMmapOwner() {
    if (data_ != nullptr && size_ != 0) {
        munmap(data_, size_);
    }
    if (!path_.empty()) {
        unlink(path_.c_str());
    }
}

const char*
BitmapMmapOwner::Data() const {
    return data_;
}

size_t
BitmapMmapOwner::Size() const {
    return size_;
}

template <typename T>
BitmapIndexReader<T>::BitmapIndexReader(OpenArgs args)
    : data_(std::move(args)) {
    AssertInfo(data_.postings != nullptr,
               "bitmap reader requires posting storage");
    AssertInfo(data_.valid_bitset != nullptr,
               "bitmap reader requires a validity bitmap");
    AssertInfo(data_.valid_bitset->size() == data_.total_num_rows,
               "bitmap validity size {} does not match row count {}",
               data_.valid_bitset->size(),
               data_.total_num_rows);
    AssertInfo(data_.postings->layout == BitmapLayout::Roaring
                   ? data_.postings->bitset_postings.empty()
                   : data_.postings->roaring_postings.empty(),
               "bitmap posting storage contains an inactive layout");
    if (data_.value_type == DataType::NONE) {
        data_.value_type = CppDataType<T>();
    }
    data_.offset_cache = data_.offset_cache && data_.value_lookup;
    if (data_.offset_cache) {
        if (data_.postings->layout == BitmapLayout::Roaring) {
            BuildOffsetCache(data_.postings->roaring_postings,
                             offset_cache_,
                             data_.total_num_rows);
        } else {
            BuildOffsetCache(data_.postings->bitset_postings,
                             offset_cache_,
                             data_.total_num_rows);
        }
    }
}

template <typename T>
BitmapIndexReader<T>::~BitmapIndexReader() = default;

template <typename T>
ReaderCaps
BitmapIndexReader<T>::Caps() const {
    return ReaderCaps{
        .predicate = true,
        .nested = data_.nested,
        .value_lookup = data_.value_lookup,
        .cheap_value_lookup = data_.value_lookup && data_.offset_cache,
        .exact = !data_.nested};
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
    return data_.value_type;
}

template <typename T>
int64_t
BitmapIndexReader<T>::MemoryUsage() const {
    int64_t total = static_cast<int64_t>(data_.valid_bitset->size_in_bytes());
    if (data_.postings->layout == BitmapLayout::Roaring) {
        total += PostingMapHeapBytes(data_.postings->roaring_postings,
                                     data_.postings->mmap_owner != nullptr);
    } else {
        total += PostingMapHeapBytes(data_.postings->bitset_postings, false);
    }
    total += static_cast<int64_t>(
        offset_cache_.capacity() *
        sizeof(typename decltype(offset_cache_)::value_type));
    return total;
}

template <typename T>
cachinglayer::ResourceUsage
BitmapIndexReader<T>::CellByteSize() const {
    return {MemoryUsage(),
            data_.postings->mmap_owner == nullptr
                ? 0
                : static_cast<int64_t>(data_.postings->mmap_owner->Size())};
}

template <typename T>
TargetBitmap
BitmapIndexReader<T>::In(size_t n, const T* values) const {
    AssertInfo(n == 0 || values != nullptr,
               "bitmap In received null values with non-zero count");
    return data_.postings->layout == BitmapLayout::Roaring
               ? MapIn(data_.postings->roaring_postings,
                       data_.total_num_rows,
                       n,
                       values)
               : MapIn(data_.postings->bitset_postings,
                       data_.total_num_rows,
                       n,
                       values);
}

template <typename T>
TargetBitmap
BitmapIndexReader<T>::NotIn(size_t n, const T* values) const {
    AssertInfo(n == 0 || values != nullptr,
               "bitmap NotIn received null values with non-zero count");
    return data_.postings->layout == BitmapLayout::Roaring
               ? MapNotIn(data_.postings->roaring_postings,
                          *data_.valid_bitset,
                          data_.total_num_rows,
                          n,
                          values)
               : MapNotIn(data_.postings->bitset_postings,
                          *data_.valid_bitset,
                          data_.total_num_rows,
                          n,
                          values);
}

template <typename T>
TargetBitmap
BitmapIndexReader<T>::Range(const T& value, CompareOp op) const {
    auto result = data_.postings->layout == BitmapLayout::Roaring
                      ? MapRange(data_.postings->roaring_postings,
                                 data_.total_num_rows,
                                 value,
                                 op)
                      : MapRange(data_.postings->bitset_postings,
                                 data_.total_num_rows,
                                 value,
                                 op);
    if (op == CompareOp::NotEqual) {
        result &= *data_.valid_bitset;
    }
    return result;
}

template <typename T>
TargetBitmap
BitmapIndexReader<T>::Range(const T& lo,
                            bool lo_inc,
                            const T& hi,
                            bool hi_inc) const {
    return data_.postings->layout == BitmapLayout::Roaring
               ? MapRange(data_.postings->roaring_postings,
                          data_.total_num_rows,
                          lo,
                          lo_inc,
                          hi,
                          hi_inc)
               : MapRange(data_.postings->bitset_postings,
                          data_.total_num_rows,
                          lo,
                          lo_inc,
                          hi,
                          hi_inc);
}

template <typename T>
std::optional<owned_t<T>>
BitmapIndexReader<T>::Lookup(int64_t offset) const {
    if (!data_.value_lookup) {
        return std::nullopt;
    }
    AssertInfo(
        offset >= 0 && static_cast<size_t>(offset) < data_.total_num_rows,
        "bitmap lookup offset {} is outside [0, {})",
        offset,
        data_.total_num_rows);
    const auto coordinate = static_cast<size_t>(offset);
    if (!(*data_.valid_bitset)[coordinate]) {
        return std::nullopt;
    }
    const T* value = nullptr;
    if (data_.offset_cache) {
        value = offset_cache_[coordinate];
    } else if (data_.postings->layout == BitmapLayout::Roaring) {
        value = LookupMap(data_.postings->roaring_postings, coordinate);
    } else {
        value = LookupMap(data_.postings->bitset_postings, coordinate);
    }
    if (value == nullptr) {
        return std::nullopt;
    }
    return *value;
}

template <typename T>
void
BitmapIndexReader<T>::Gather(
    const int64_t* offsets,
    int64_t count,
    const std::function<void(int64_t i, const T*, bool valid)>& out) const {
    AssertInfo(count >= 0 && (count == 0 || offsets != nullptr),
               "bitmap Gather received invalid offsets/count");
    if (!data_.value_lookup) {
        for (int64_t i = 0; i < count; ++i) {
            out(i, nullptr, false);
        }
        return;
    }
    std::vector<const T*> values;
    if (data_.offset_cache) {
        values.resize(static_cast<size_t>(count), nullptr);
        for (int64_t i = 0; i < count; ++i) {
            AssertInfo(offsets[i] >= 0 && static_cast<size_t>(offsets[i]) <
                                              data_.total_num_rows,
                       "bitmap gather offset {} is outside [0, {})",
                       offsets[i],
                       data_.total_num_rows);
            values[static_cast<size_t>(i)] =
                offset_cache_[static_cast<size_t>(offsets[i])];
        }
    } else if (data_.postings->layout == BitmapLayout::Roaring) {
        values = GatherFromMap(data_.postings->roaring_postings,
                               *data_.valid_bitset,
                               data_.total_num_rows,
                               offsets,
                               count);
    } else {
        values = GatherFromMap(data_.postings->bitset_postings,
                               *data_.valid_bitset,
                               data_.total_num_rows,
                               offsets,
                               count);
    }
    for (int64_t i = 0; i < count; ++i) {
        const auto* value = values[static_cast<size_t>(i)];
        if (value != nullptr) {
            out(i, value, true);
        } else {
            out(i, nullptr, false);
        }
    }
}

template <typename T>
TargetBitmap
BitmapIndexReader<T>::IsNull() const {
    TargetBitmap result = data_.valid_bitset->clone();
    result.flip();
    return result;
}

template <typename T>
TargetBitmap
BitmapIndexReader<T>::IsNotNull() const {
    return data_.valid_bitset->clone();
}

template <typename T>
bool
BitmapIndexReader<T>::ShouldSkip(const T& lower,
                                 const T& upper,
                                 CompareOp op) const {
    const auto check = [&](const auto& postings) {
        if (postings.empty()) {
            return true;
        }
        const auto& min = postings.begin()->first;
        const auto& max = postings.rbegin()->first;
        switch (op) {
            case CompareOp::Equal:
                return lower < min || max < lower;
            case CompareOp::NotEqual:
                return postings.size() == 1 && min == lower;
            case CompareOp::GreaterThan:
                return !(lower < max);
            case CompareOp::GreaterEqual:
                return max < lower;
            case CompareOp::LessThan:
                return !(min < upper);
            case CompareOp::LessEqual:
                return upper < min;
        }
        return false;
    };
    return data_.postings->layout == BitmapLayout::Roaring
               ? check(data_.postings->roaring_postings)
               : check(data_.postings->bitset_postings);
}

BitmapStringIndexReader::BitmapStringIndexReader(OpenArgs args)
    : data_(std::move(args)) {
    AssertInfo(data_.postings != nullptr,
               "bitmap string reader requires posting storage");
    AssertInfo(data_.valid_bitset != nullptr,
               "bitmap string reader requires a validity bitmap");
    AssertInfo(data_.valid_bitset->size() == data_.total_num_rows,
               "bitmap string validity size {} does not match row count {}",
               data_.valid_bitset->size(),
               data_.total_num_rows);
    AssertInfo(data_.postings->layout == BitmapLayout::Roaring
                   ? data_.postings->bitset_postings.empty()
                   : data_.postings->roaring_postings.empty(),
               "bitmap string posting storage contains an inactive layout");
    if (data_.value_type == DataType::NONE) {
        data_.value_type = DataType::VARCHAR;
    }
    data_.offset_cache = data_.offset_cache && data_.value_lookup;
    if (data_.offset_cache) {
        if (data_.postings->layout == BitmapLayout::Roaring) {
            BuildOffsetCache(data_.postings->roaring_postings,
                             offset_cache_,
                             data_.total_num_rows);
        } else {
            BuildOffsetCache(data_.postings->bitset_postings,
                             offset_cache_,
                             data_.total_num_rows);
        }
    }
}

BitmapStringIndexReader::~BitmapStringIndexReader() = default;

ReaderCaps
BitmapStringIndexReader::Caps() const {
    return ReaderCaps{
        .predicate = true,
        .pattern_match = true,
        .nested = data_.nested,
        .value_lookup = data_.value_lookup,
        .cheap_value_lookup = data_.value_lookup && data_.offset_cache,
        .exact = !data_.nested};
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
    return data_.value_type;
}

int64_t
BitmapStringIndexReader::MemoryUsage() const {
    int64_t total = static_cast<int64_t>(data_.valid_bitset->size_in_bytes());
    if (data_.postings->layout == BitmapLayout::Roaring) {
        total += PostingMapHeapBytes(data_.postings->roaring_postings,
                                     data_.postings->mmap_owner != nullptr);
    } else {
        total += PostingMapHeapBytes(data_.postings->bitset_postings, false);
    }
    total += static_cast<int64_t>(
        offset_cache_.capacity() *
        sizeof(typename decltype(offset_cache_)::value_type));
    return total;
}

cachinglayer::ResourceUsage
BitmapStringIndexReader::CellByteSize() const {
    return {MemoryUsage(),
            data_.postings->mmap_owner == nullptr
                ? 0
                : static_cast<int64_t>(data_.postings->mmap_owner->Size())};
}

TargetBitmap
BitmapStringIndexReader::In(size_t n, const std::string_view* values) const {
    AssertInfo(n == 0 || values != nullptr,
               "bitmap string In received null values with non-zero count");
    TargetBitmap result(data_.total_num_rows, false);
    const auto apply = [&](const auto& postings) {
        for (size_t i = 0; i < n; ++i) {
            auto it = postings.find(std::string(values[i]));
            if (it != postings.end()) {
                UnionPosting(result, it->second);
            }
        }
    };
    if (data_.postings->layout == BitmapLayout::Roaring) {
        apply(data_.postings->roaring_postings);
    } else {
        apply(data_.postings->bitset_postings);
    }
    return result;
}

TargetBitmap
BitmapStringIndexReader::NotIn(size_t n, const std::string_view* values) const {
    auto excluded = In(n, values);
    excluded.flip();
    TargetBitmap result = data_.valid_bitset->clone();
    result &= excluded;
    return result;
}

TargetBitmap
BitmapStringIndexReader::Range(const std::string_view& value,
                               CompareOp op) const {
    const std::string owned(value);
    auto result = data_.postings->layout == BitmapLayout::Roaring
                      ? MapRange(data_.postings->roaring_postings,
                                 data_.total_num_rows,
                                 owned,
                                 op)
                      : MapRange(data_.postings->bitset_postings,
                                 data_.total_num_rows,
                                 owned,
                                 op);
    if (op == CompareOp::NotEqual) {
        result &= *data_.valid_bitset;
    }
    return result;
}

TargetBitmap
BitmapStringIndexReader::Range(const std::string_view& lo,
                               bool lo_inc,
                               const std::string_view& hi,
                               bool hi_inc) const {
    const std::string owned_lo(lo);
    const std::string owned_hi(hi);
    return data_.postings->layout == BitmapLayout::Roaring
               ? MapRange(data_.postings->roaring_postings,
                          data_.total_num_rows,
                          owned_lo,
                          lo_inc,
                          owned_hi,
                          hi_inc)
               : MapRange(data_.postings->bitset_postings,
                          data_.total_num_rows,
                          owned_lo,
                          lo_inc,
                          owned_hi,
                          hi_inc);
}

std::optional<std::string>
BitmapStringIndexReader::Lookup(int64_t offset) const {
    if (!data_.value_lookup) {
        return std::nullopt;
    }
    AssertInfo(
        offset >= 0 && static_cast<size_t>(offset) < data_.total_num_rows,
        "bitmap lookup offset {} is outside [0, {})",
        offset,
        data_.total_num_rows);
    const auto coordinate = static_cast<size_t>(offset);
    if (!(*data_.valid_bitset)[coordinate]) {
        return std::nullopt;
    }
    const std::string* value = nullptr;
    if (data_.offset_cache) {
        value = offset_cache_[coordinate];
    } else if (data_.postings->layout == BitmapLayout::Roaring) {
        value = LookupMap(data_.postings->roaring_postings, coordinate);
    } else {
        value = LookupMap(data_.postings->bitset_postings, coordinate);
    }
    return value == nullptr ? std::nullopt : std::optional<std::string>(*value);
}

void
BitmapStringIndexReader::Gather(
    const int64_t* offsets,
    int64_t count,
    const std::function<void(int64_t i, const std::string_view*, bool valid)>&
        out) const {
    AssertInfo(count >= 0 && (count == 0 || offsets != nullptr),
               "bitmap Gather received invalid offsets/count");
    if (!data_.value_lookup) {
        for (int64_t i = 0; i < count; ++i) {
            out(i, nullptr, false);
        }
        return;
    }
    std::vector<const std::string*> values;
    if (data_.offset_cache) {
        values.resize(static_cast<size_t>(count), nullptr);
        for (int64_t i = 0; i < count; ++i) {
            AssertInfo(offsets[i] >= 0 && static_cast<size_t>(offsets[i]) <
                                              data_.total_num_rows,
                       "bitmap gather offset {} is outside [0, {})",
                       offsets[i],
                       data_.total_num_rows);
            values[static_cast<size_t>(i)] =
                offset_cache_[static_cast<size_t>(offsets[i])];
        }
    } else if (data_.postings->layout == BitmapLayout::Roaring) {
        values = GatherFromMap(data_.postings->roaring_postings,
                               *data_.valid_bitset,
                               data_.total_num_rows,
                               offsets,
                               count);
    } else {
        values = GatherFromMap(data_.postings->bitset_postings,
                               *data_.valid_bitset,
                               data_.total_num_rows,
                               offsets,
                               count);
    }
    for (int64_t i = 0; i < count; ++i) {
        const auto* value = values[static_cast<size_t>(i)];
        if (value != nullptr) {
            const std::string_view view(*value);
            out(i, &view, true);
        } else {
            out(i, nullptr, false);
        }
    }
}

TargetBitmap
BitmapStringIndexReader::PatternMatch(std::string_view pattern,
                                      PatternOp op) const {
    if (op == PatternOp::Match) {
        return PatternQuery(pattern);
    }

    std::function<bool(const std::string&)> matches;
    const std::string owned(pattern);
    switch (op) {
        case PatternOp::PrefixMatch:
            matches = [owned](const std::string& value) {
                return value.size() >= owned.size() &&
                       value.compare(0, owned.size(), owned) == 0;
            };
            break;
        case PatternOp::PostfixMatch:
            matches = [owned](const std::string& value) {
                return value.size() >= owned.size() &&
                       value.compare(value.size() - owned.size(),
                                     owned.size(),
                                     owned) == 0;
            };
            break;
        case PatternOp::InnerMatch:
            matches = [owned](const std::string& value) {
                return value.find(owned) != std::string::npos;
            };
            break;
        case PatternOp::RegexMatch: {
            auto matcher = std::make_shared<PartialRegexMatcher>(owned);
            matches = [matcher](const std::string& value) {
                return (*matcher)(value);
            };
            break;
        }
        case PatternOp::Match:
            break;
    }

    TargetBitmap result(data_.total_num_rows, false);
    const auto apply = [&](const auto& postings) {
        for (const auto& [key, posting] : postings) {
            if (matches(key)) {
                UnionPosting(result, posting);
            }
        }
    };
    if (data_.postings->layout == BitmapLayout::Roaring) {
        apply(data_.postings->roaring_postings);
    } else {
        apply(data_.postings->bitset_postings);
    }
    return result;
}

TargetBitmap
BitmapStringIndexReader::PatternQuery(std::string_view pattern) const {
    LikePatternMatcher matcher{std::string(pattern)};
    TargetBitmap result(data_.total_num_rows, false);
    const auto apply = [&](const auto& postings) {
        for (const auto& [key, posting] : postings) {
            if (matcher(key)) {
                UnionPosting(result, posting);
            }
        }
    };
    if (data_.postings->layout == BitmapLayout::Roaring) {
        apply(data_.postings->roaring_postings);
    } else {
        apply(data_.postings->bitset_postings);
    }
    return result;
}

TargetBitmap
BitmapStringIndexReader::IsNull() const {
    TargetBitmap result = data_.valid_bitset->clone();
    result.flip();
    return result;
}

TargetBitmap
BitmapStringIndexReader::IsNotNull() const {
    return data_.valid_bitset->clone();
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
