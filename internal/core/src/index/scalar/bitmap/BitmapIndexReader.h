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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string_view>

#include <roaring/roaring.hh>

#include "common/Types.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/NullReader.h"
#include "index/contracts/PatternMatchReader.h"
#include "index/contracts/ScalarPredicateReader.h"
#include "index/contracts/ScalarValueReader.h"

// The READER of the bitmap family.
//
// See 01-scalar-index.md §5.1, §5.5, §5.8, and §8's row:
//   | `BitmapIndex<T>` / `ScalarIndexSort<T>` / `StringIndexMarisa`
//   | `ScalarPredicateReader<T>` + `ScalarValueReader<T>`
//   | the `is_nested_index_` mode bit survives, expressed as
//   | `CoordDomain() == Element` (§5.8)
//
// ==========================================================================
// A DEVIATION FROM §8'S TABLE, DELIBERATE AND REPORTED.
//
// That row attaches `PatternMatchReader` only to marisa ("marisa additionally
// + `PatternMatchReader`"). The code disagrees: `BitmapIndex<std::string>`
// implements the FULL LIKE family — `SupportPatternMatch()` returns
// `std::is_same_v<T, std::string>` (`BitmapIndex.h:218-221`) and `PatternMatch`
// (`BitmapIndex.h:223-277`) handles prefix / postfix / inner / match / regex,
// with `PatternQuery` at `:280-316` and a `Query` specialization using
// `milvus::query::Match` at `BitmapIndex.cpp:1375-1415`. `StringIndexSort` does
// the same (`StringIndexSort.h:130-133`, `.cpp:498-521`). Following the table
// literally would silently delete two live pattern-match paths, so the
// skeleton follows the code and flags the table.
// ==========================================================================
//
// TWO CLASSES, NOT ONE TEMPLATE, and that also follows the code: `BitmapIndex`
// specializes `ParseKey`, `SerializeIndexData`, `GetIndexDataSize`,
// `DeserializeIndexData` and `Query` for `std::string`
// (`BitmapIndex.cpp:258,313,509,550,1375`). Splitting them makes the capability
// difference structural — the numeric reader HAS NO pattern-match method to
// call, rather than one that throws (§3 principle 3, §10 rule 4).

namespace milvus::index {

// How the postings are held in memory. Was `BitmapIndexBuildMode`
// (`BitmapIndex.h:38-41`); it is a LOAD-TIME layout choice
// (`ChooseIndexLoadMode`, `BitmapIndex.cpp:434-444`), not a build mode, hence
// the rename.
enum class BitmapLayout {
    Roaring,
    Bitset,
};

template <typename T>
class BitmapIndexReader final : public IndexReaderBase,
                                public ScalarPredicateReader<T>,
                                public ScalarValueReader<T>,
                                public NullReader {
 public:
    struct OpenArgs {
        BitmapLayout layout{BitmapLayout::Roaring};
        std::map<T, roaring::Roaring> roaring_postings;
        std::map<T, TargetBitmap> bitset_postings;
        TargetBitmap valid_bitset;
        size_t total_num_rows{0};
        bool nested{false};
        // When true the reader keeps the sorted-iterator vector that makes
        // per-row reverse lookup O(1) instead of O(cardinality). Was
        // `use_offset_cache_` (`BitmapIndex.h:455`), driven by the
        // `ENABLE_OFFSET_CACHE` config key (`index/Meta.h:95`).
        bool offset_cache{false};
    };

    explicit BitmapIndexReader(OpenArgs args);

    ~BitmapIndexReader() override;

    // ---- IndexReaderBase ------------------------------------------------

    ReaderCaps
    Caps() const override;

    Domain
    CoordDomain() const override;

    int64_t
    Count() const override;

    DataType
    ValueType() const override;

    int64_t
    MemoryUsage() const override;

    ResourceUsage
    CellByteSize() const override;

    // ---- ScalarPredicateReader<T> (§5.1) --------------------------------

    TargetBitmap
    In(size_t n, const T* values) const override;

    TargetBitmap
    NotIn(size_t n, const T* values) const override;

    TargetBitmap
    Range(const T& value, CompareOp op) const override;

    TargetBitmap
    Range(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const override;

    // ---- ScalarValueReader<T> (§5.5) ------------------------------------

    std::optional<owned_t<T>>
    Lookup(int64_t offset) const override;

    void
    Gather(const int64_t* offsets,
           int64_t count,
           const std::function<void(int64_t i, const T*, bool valid)>& out)
        const override;

    // ---- NullReader ------------------------------------------------------

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    // Zone-map style short circuit; kept private, it is not an interface.
    bool
    ShouldSkip(const T& lower, const T& upper, CompareOp op) const;

    OpenArgs data_;

    // GONE: `is_mmap_`, `mmap_data_`, `mmap_size_`, `bitmap_info_map_` and
    // `MMapIndexData` / `UnmapIndexData` (`BitmapIndex.h:448-452`,
    // `.cpp:564-628`, `:70-81`). Whether the postings are mapped or heap-backed
    // is decided by the LOADER from `storage::LoadOptions` and expressed to the
    // reader as the postings it is handed. That is §3 principle 6 (IO is
    // injected) and §10 rule 2 applied to mmap.
};

// The string bitmap reader. Same interfaces plus `PatternMatchReader`.
class BitmapStringIndexReader final
    : public IndexReaderBase,
      public ScalarPredicateReader<std::string_view>,
      public ScalarValueReader<std::string_view>,
      public PatternMatchReader,
      public NullReader {
 public:
    struct OpenArgs {
        BitmapLayout layout{BitmapLayout::Roaring};
        std::map<std::string, roaring::Roaring> roaring_postings;
        std::map<std::string, TargetBitmap> bitset_postings;
        TargetBitmap valid_bitset;
        size_t total_num_rows{0};
        bool nested{false};
        bool offset_cache{false};
    };

    explicit BitmapStringIndexReader(OpenArgs args);

    ~BitmapStringIndexReader() override;

    ReaderCaps
    Caps() const override;

    Domain
    CoordDomain() const override;

    int64_t
    Count() const override;

    DataType
    ValueType() const override;

    int64_t
    MemoryUsage() const override;

    ResourceUsage
    CellByteSize() const override;

    TargetBitmap
    In(size_t n, const std::string_view* values) const override;

    TargetBitmap
    NotIn(size_t n, const std::string_view* values) const override;

    TargetBitmap
    Range(const std::string_view& value, CompareOp op) const override;

    TargetBitmap
    Range(const std::string_view& lo,
          bool lo_inc,
          const std::string_view& hi,
          bool hi_inc) const override;

    // The value interface returns an OWNING `std::string` —
    // `owned_t<string_view>` (§5.5). The view decision of §5.1 covers inputs
    // only.
    std::optional<std::string>
    Lookup(int64_t offset) const override;

    void
    Gather(const int64_t* offsets,
           int64_t count,
           const std::function<
               void(int64_t i, const std::string_view*, bool valid)>& out)
        const override;

    TargetBitmap
    PatternMatch(std::string_view pattern, PatternOp op) const override;

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    TargetBitmap
    PatternQuery(std::string_view pattern) const;

    OpenArgs data_;
};

}  // namespace milvus::index
