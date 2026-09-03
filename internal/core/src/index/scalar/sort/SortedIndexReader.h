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
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/Types.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/NullReader.h"
#include "index/contracts/PatternMatchReader.h"
#include "index/contracts/ScalarPredicateReader.h"
#include "index/contracts/ScalarValueReader.h"
#include "index/scalar/sort/IndexStructure.h"

// The READERS of the sorted family ("STL_SORT").
//
// See 01-scalar-index.md §5.1, §5.5, §5.8 and §8.
//
// ==========================================================================
// TWO READERS, BECAUSE THERE ARE TWO IMPLEMENTATIONS — AND §8 IS WRONG ABOUT
// THAT ROW.
//
// §8 lists `StringIndexSort` under "`StringIndexSort` / `BoolIndex` — thin
// aliases, migrate along". `BoolIndex` genuinely is one: `BoolIndex.h` is 32
// lines with no class at all, just `using BoolIndexPtr =
// shared_ptr<ScalarIndexSort<bool>>` and a factory (`BoolIndex.h:25-31`). That
// file disappears entirely; `bool` is simply an instantiation.
//
// `StringIndexSort` is NOT an alias. It is 576 header lines plus 1860
// implementation lines with its OWN pImpl hierarchy
// (`StringIndexSortImpl` -> `StringIndexSortMemoryImpl` /
// `StringIndexSortMmapImpl`, `StringIndexSort.h:187-564`), its own binary
// format with a magic number and version (`StringIndexSort.h:45-47`), and
// `PatternMatch` support that `ScalarIndexSort` does not have
// (`StringIndexSort.h:130-133`, `.cpp:498-521`). It exists at all because
// `ScalarIndexSort<T>` is `static_assert(std::is_arithmetic_v<T>)`
// (`ScalarIndexSort.h:54-55`).
//
// The two duplicate about a dozen members and diverge on the meaning of
// `idx_to_offsets_` (sorted-array offset vs unique-value index,
// `ScalarIndexSort.h:259-260` vs `StringIndexSort.h:166-167`). MERGING THEM IS
// A REAL PIECE OF WORK, NOT A RENAME, and it is not attempted here — the
// skeleton keeps them as two readers of one family and records the duplication
// rather than hiding it behind a shared name.
// ==========================================================================

namespace milvus::index {

// Numeric / bool. Was `ScalarIndexSort<T>`.
template <typename T>
class SortedIndexReader final : public IndexReaderBase,
                                public ScalarPredicateReader<T>,
                                public ScalarValueReader<T>,
                                public NullReader {
 public:
    static_assert(std::is_arithmetic_v<T>,
                  "the sorted numeric reader only handles arithmetic types; "
                  "strings use SortedStringIndexReader");

    struct OpenArgs {
        // Sorted (value, row-index) pairs. Either heap-owned or a view over
        // mapped bytes — the LOADER decides which and hands over the result;
        // the reader has no mmap branch (§3 principle 6).
        const IndexStructure<T>* data{nullptr};
        size_t size{0};
        const int32_t* idx_to_offsets{nullptr};
        size_t idx_to_offsets_size{0};
        TargetBitmap valid_bitset;
        size_t total_num_rows{0};
        bool nested{false};
    };

    explicit SortedIndexReader(OpenArgs args);

    ~SortedIndexReader() override;

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
    In(size_t n, const T* values) const override;

    TargetBitmap
    NotIn(size_t n, const T* values) const override;

    TargetBitmap
    Range(const T& value, CompareOp op) const override;

    TargetBitmap
    Range(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const override;

    std::optional<T>
    Lookup(int64_t offset) const override;

    void
    Gather(const int64_t* offsets,
           int64_t count,
           const std::function<void(int64_t i, const T*, bool valid)>& out)
        const override;

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    bool
    ShouldSkip(const T& lower, const T& upper, CompareOp op) const;

    OpenArgs data_;

    // GONE: the zero-cost iteration API `operator[]` / `begin` / `end` /
    // `rbegin` (`ScalarIndexSort.h:204-228`), and the dead accessors `GetData`
    // / `IsBuilt` (`:183-191`, zero call sites in the repo). Exposing the
    // internal sorted array is not an interface; `Gather` is (§5.5).
};

// VARCHAR. Was `StringIndexSort`.
class SortedStringIndexReader final
    : public IndexReaderBase,
      public ScalarPredicateReader<std::string_view>,
      public ScalarValueReader<std::string_view>,
      public PatternMatchReader,
      public NullReader {
 public:
    // The reader is handed a fully-parsed layout by the loader — either the
    // heap-backed `StringIndexSortMemoryImpl` shape (unique values + posting
    // lists) or the mapped `StringIndexSortMmapImpl` shape (offset arrays into
    // a byte blob). Which one is a LOAD decision, so it does not appear on any
    // query signature.
    class Layout;

    SortedStringIndexReader(std::unique_ptr<Layout> layout,
                            TargetBitmap valid_bitset,
                            std::vector<int32_t> idx_to_offsets,
                            size_t total_num_rows,
                            bool nested);

    ~SortedStringIndexReader() override;

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

    std::optional<std::string>
    Lookup(int64_t offset) const override;

    void
    Gather(const int64_t* offsets,
           int64_t count,
           const std::function<
               void(int64_t i, const std::string_view*, bool valid)>& out)
        const override;

    // Absorbs both `PatternMatch` (StringIndexSort.cpp:498-521) and the
    // separate `PrefixMatch` entry point (:492-496). Prefix is
    // `PatternOp::PrefixMatch`, not a second method — the old split existed
    // because `StringIndex::Query(DatasetPtr)` (StringIndex.h:36-44) routed
    // prefix specially out of a knowhere dataset.
    TargetBitmap
    PatternMatch(std::string_view pattern, PatternOp op) const override;

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    std::unique_ptr<Layout> layout_;
    TargetBitmap valid_bitset_;
    std::vector<int32_t> idx_to_offsets_;
    size_t total_num_rows_{0};
    bool nested_{false};
};

}  // namespace milvus::index
