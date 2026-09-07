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
#include <memory>
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

// Numeric and string readers remain separate because their persisted layouts
// and reverse-offset semantics differ.

namespace milvus::index {

// Owns one loader-created local file and its read-only mapping. Readers retain
// this object through type-erased/shared layout ownership; destruction unmaps
// before unlinking the file.
class SortedMmapOwner final {
 public:
    SortedMmapOwner(char* data,
                    size_t mapped_size,
                    size_t logical_size,
                    std::string path);
    ~SortedMmapOwner();

    SortedMmapOwner(const SortedMmapOwner&) = delete;
    SortedMmapOwner&
    operator=(const SortedMmapOwner&) = delete;

    const uint8_t*
    Data() const;

    size_t
    MappedSize() const;

    size_t
    LogicalSize() const;

 private:
    char* data_{nullptr};
    size_t mapped_size_{0};
    size_t logical_size_{0};
    std::string path_;
};

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
        std::shared_ptr<const void> data_owner;
        size_t data_heap_bytes{0};
        size_t data_file_bytes{0};
        const int32_t* idx_to_offsets{nullptr};
        size_t idx_to_offsets_size{0};
        std::shared_ptr<const void> idx_to_offsets_owner;
        size_t idx_to_offsets_heap_bytes{0};
        size_t idx_to_offsets_file_bytes{0};
        std::shared_ptr<const TargetBitmap> valid_bitset;
        size_t total_num_rows{0};
        DataType value_type{DataType::NONE};
        bool nested{false};
        bool value_lookup{true};
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

    cachinglayer::ResourceUsage
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
};

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

    struct PostingView {
        const uint8_t* data{nullptr};
        size_t size{0};

        uint32_t
        At(size_t index) const;
    };

    class Layout {
     public:
        virtual ~Layout() = default;

        virtual size_t
        UniqueCount() const = 0;

        virtual std::string_view
        Value(size_t index) const = 0;

        virtual PostingView
        Posting(size_t index) const = 0;

        virtual int64_t
        MemoryUsage() const = 0;

        virtual int64_t
        FileUsage() const = 0;

        std::vector<int32_t>
        BuildOffsets(size_t total_num_rows) const;

        static std::shared_ptr<const Layout>
        FromHeap(std::vector<std::string> unique_values,
                 std::vector<std::vector<uint32_t>> posting_lists,
                 size_t total_num_rows);

        static std::shared_ptr<const Layout>
        FromPackedHeap(std::vector<uint8_t> packed, size_t total_num_rows);

        static std::shared_ptr<const Layout>
        FromPackedMmap(std::shared_ptr<SortedMmapOwner> owner,
                       size_t total_num_rows);
    };

    struct OpenArgs {
        std::shared_ptr<const Layout> layout;
        std::shared_ptr<const TargetBitmap> valid_bitset;
        const int32_t* idx_to_offsets{nullptr};
        size_t idx_to_offsets_size{0};
        std::shared_ptr<const void> idx_to_offsets_owner;
        size_t idx_to_offsets_heap_bytes{0};
        size_t idx_to_offsets_file_bytes{0};
        size_t total_num_rows{0};
        DataType value_type{DataType::VARCHAR};
        bool nested{false};
        bool value_lookup{true};
    };

    explicit SortedStringIndexReader(OpenArgs args);

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

    cachinglayer::ResourceUsage
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

    TargetBitmap
    PatternMatch(std::string_view pattern, PatternOp op) const override;

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    std::shared_ptr<const Layout> layout_;
    std::shared_ptr<const TargetBitmap> valid_bitset_;
    const int32_t* idx_to_offsets_{nullptr};
    size_t idx_to_offsets_size_{0};
    std::shared_ptr<const void> idx_to_offsets_owner_;
    size_t idx_to_offsets_heap_bytes_{0};
    size_t idx_to_offsets_file_bytes_{0};
    size_t total_num_rows_{0};
    DataType value_type_{DataType::VARCHAR};
    bool nested_{false};
    bool value_lookup_{true};
};

}  // namespace milvus::index
