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

#include <marisa.h>

#include "common/Types.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/NullReader.h"
#include "index/contracts/PatternMatchReader.h"
#include "index/contracts/ScalarPredicateReader.h"
#include "index/contracts/ScalarValueReader.h"

// The READER of the marisa (trie) family.
//
// See 01-scalar-index.md §5.1, §5.2, §5.5 and §8.
//
// INTERFACES: `ScalarPredicateReader<string_view>` + `PatternMatchReader` +
// `ScalarValueReader<string_view>` + `NullReader`.
//
// §5.2 describes marisa's pattern contribution as "prefix". The code does more:
// `StringIndexMarisa::PatternMatch` (`.cpp:640-716`) handles the LIKE family
// and rejects only what it cannot serve (`ThrowInfo(Unsupported)` at :651).
// The interface is the whole `PatternMatchReader`; which operators are cheap is
// an implementation matter.
//
// MARISA IS THE ONE FAMILY WITH NO NESTED MODE — it takes no
// `is_nested_index` constructor argument (`StringIndexMarisa.h:31-33`) and
// appears in none of the nested factory paths, so `CoordDomain()` is always
// `Row`. That is a fact about the family, not an omission.

namespace milvus::index {

// Owns the loader-created mappings and their local staging files. marisa owns
// the trie mapping itself; this object owns the separate row-id/CSR mappings
// and keeps every backing path alive until the reader is destroyed.
class MarisaMmapOwner final {
 public:
    MarisaMmapOwner(char* str_ids_data,
                    size_t str_ids_bytes,
                    char* csr_data,
                    size_t csr_bytes,
                    std::vector<std::string> paths,
                    std::string staging_dir);

    ~MarisaMmapOwner();

    MarisaMmapOwner(const MarisaMmapOwner&) = delete;
    MarisaMmapOwner&
    operator=(const MarisaMmapOwner&) = delete;

    const int64_t*
    StrIds() const;

    const uint32_t*
    Csr() const;

 private:
    char* str_ids_data_{nullptr};
    size_t str_ids_bytes_{0};
    char* csr_data_{nullptr};
    size_t csr_bytes_{0};
    std::vector<std::string> paths_;
    std::string staging_dir_;
};

// Immutable owner/view bundle shared by a reader and a rewrite artifact. Owner
// fields precede the trie so the trie is destroyed before mapped paths are
// removed by MarisaMmapOwner.
struct MarisaIndexStorage final {
    std::shared_ptr<MarisaMmapOwner> mmap_owner;
    std::shared_ptr<const std::vector<int64_t>> str_ids_owner;
    std::shared_ptr<const std::vector<uint32_t>> csr_index_owner;
    std::shared_ptr<const std::vector<uint32_t>> csr_offsets_owner;
    std::shared_ptr<const marisa::Trie> trie;
    // row -> trie key id. Heap-owned or a view over mapped bytes.
    const int64_t* str_ids{nullptr};
    size_t str_ids_size{0};
    // CSR: key id -> the rows holding it.
    const uint32_t* csr_index{nullptr};
    const uint32_t* csr_offsets{nullptr};
    size_t csr_num_keys{0};
    DataType value_type{DataType::VARCHAR};
    // Exact bytes owned through mmap-backed staging files.
    size_t file_backed_bytes{0};
};

class MarisaIndexReader final : public IndexReaderBase,
                                public ScalarPredicateReader<std::string_view>,
                                public ScalarValueReader<std::string_view>,
                                public PatternMatchReader,
                                public NullReader {
 public:
    explicit MarisaIndexReader(
        std::shared_ptr<const MarisaIndexStorage> storage);

    ~MarisaIndexReader() override;

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

    // THE RETURN TYPE IS OWNING, AND MARISA IS THE REASON §5.5 SAYS SO.
    // `StringIndexMarisa::Reverse_Lookup` (`.cpp:799-811`) ends in
    // `std::string(agent.key().ptr(), agent.key().length())`: the bytes live in
    // a `marisa::Agent` local to the call. A trie stores compressed, so the
    // original value is RECONSTRUCTED ON DEMAND — there is no resident buffer
    // to point a view at, and a returned view would always dangle. Every trie
    // or compressed family has this shape.
    std::optional<std::string>
    Lookup(int64_t offset) const override;

    // `Gather` may hand out views: the implementation can keep its agent alive
    // until the callback returns (§5.5).
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
    size_t
    LookupKeyId(std::string_view value) const;

    std::vector<size_t>
    PrefixMatchKeyIds(std::string_view prefix) const;

    // marisa's key order is not always lexicographic; the range paths take a
    // fast or a slow route depending on this (`StringIndexMarisa.cpp:813-823`).
    bool
    InLexicographicOrder() const;

    std::shared_ptr<const MarisaIndexStorage> storage_;
};

}  // namespace milvus::index
