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
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/Types.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/NullReader.h"
#include "index/contracts/PatternMatchReader.h"

// The READER of the FM-index family.
//
// See 01-scalar-index.md §5.2 (`PatternMatchReader`) and §8:
//   | `FMIndex` | `PatternMatchReader` **this predicate interface only** |
//
// Today `FMIndex : ScalarIndex<std::string>` carries about twenty methods it
// cannot answer; SIX OF THEM ARE PURE THROW SHELLS
// (`FMIndex.h:82,91,100,148,156,268` and `FMIndex.cpp:379,386`). They are not
// re-declared here — §3 principle 3 and §10 rule 4: an unsupported operation
// must not exist on the type.
//
// INTERFACES: `PatternMatchReader` + `NullReader`. The null interface is real,
// not a courtesy: `FMIndex.cpp:391-403` implements `IsNull`/`IsNotNull` from
// `null_bitmap_`, and §5's cross-family note lists FMIndex among the seven
// scalar families that all really implement it.

namespace milvus::index {

namespace fmindex {
class FMIndex;
}

// Owns the family-private mmap and its unique staging directory. The engine
// stores views into Data(), so readers retain this object for their lifetime.
class FmIndexMappedFile final {
 public:
    FmIndexMappedFile(void* data,
                      size_t mapped_bytes,
                      std::string staging_directory);
    FmIndexMappedFile(const FmIndexMappedFile&) = delete;
    FmIndexMappedFile&
    operator=(const FmIndexMappedFile&) = delete;
    ~FmIndexMappedFile();

    const uint8_t*
    Data() const;

    size_t
    MappedBytes() const;

    size_t
    HeapBytes() const;

 private:
    void* data_{nullptr};
    size_t mapped_bytes_{0};
    std::string staging_directory_;
};

enum class FmIndexStateOrigin {
    Builder,
    Persisted,
};

// Immutable owner/state bundle shared by readers and a rewrite artifact.
// mapped_file precedes engine so the FM-index view is destroyed before its
// backing mapping is unmapped.
class FmIndexStorage final {
 public:
    static std::shared_ptr<const FmIndexStorage>
    Create(std::shared_ptr<const FmIndexMappedFile> mapped_file,
           std::shared_ptr<const fmindex::FMIndex> engine,
           TargetBitmap null_bitmap,
           int64_t total_rows,
           DataType value_type,
           bool nullable,
           FmIndexStateOrigin origin);

    const fmindex::FMIndex&
    Engine() const;

    const TargetBitmap&
    NullBitmap() const;

    int64_t
    Count() const;

    int64_t
    TotalTokens() const;

    DataType
    ValueType() const;

    bool
    Nullable() const;

    int64_t
    MemoryUsage() const;

    int64_t
    FileBytes() const;

 private:
    FmIndexStorage(std::shared_ptr<const FmIndexMappedFile> mapped_file,
                   std::shared_ptr<const fmindex::FMIndex> engine,
                   TargetBitmap null_bitmap,
                   int64_t total_rows,
                   int64_t total_tokens,
                   DataType value_type,
                   bool nullable,
                   int64_t memory_usage,
                   int64_t file_bytes);

    std::shared_ptr<const FmIndexMappedFile> mapped_file_;
    std::shared_ptr<const fmindex::FMIndex> engine_;
    TargetBitmap null_bitmap_;
    int64_t total_rows_{0};
    int64_t total_tokens_{0};
    DataType value_type_{DataType::VARCHAR};
    bool nullable_{false};
    int64_t memory_usage_{0};
    int64_t file_bytes_{0};
};

class FmIndexReader final : public IndexReaderBase,
                            public PatternMatchReader,
                            public NullReader {
 public:
    // `cost_ratio` — see `ShouldUseForOp` below. Injected rather than read from a
    // global (§8's mapping-table note for this row: "the `SegcoreConfig`
    // dependency becomes a constructor parameter").
    FmIndexReader(std::shared_ptr<const FmIndexStorage> storage,
                  double cost_ratio = 0.001);

    ~FmIndexReader() override;

    // ---- IndexReaderBase (§4.2) ----------------------------------------

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

    // ---- PatternMatchReader (§5.2) -------------------------------------

    // Only PrefixMatch / PostfixMatch / InnerMatch ever arrive; `Match` and
    // `RegexMatch` are declined by `ShouldUseForOp` below.
    TargetBitmap
    PatternMatch(std::string_view pattern, PatternOp op) const override;

    bool
    ShouldUseForOp(PatternOp op, std::string_view pattern) const override;

    // ---- NullReader (§5) -----------------------------------------------

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    // O(|pattern|) occurrence count; -1 means "unknown, accept".
    int64_t
    PatternCount(std::string_view pattern, PatternOp op) const;

    TargetBitmap
    DocsToBitmap(const std::vector<uint64_t>& docs) const;

    std::shared_ptr<const FmIndexStorage> storage_;

    // WAS: `segcore::SegcoreConfig::default_config().get_fmindex_cost_ratio()`
    // read inline at `FMIndex.h:226-228`, the single `index/ -> segcore/`
    // header edge in the scalar tree (§2.2 row 6, §10 rule 1).
    //
    // BEHAVIOUR DELTA TO DECIDE WHEN THE LOGIC MOVES: the old read went through
    // a process-wide static (`SegcoreConfig.h:265`), so a live config change to
    // `queryNode.fmindexCostRatio` took effect on the next query. A value
    // copied at construction freezes it for the lifetime of the loaded index.
    // If live updates must be preserved, inject a policy callback instead of a
    // double — but make that an explicit choice, not an accident of the move.
    double cost_ratio_{0.001};
};

}  // namespace milvus::index
