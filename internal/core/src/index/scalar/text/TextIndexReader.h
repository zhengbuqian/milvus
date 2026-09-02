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

#include <cstdint>
#include <memory>
#include <string_view>

#include "common/Types.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/TextMatchReader.h"
#include "tantivy-wrapper.h"

// The sealed-side READER of the text family.
//
// See 01-scalar-index.md §5.3 (`TextMatchReader`), §8 (mapping table row
// `TextMatchIndex`), and §3 principle 2 (composition over inheritance).
//
// ==========================================================================
// THIS CLASS IS ONE QUARTER OF TODAY'S `TextMatchIndex`.
//
// `TextMatchIndex` had FOUR CONSTRUCTORS FOR FOUR LIFECYCLES
// (`TextMatchIndex.h:31-50`, §2.2 row 2 — the headline pathology of this wave):
//
//   TextMatchIndex.h:32  (commit_interval, unique_id, analyzer..., bg_merge)
//        growing in-memory writer  -> GrowingTextIndex  (contracts/GrowingIndex.h)
//                                     lives in segcore's growing index set;
//                                     NOT in this directory, and NOT this class.
//   TextMatchIndex.h:38  (path, unique_id, tantivy_version, analyzer...)
//        sealed in-place build at load time -> TextIndexBuilder (this dir).
//        §7 point 3: "in-place build != growing" — it is the Builder face,
//        fed chunk by chunk from an already-loaded column by the caller.
//   TextMatchIndex.h:44  (FileManagerContext, tantivy_version, analyzer...)
//        the index-build service       -> TextIndexBuilder (this dir),
//                                         the very same class: the two build
//                                         paths differed only in WHERE the
//                                         bytes went, which is now the
//                                         `storage::FileSink` handed to
//                                         `TextIndexArtifact::Serialize`.
//   TextMatchIndex.h:50  (FileManagerContext)
//        load an already-built index   -> TextIndexLoader (this dir).
//
// Once the four are split, the "which constructor am I" question disappears:
// each face has exactly one way to come into existence.
// ==========================================================================
//
// COMPOSITION, NOT INHERITANCE (§3 principle 2, §10 rule 3).
// Today the chain is `TextMatchIndex : InvertedIndexTantivy<std::string> :
// ScalarIndex<std::string> : IndexBase`, which drags `In` / `NotIn` / `Range` /
// `Reverse_Lookup` / `Query(DatasetPtr)` onto a full-text index that must never
// answer them. Here the tantivy reader snapshot is a MEMBER. The repo's own
// positive example is `json_stats/bson_inverted.h:42` (`BsonInvertedIndex`
// holds a `shared_ptr<TantivyIndexWrapper>` and inherits nothing).
//
// FACES: `TextMatchReader` only.
// Deliberately NOT `NullReader`: `IS NULL` on a VARCHAR field routes to that
// field's SCALAR index, not to its text index (the text index is a separate
// artifact under the `text_log/` prefix and is only reachable through the
// text-match expressions). If a null predicate ever needs to be answered from
// the text artifact, add the face here — do not add a throwing stub (§10 rule 4).

namespace milvus::index {

class TextIndexReader final : public IndexReaderBase, public TextMatchReader {
 public:
    // Produced by exactly two call sites: `TextIndexLoader::OpenIndex` (from
    // bytes) and `TextIndexArtifact::OpenReader` (in place, right after a
    // build). §6 states the pair explicitly. There is no other way to get one,
    // and in particular no constructor that also knows how to build.
    TextIndexReader(std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
                    int64_t count,
                    bool mmap_enabled);

    ~TextIndexReader() override;

    TextIndexReader(const TextIndexReader&) = delete;
    TextIndexReader&
    operator=(const TextIndexReader&) = delete;

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

    // ---- storage::LoadedArtifact (§11.2 rule 1) ------------------------

    ResourceUsage
    CellByteSize() const override;

    // ---- TextMatchReader (§5.3) ----------------------------------------

    TargetBitmap
    MatchQuery(std::string_view query,
               uint32_t min_should_match) const override;

    TargetBitmap
    PhraseMatchQuery(std::string_view query, uint32_t slop) const override;

    TargetBitmap
    FuzzyMatchQuery(std::string_view query,
                    uint32_t max_edit_distance) const override;

 private:
    // Allocates the result bitmap and installs the tantivy set-bit callback.
    TargetBitmap
    PrepareBitset() const;

    // The ENGINE, held by composition. Immutable after construction: §5's
    // "every reader is immutable once produced by Seal()/Open(), therefore
    // thread-safe and lock-free for concurrent reads". Note what is gone with
    // the growing half: `mtx_`, `last_commit_time_`, `commit_interval_in_ms_`
    // (`TextMatchIndex.h:114-116`) belong to the appender, not here.
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;

    int64_t count_{0};
    bool mmap_enabled_{false};
};

}  // namespace milvus::index
