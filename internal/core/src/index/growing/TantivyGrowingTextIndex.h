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
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "common/Types.h"
#include "index/contracts/GrowingIndex.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/TextMatchReader.h"
#include "index/growing/GrowingAppenderBase.h"
#include "index/growing/GrowingCommitPolicy.h"
#include "index/scalar/text/TextIndexReader.h"
#include "tantivy-wrapper.h"

// The growing full-text appender — THE ONE MECHANISM §7 IS EXTRACTED FROM.
//
// See core_refactor/01-scalar-index.md §7 ("growing scalar indexes are not a
// future hypothesis but an existing production fact: `TextMatchIndex`'s growing
// constructor — commit interval + background merge + `AddTextsGrowing` +
// `Commit`/`Reload` + a reader snapshot — IS a growing incremental index"), §7
// point 3, §5.3, §12.6.
//
// WHAT THIS SPLITS APART. `TextMatchIndex` has FOUR CONSTRUCTORS FOR FOUR
// LIFETIMES (`index/TextMatchIndex.h:31-52`, the §2.2 exhibit):
//
//   1. growing in-memory writer (commit interval, background merge)  -> THIS FILE
//   2. sealed, build from raw data during load                       -> Builder face
//      (`IndexBuilder<std::string_view>`'s text implementation; the caller feeds
//       it chunk by chunk from the loaded column — §7 point 3's "build-in-place
//       is a BUILDER, not another constructor branch of a growing class")
//   3. build service (`FileManagerContext`)                          -> Builder face
//   4. load an already-built index                                   -> Loader face
//
// Constructors 2 and 3 differ only in where the bytes come from, which is
// exactly the difference the shared materializer absorbs (§6.1.2).
//
// LAG POLICY (§7 semantics 2, §12.6): TEXT MATCH IS THE FAMILY EXPLICITLY
// ALLOWED TO LAG — its uncovered tail is not back-filled by a column scan. THAT
// DECISION IS NOT ENCODED HERE. The policy table lives in segcore keyed by
// family, and the index side carries no bit expressing it, "otherwise
// `ReaderCaps` starts carrying product semantics". `Family()` (from
// `GrowingAppenderBase`) is how segcore looks the row up.

namespace milvus::index {

// THE SNAPSHOT IS THE SEALED READER, REUSED — `TextIndexReader`
// (index/scalar/text/TextIndexReader.h), which already implements
// `IndexReaderBase` + `TextMatchReader` over a
// `shared_ptr<TantivyIndexWrapper>`, and whose constructor
// `(engine, count, mmap_enabled)` takes the count EXPLICITLY — exactly what a
// growing snapshot needs, since its coverage is the watermark and not the
// engine's live document count.
//
// The reuse is the point, not an economy: THE READ IMPLEMENTATION IS SHARED
// BETWEEN SEALED AND GROWING, ONLY THE WRITE FACE DIFFERS (§3 principle 1 —
// faces are cut by CALLER, not by segment state). One commit produces one new
// reader over the reloaded tantivy generation, shared by every concurrent query
// through `shared_ptr` (§4.3's growing asymmetry).
//
// !! ONE BEHAVIOURAL CHANGE COMES WITH IT, AND IT IS DELIBERATE.
// Today `TextMatchIndex::MatchQuery` and friends are NON-const and may commit
// and reload MID-QUERY (`index/TextMatchIndex.cpp:384-388`:
// "if (shouldTriggerCommit()) { Commit(); Reload(); }"), so a query can pull
// the index forward. `TextIndexReader`'s methods are `const` over an immutable
// snapshot; a query reads the last published generation and the lag shows up in
// `CommittedRows()` instead. That is §7 semantics 1 and 2 working as intended —
// the index reports a watermark, segcore decides what to do about the tail — and
// text match is the family §12.6 explicitly allows to lag.
//
// The bitset callback is bound on the ENGINE (`create_reader(SetBitsetFn)`), so
// the appender must install `SetBitsetGrowing`: on a growing segment tantivy can
// return doc ids past the end of the bitset a query allocated, which the sealed
// setter's contract says cannot happen (`index/Utils.h`).

class TantivyGrowingTextIndex final : public GrowingTextIndex,
                                      public GrowingAppenderBase {
 public:
    // Analyzer configuration is a CONSTRUCTION ARGUMENT of this family's
    // appender, not a face concern (§6.1's "the text builder's tokenizer
    // configuration is a constructor argument").
    TantivyGrowingTextIndex(const char* unique_id,
                            const char* analyzer_name,
                            const char* analyzer_params,
                            int64_t commit_interval_in_ms);

    ~TantivyGrowingTextIndex() override = default;

    // --- GrowingTextIndex (§7) ----------------------------------------------

    void
    Append(int64_t reserved_offset,
           size_t n,
           const std::string_view* values,
           const bool* valid) override;

    std::shared_ptr<const TextMatchReader>
    ReaderSnapshot() const override;

    int64_t
    CommittedRows() const override;

    // --- GrowingAppenderBase ------------------------------------------------

    DataType
    ValueType() const override;

    std::string
    Family() const override;

    IndexReaderBasePtr
    ReaderSnapshotErased() const override;

 private:
    void
    CommitAndPublish();

    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> wrapper_;
    GrowingCommitPolicy commit_policy_;

    mutable std::mutex mtx_;
    std::shared_ptr<const TextIndexReader> snapshot_;
    std::vector<int64_t> null_offsets_;
};

}  // namespace milvus::index
