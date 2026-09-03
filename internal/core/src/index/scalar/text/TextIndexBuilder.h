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

#include "index/contracts/IndexBuilder.h"
#include "storage/artifact/Artifact.h"
#include "tantivy-wrapper.h"

// The BUILDER of the text family — `IndexBuilder<std::string_view>`.
//
// See 01-scalar-index.md §6.1 (the one builder interface), §6.1.1 (input forms;
// tantivy is form **A, truly streaming**), §7 point 3 ("in-place build is NOT
// growing"), and §8 (mapping table row `TextMatchIndex`).
//
// IT ABSORBS TWO OF `TextMatchIndex`'S FOUR CONSTRUCTORS, and that merge is the
// point:
//   - `TextMatchIndex.h:44` / `TextMatchIndex.cpp:79-104` — the build service
//     (`IndexFactory.cpp:372`), which wrote into a local temp dir owned by a
//     `FileManagerContext`;
//   - `TextMatchIndex.h:38` / `TextMatchIndex.cpp:59-77` — the sealed in-place
//     build during load (`ChunkedSegmentSealedImpl.cpp:5382`), which wrote into
//     the segment's mmap dir;
//   - and the RAM-directory half of `TextMatchIndex.cpp:36-57` reached from
//     `ChunkedSegmentSealedImpl.cpp:5374` with `commit_interval = INT64_MAX,
//     enable_background_merge = false` — i.e. a sealed interim build wearing the
//     growing constructor's clothes.
//
// THE THREE DIFFERED ONLY IN WHERE THE BYTES LAND. That is now `local_dir_`
// (empty => tantivy RAM directory) plus the `storage::FileSink` handed to
// `TextIndexArtifact::Serialize` — NOT three constructors, and NOT a
// `FileManagerContext` member (§3 principle 6, §10 rule 2).
//
// The remaining half of `TextMatchIndex.cpp:36-57` — the real growing writer
// with a 200ms commit interval and background merge
// (`SegmentGrowingImpl.cpp:2496`) — is the APPENDER interface,
// `GrowingTextIndex` in index/contracts/GrowingIndex.h. It is NOT in this
// directory and is owned by the growing-side work.

namespace milvus::index {

// Family-specific build parameters. §6.1: "the tokenizer configuration of the
// text family and the path configuration of the json family are CONSTRUCTOR
// ARGUMENTS" — they never become extra methods on the builder interface.
struct TextIndexBuildParams {
    std::string analyzer_name;
    std::string analyzer_params;
    // Only the build service ever had this one (`TextMatchIndex.h:47`).
    std::string analyzer_extra_info;
    uint32_t tantivy_index_version{0};
    // Distinguishes concurrent builds sharing a directory.
    std::string unique_id;
    // Empty => build into a tantivy RAM directory (the sealed interim path).
    // Non-empty => build into this local directory (build service, or the
    // segment's mmap dir on the sealed in-place path).
    std::string local_dir;
};

class TextIndexBuilder final : public IndexBuilder<std::string_view> {
 public:
    explicit TextIndexBuilder(TextIndexBuildParams params);

    ~TextIndexBuilder() override;

    // Form A — truly streaming (§6.1.1). Text goes straight into the tantivy
    // writer slice by slice; the builder buffers nothing, so an eventual cursor
    // input really does save memory here (unlike forms B/B+).
    BuilderInputSpec
    InputSpec() const override;

    // Replaces `AddTextSealed` (TextMatchIndex.cpp:256-265), `AddNullSealed`
    // (TextMatchIndex.cpp:268-274) and the field-data loop of
    // `BuildIndexFromFieldData` (TextMatchIndex.cpp:298-338). The `valid` array
    // subsumes `AddNullSealed`: a null is a value slot with `valid[i] == false`,
    // not a separate entry point.
    //
    // NOTE the parameter is a raw array, not a column or a cursor: §6.1's
    // "the builder's input currency is a plain array — it does not know about
    // columns, cursors or storage formats", which is what makes the
    // `index -> columnar-format` edge zero on the build side.
    void
    Add(size_t n, const std::string_view* values, const bool* valid) override;

    // Commits and merges the tantivy writer, then hands over the directory.
    // `&&`-qualified: the builder is one-shot and terminates here (§3
    // principle 1, "one-shot, finished by Seal()").
    storage::ArtifactPtr
    Seal() && override;

 private:
    TextIndexBuildParams params_;

    // The engine, composed. Writer-mode wrapper; see
    // `TextMatchIndex.cpp:79-104` for how it is configured today.
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;

    // Row offsets whose value was null. Serialized as a side entry named
    // `INDEX_NULL_OFFSET_FILE_NAME` (`InvertedIndexTantivy.h:49`).
    std::vector<int64_t> null_offsets_;

    int64_t count_{0};
};

}  // namespace milvus::index
