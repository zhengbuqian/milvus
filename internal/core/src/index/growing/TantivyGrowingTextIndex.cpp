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

#include "index/growing/TantivyGrowingTextIndex.h"

// SKELETON. Line references are to the tree before refactor phase 1 (master
// e255009e01).

namespace milvus::index {

TantivyGrowingTextIndex::TantivyGrowingTextIndex(const char* unique_id,
                                                 const char* analyzer_name,
                                                 const char* analyzer_params,
                                                 int64_t commit_interval_in_ms)
    : commit_policy_(commit_interval_in_ms) {
    // TODO: move existing logic here (see TextMatchIndex.cpp:36-57) — the
    // growing ctor, background merge enabled, `set_is_growing(true)`, plus
    // `CreateReader(SetBitsetGrowing)` which segcore does separately today
    // (`SegmentGrowingImpl.cpp:2502-2503`).
}

void
TantivyGrowingTextIndex::Append(int64_t reserved_offset,
                                size_t n,
                                const std::string_view* values,
                                const bool* valid) {
    // TODO: move existing logic here (see TextMatchIndex.cpp:277-295
    // `AddTextsGrowing`). Callers today: `SegmentGrowingImpl.cpp:2461,2600,2696`.
}

void
TantivyGrowingTextIndex::CommitAndPublish() {
    // TODO: move existing logic here (see TextMatchIndex.cpp:353-370), then
    // construct a `TextIndexReader(wrapper_, covered_rows, /*mmap=*/false)` and
    // swap it in under `mtx_`. segcore
    // calls `Commit()` + `Reload()` explicitly in three places today
    // (`SegmentGrowingImpl.cpp:1069-1071,2470-2472,2699-2700`); after §7 those
    // become "ask the appender to publish", and the watermark is what the query
    // side reads instead of guessing.
}

std::shared_ptr<const TextMatchReader>
TantivyGrowingTextIndex::ReaderSnapshot() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return snapshot_;
}

int64_t
TantivyGrowingTextIndex::CommittedRows() const {
    return commit_policy_.CommittedRows();
}

DataType
TantivyGrowingTextIndex::ValueType() const {
    return DataType::VARCHAR;
}

std::string
TantivyGrowingTextIndex::Family() const {
    // `families::kText` (index/Families.h) — the key segcore's §12.6 lag-policy
    // table looks up, and the only family in it allowed to lag.
    return "text";
}

IndexReaderBasePtr
TantivyGrowingTextIndex::ReaderSnapshotErased() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return std::const_pointer_cast<IndexReaderBase>(
        std::static_pointer_cast<const IndexReaderBase>(snapshot_));
}

}  // namespace milvus::index
