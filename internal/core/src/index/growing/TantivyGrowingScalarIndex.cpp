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

#include "index/growing/TantivyGrowingScalarIndex.h"

// SKELETON. Line references are to the pre-W1 tree (master e255009e01).
//
// !! See the out-of-bounds warning at the top of the header (§13.1) before
// landing any of this.

namespace milvus::index {

template <typename T>
TantivyGrowingScalarIndex<T>::TantivyGrowingScalarIndex(
    DataType value_type,
    std::string family,
    const char* unique_id,
    int64_t commit_interval_in_ms)
    : value_type_(value_type),
      family_(std::move(family)),
      commit_policy_(commit_interval_in_ms) {
    // TODO: construct the wrapper in GROWING mode, as `TextMatchIndex`'s growing
    // ctor does (`index/TextMatchIndex.cpp:36-57`): in-memory writer,
    // TANTIVY_INDEX_LATEST_VERSION, background merge ENABLED (its comment:
    // background merge must be true only for a long-lived growing segment, since
    // periodic commits would otherwise grow the segment count without bound),
    // then `set_is_growing(true)` and `create_reader(SetBitsetGrowing)`.
    //
    // The growing bitset setter is not cosmetic — see check 3 in the header.
}

template <typename T>
void
TantivyGrowingScalarIndex<T>::Append(int64_t reserved_offset,
                                     size_t n,
                                     const T* values,
                                     const bool* valid) {
    // TODO: move existing logic here (see TextMatchIndex.cpp:277-295
    // `AddTextsGrowing`, the same shape for a different value type):
    //   - record null rows into `null_offsets_` under the lock
    //   - `wrapper_->add_data(values, n, reserved_offset)`
    //   - `commit_policy_.NoteAppended(n)`
    //   - if `commit_policy_.ShouldCommit()` -> `CommitAndPublish()`
    //
    // WHAT IS NEW RELATIVE TO TODAY: the watermark. `AddTextsGrowing` commits and
    // forgets; §7 requires the appender to publish how far the committed
    // snapshot reaches.
}

template <typename T>
void
TantivyGrowingScalarIndex<T>::CommitAndPublish() {
    // TODO: move existing logic here (see TextMatchIndex.cpp:353-370 `Commit` /
    // `Reload`), then build an `InvertedIndexReader<T>` over the reloaded
    // generation, swap it in under `mtx_`, and
    // `commit_policy_.NoteCommitted(rows_covered)`.
    //
    // ORDERING IS THE WHOLE OF §7 SEMANTICS 1: publish the watermark only AFTER
    // the snapshot is visible, and never publish a smaller one. A failed commit
    // publishes nothing and leaves the previous snapshot current — segcore's
    // fallback (§7 semantics 2) then covers a longer tail, which is a
    // performance loss and not a correctness one.
}

template <typename T>
std::shared_ptr<const ScalarPredicateReader<T>>
TantivyGrowingScalarIndex<T>::ReaderSnapshot() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return snapshot_;
}

template <typename T>
int64_t
TantivyGrowingScalarIndex<T>::CommittedRows() const {
    return commit_policy_.CommittedRows();
}

template <typename T>
DataType
TantivyGrowingScalarIndex<T>::ValueType() const {
    return value_type_;
}

template <typename T>
std::string
TantivyGrowingScalarIndex<T>::Family() const {
    return family_;
}

template <typename T>
IndexReaderBasePtr
TantivyGrowingScalarIndex<T>::ReaderSnapshotErased() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return std::const_pointer_cast<IndexReaderBase>(
        std::static_pointer_cast<const IndexReaderBase>(snapshot_));
}

// The scalar value types a growing inverted index can serve. `std::string` is
// the owned type here because the APPEND side takes ownership of the bytes it
// hands to tantivy; §5.1's `string_view` decision covers the QUERY side's inputs
// only.
template class TantivyGrowingScalarIndex<bool>;
template class TantivyGrowingScalarIndex<int8_t>;
template class TantivyGrowingScalarIndex<int16_t>;
template class TantivyGrowingScalarIndex<int32_t>;
template class TantivyGrowingScalarIndex<int64_t>;
template class TantivyGrowingScalarIndex<float>;
template class TantivyGrowingScalarIndex<double>;
template class TantivyGrowingScalarIndex<std::string>;

}  // namespace milvus::index
