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

#include "index/scalar/inverted/InvertedIndexReader.h"

#include <utility>

namespace milvus::index {

template <typename T>
InvertedIndexReader<T>::InvertedIndexReader(
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::vector<size_t> null_offsets,
    bool is_nested_index)
    : engine_(std::move(engine)),
      null_offsets_(std::move(null_offsets)),
      is_nested_index_(is_nested_index) {
}

template <typename T>
InvertedIndexReader<T>::~InvertedIndexReader() {
    // NOTE: today's destructor (InvertedIndexTantivy.cpp:116-129) also does
    // `LocalChunkManagerSingleton::RemoveDir(path_)`. Owning the local scratch
    // directory is the LOADER's business (it is what materialized the files),
    // not the reader's; §10 rule 2 keeps storage out of reader members.
}

template <typename T>
ReaderCaps
InvertedIndexReader<T>::Caps() const {
    // `pattern_match` is true only for the string instantiation — today's
    // `SupportPatternMatch()` is `std::is_same_v<T, std::string>`
    // (InvertedIndexTantivy.h:285-288). Must agree with
    // InvertedIndexLoader::DeriveCaps, which derives the same thing from the
    // persisted value type without opening the index (§4.1).
    return ReaderCaps{};
}

template <typename T>
Domain
InvertedIndexReader<T>::CoordDomain() const {
    return is_nested_index_ ? Domain::Element : Domain::Row;
}

template <typename T>
int64_t
InvertedIndexReader<T>::Count() const {
    // TODO: move existing logic here (see InvertedIndexTantivy.h:145-148 —
    // `wrapper_->count()`).
}

template <typename T>
DataType
InvertedIndexReader<T>::ValueType() const {
    // TODO: derive from T. Today this was implicit in the tantivy data type
    // (`get_tantivy_data_type`, InvertedIndexTantivy.h:51-87) and in the
    // `proto::schema::FieldSchema schema_` member (:368) — a proto object held
    // as index state, which the contract replaces with `DataType`.
}

template <typename T>
int64_t
InvertedIndexReader<T>::MemoryUsage() const {
    // TODO: move existing logic here (see InvertedIndexTantivy.h:219-231
    // ComputeByteSize: `wrapper_->index_size_bytes()` + null-offset capacity).
    //
    // WHILE MOVING, note the inconsistency this replaces: `ComputeByteSize()`
    // is called from `Load` (InvertedIndexTantivy.cpp:252),
    // `BuildWithRawDataForUT` (:641) and `LoadEntries` (:958) — but NOT from
    // `Build`, `BuildWithFieldData`, `Upload`, `TextMatchIndex::Load`'s V2
    // branch, or `NgramInvertedIndex::Load`. A reader that computes its size at
    // construction cannot have a stale one.
}

template <typename T>
ResourceUsage
InvertedIndexReader<T>::CellByteSize() const {
    // See §12.3: this family reports the uncompressed FILE size through the
    // translator, while text/FM report measured memory. Unified elsewhere.
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::In(size_t n, const T* values) const {
    // TODO: move existing logic here (see InvertedIndexTantivy.cpp:338-345).
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::NotIn(size_t n, const T* values) const {
    // TODO: move existing logic here (see InvertedIndexTantivy.cpp:438-464).
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::Range(const T& value, CompareOp op) const {
    // TODO: move existing logic here (see InvertedIndexTantivy.cpp:466-491),
    // switching on the native CompareOp instead of proto::plan::OpType.
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::Range(const T& lo, bool lo_inc, const T& hi,
                              bool hi_inc) const {
    // TODO: move existing logic here (see InvertedIndexTantivy.cpp:493-508).
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::PatternMatch(std::string_view pattern,
                                     PatternOp op) const {
    // TODO: move existing logic here (see InvertedIndexTantivy.h:244-283) —
    // the dispatch over prefix / postfix / inner / match / regex, including the
    // `PartialRegexMatcher` + C callback branch at :261-276, and the
    // `PrefixMatch` implementation at InvertedIndexTantivy.cpp:510-519.
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::PatternQuery(std::string_view pattern) const {
    // TODO: move existing logic here (see InvertedIndexTantivy.cpp:539-549).
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::IsNull() const {
    // TODO: move existing logic here (see InvertedIndexTantivy.cpp:347-378),
    // minus the `is_growing_` lock branch at :370.
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::IsNotNull() const {
    // TODO: move existing logic here (see InvertedIndexTantivy.cpp:380-411),
    // minus the lock branch at :403.
}

// GONE, NOT MOVED — deleted surface, with the paragraph of the design that
// removes each:
//   `InApplyFilter`  (InvertedIndexTantivy.cpp:413-424) — §5.1: zero production
//       call sites; the only reference in the tree is JsonFlatIndexTest.cpp:799.
//   `InApplyCallback` (:426-436) — §5.1: one consumer
//       (`PhyUnaryRangeFilterExpr::ExecArrayEqualForIndex`, UnaryExpr.cpp:804),
//       and the implementation materializes the full bitmap anyway before
//       walking it, so the "avoid materializing a bitmap" rationale is void.
//       exec does the same job with `In()` plus a bitmap intersection.
//       Both took `index/InvertedIndexUtil.h`'s `apply_hits_with_filter` /
//       `apply_hits_with_callback`, whose only callers these were — that header
//       is deleted with them.
//   `Query(const DatasetPtr&)` (:521-537) — §5.1: the knowhere-style universal
//       entry point; typed faces replace it.
//   `Reverse_Lookup` (InvertedIndexTantivy.h:209-212) — a NotImplemented shell.
//   `ShouldUseOp` (:290-308) — this family's override ignores the literal
//       entirely and answers on the op alone, i.e. it is static per index, so
//       it is `ReaderCaps::pattern_match` and nothing more. (FMIndex's override
//       is the one that genuinely needs a per-call gate; see FmIndexReader.h.)

#define INSTANTIATE_INVERTED_READER(T) template class InvertedIndexReader<T>;
INSTANTIATE_INVERTED_READER(bool)
INSTANTIATE_INVERTED_READER(int8_t)
INSTANTIATE_INVERTED_READER(int16_t)
INSTANTIATE_INVERTED_READER(int32_t)
INSTANTIATE_INVERTED_READER(int64_t)
INSTANTIATE_INVERTED_READER(float)
INSTANTIATE_INVERTED_READER(double)
INSTANTIATE_INVERTED_READER(std::string_view)
#undef INSTANTIATE_INVERTED_READER

}  // namespace milvus::index
