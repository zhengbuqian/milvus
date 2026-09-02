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

#include "index/scalar/json/JsonFlatIndexReader.h"

#include <utility>

namespace milvus::index {

template <typename T>
JsonPathPredicateReader<T>::JsonPathPredicateReader(
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::string tantivy_path,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    bool comparable_value_mask)
    : engine_(std::move(engine)),
      tantivy_path_(std::move(tantivy_path)),
      null_offsets_(std::move(null_offsets)),
      comparable_value_mask_(comparable_value_mask) {
}

template <typename T>
JsonPathPredicateReader<T>::~JsonPathPredicateReader() = default;

template <typename T>
ReaderCaps
JsonPathPredicateReader<T>::Caps() const {
    return ReaderCaps{.predicate = true};
}

template <typename T>
Domain
JsonPathPredicateReader<T>::CoordDomain() const {
    return Domain::Row;
}

template <typename T>
int64_t
JsonPathPredicateReader<T>::Count() const {
    // TODO: `engine_->count()`.
}

template <typename T>
DataType
JsonPathPredicateReader<T>::ValueType() const {
    // TODO: derive from T.
}

template <typename T>
int64_t
JsonPathPredicateReader<T>::MemoryUsage() const {
    // A path reader BORROWS the field-level engine; it owns no bytes of its
    // own, so it must not report the engine's size or the same bytes would be
    // counted once per resolved path.
    return 0;
}

template <typename T>
ResourceUsage
JsonPathPredicateReader<T>::CellByteSize() const {
    // Same reason as MemoryUsage: accounting belongs to the owning
    // JsonFlatIndexReader.
    return {};
}

template <typename T>
TargetBitmap
JsonPathPredicateReader<T>::In(size_t n, const T* values) const {
    // TODO: move existing logic here (see JsonFlatIndex.h:44-50 plus
    // TermBitset :224-230 and the u64 widening at :232-259).
}

template <typename T>
TargetBitmap
JsonPathPredicateReader<T>::NotIn(size_t n, const T* values) const {
    // TODO: move existing logic here (see JsonFlatIndex.h:95-108).
}

template <typename T>
TargetBitmap
JsonPathPredicateReader<T>::Range(const T& value, CompareOp op) const {
    // TODO: move existing logic here (see JsonFlatIndex.h:127-157 and the range
    // derivation helpers :457-707).
    //
    // Four of those helpers end in `ThrowInfo(OpTypeInvalid)` default arms
    // (JsonFlatIndex.h:150, :322, :505, :590) reached only if an operator
    // outside the comparison family arrives. With `CompareOp` those arms become
    // exhaustive switches over a six-value native enum — a compiler warning
    // instead of a runtime throw.
}

template <typename T>
TargetBitmap
JsonPathPredicateReader<T>::Range(const T& lo, bool lo_inc, const T& hi,
                                  bool hi_inc) const {
    // TODO: move existing logic here (see JsonFlatIndex.h:164-196).
}

template <typename T>
TargetBitmap
JsonPathPredicateReader<T>::PatternMatch(std::string_view pattern,
                                         PatternOp op) const {
    // TODO: move existing logic here (see JsonFlatIndex.h:198-206 PrefixMatch
    // and :209-218 PatternQuery -> `json_regex_query`).
}

template <typename T>
TargetBitmap
JsonPathPredicateReader<T>::IsNull() const {
    // TODO: move existing logic here (see JsonFlatIndex.h:110-115).
}

template <typename T>
TargetBitmap
JsonPathPredicateReader<T>::IsNotNull() const {
    // TODO: move existing logic here (see JsonFlatIndex.h:117-125 and
    // ComparableValueBitset :709-722).
}

template <typename T>
TargetBitmap
JsonPathPredicateReader<T>::TermBitset(size_t n, const T* values) const {
    // TODO: move existing logic here (see JsonFlatIndex.h:224-230).
}

template <typename T>
TargetBitmap
JsonPathPredicateReader<T>::ComparableValueBitset() const {
    // TODO: move existing logic here (see JsonFlatIndex.h:709-722).
}

// DELETED, NOT MOVED (§5.1): `InApplyFilter` (JsonFlatIndex.h:72-82) and
// `InApplyCallback` (:84-93). The former's only reference in the entire tree is
// `JsonFlatIndexTest.cpp:799`. Also deleted: `Query(const DatasetPtr&)` (:159-162).

JsonFlatIndexReader::JsonFlatIndexReader(
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::string field_path_prefix,
    std::vector<size_t> null_offsets)
    : engine_(std::move(engine)),
      field_path_prefix_(std::move(field_path_prefix)),
      null_offsets_(std::make_shared<const std::vector<size_t>>(
          std::move(null_offsets))) {
}

JsonFlatIndexReader::~JsonFlatIndexReader() = default;

ReaderCaps
JsonFlatIndexReader::Caps() const {
    // §5.7 narrows what this bit means: "this index object is addressed by
    // path". It no longer hints that a shredded sub-column exists — that is the
    // COLUMN's self-description now (§4.1, §5.7).
    return ReaderCaps{.json_paths = true};
}

Domain
JsonFlatIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
JsonFlatIndexReader::Count() const {
    // TODO: `engine_->count()`.
}

DataType
JsonFlatIndexReader::ValueType() const {
    return DataType::JSON;
}

int64_t
JsonFlatIndexReader::MemoryUsage() const {
    // TODO: engine index size + null offsets. This object owns the engine, so
    // it is the one that reports it.
}

ResourceUsage
JsonFlatIndexReader::CellByteSize() const {
    // See §12.3.
}

std::shared_ptr<const IndexReaderBase>
JsonFlatIndexReader::Resolve(std::string_view path,
                             JsonCastType cast_type) const {
    // TODO: move existing logic here (see JsonFlatIndex.h:752-764
    // create_executor): rewrite the JSON-pointer path `/a/b/c` into tantivy's
    // dotted `a.b.c` (`std::replace('/', '.')` at :757, strip the leading
    // separator at :758-760), then construct the
    // `JsonPathPredicateReader<T>` matching `cast_type`.
    //
    // GONE with the composition rewrite: the `friend class` grant
    // (JsonFlatIndex.h:732-733) and the per-resolve copy of the whole
    // null-offset vector (:798) — the readers share one immutable copy.
    return nullptr;
}

TargetBitmap
JsonFlatIndexReader::Exists(std::string_view path, JsonValueType type) const {
    // TODO: move existing logic here (see JsonFlatIndex.h:52-60 Exists and
    // :62-70 ExactPathExists). The two differ only in the `json_subpaths`
    // argument to `json_exist_query`: true = "this path or anything under it",
    // false = "exactly this path, holding a value of this family". The
    // contract's `JsonValueType` parameter carries the second case, so the two
    // methods become one.
    //
    // `JsonValueType` here is the CONTRACT LAYER's native enum. Today
    // `index::JsonValueType` is an alias for the raw tantivy FFI enum
    // `::JsonExistValueType` (JsonFlatIndex.h:31) — an engine type on an index
    // signature. Mapping to the FFI value happens inside this function.
}

std::vector<JsonCastType>
JsonFlatIndexReader::CastTypesOf(std::string_view path) const {
    // For the flat index the answer is uniform: one tantivy index covers every
    // path of the field, so `GetCastType()` was a constant
    // (`JsonCastType::FromString("JSON")`, JsonFlatIndex.h:766-769). Callers
    // key off it today — `ExistsExpr.cpp:107-108` and
    // `ChunkedSegmentSealedImpl.cpp:405-410` both branch on
    // `cast_type == JSON` — so the constant must survive the move, just not on
    // the shared root: §5.7 takes `GetCastType`/`Exists` OFF `IndexBase` and
    // puts them here, which is why `IndexReaderBase` has neither.
    return {};
}

#define INSTANTIATE_JSON_PATH_READER(T) \
    template class JsonPathPredicateReader<T>;
INSTANTIATE_JSON_PATH_READER(bool)
INSTANTIATE_JSON_PATH_READER(int64_t)
INSTANTIATE_JSON_PATH_READER(double)
INSTANTIATE_JSON_PATH_READER(std::string_view)
#undef INSTANTIATE_JSON_PATH_READER

}  // namespace milvus::index
