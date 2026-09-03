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

#include "index/scalar/sort/SortedIndexArtifact.h"

#include <utility>

#include "index/Families.h"

namespace milvus::index {

template <typename T>
SortedIndexArtifact<T>::SortedIndexArtifact(std::vector<IndexStructure<T>> data,
                                            TargetBitmap valid_bitset,
                                            size_t total_num_rows,
                                            DataType value_type,
                                            bool nested)
    : data_(std::move(data)),
      valid_bitset_(std::move(valid_bitset)),
      total_num_rows_(total_num_rows),
      value_type_(value_type),
      nested_(nested) {
}

template <typename T>
SortedIndexArtifact<T>::~SortedIndexArtifact() = default;

template <typename T>
std::shared_ptr<storage::LoadedArtifact>
SortedIndexArtifact<T>::OpenReader() const {
    return nullptr;
}

template <typename T>
void
SortedIndexArtifact<T>::Serialize(storage::FileSink& sink) const {
    // TODO: move existing logic here (see ScalarIndexSort.cpp:692-711
    // WriteEntries — entries `index_data`, `idx_to_offsets`, `valid_bitset`,
    // meta `index_length` / `num_rows` / `is_nested`), plus the
    // families::k*MetaKey set (§4.1).
    //
    // NOT moved: `Serialize(Config) -> BinarySet` (:253-285) and `Upload`
    // (:287-308).
}

SortedStringIndexArtifact::SortedStringIndexArtifact(
    std::vector<std::string> unique_values,
    std::vector<std::vector<uint32_t>> posting_lists,
    TargetBitmap valid_bitset,
    size_t total_num_rows,
    bool nested)
    : unique_values_(std::move(unique_values)),
      posting_lists_(std::move(posting_lists)),
      valid_bitset_(std::move(valid_bitset)),
      total_num_rows_(total_num_rows),
      nested_(nested) {
}

SortedStringIndexArtifact::~SortedStringIndexArtifact() = default;

std::shared_ptr<storage::LoadedArtifact>
SortedStringIndexArtifact::OpenReader() const {
    return nullptr;
}

void
SortedStringIndexArtifact::Serialize(storage::FileSink& sink) const {
    // TODO: move existing logic here (see StringIndexSort.cpp:576-607
    // WriteEntries, which rests on `StringIndexSortMemoryImpl::
    // SerializeToBinary` (:900-971) and `GetSerializedSize` (:872-898); the
    // format is documented at StringIndexSort.h:287-292).
    //
    // Note the old `WriteEntries` had to `dynamic_cast` its own pImpl to the
    // memory variant and assert (StringIndexSort.cpp:581-583) — serialization
    // is only possible from the just-built shape. With the builder owning the
    // build shape and the artifact owning serialization, the cast is gone.
}

#define INSTANTIATE_SORTED_ARTIFACT(T) template class SortedIndexArtifact<T>;
INSTANTIATE_SORTED_ARTIFACT(bool)
INSTANTIATE_SORTED_ARTIFACT(int8_t)
INSTANTIATE_SORTED_ARTIFACT(int16_t)
INSTANTIATE_SORTED_ARTIFACT(int32_t)
INSTANTIATE_SORTED_ARTIFACT(int64_t)
INSTANTIATE_SORTED_ARTIFACT(float)
INSTANTIATE_SORTED_ARTIFACT(double)
#undef INSTANTIATE_SORTED_ARTIFACT

}  // namespace milvus::index
