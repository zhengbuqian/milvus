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

#include "index/scalar/bitmap/BitmapIndexArtifact.h"

#include <utility>

#include "index/Families.h"
#include "index/scalar/bitmap/BitmapIndexReader.h"

namespace milvus::index {

template <typename T>
BitmapIndexArtifact<T>::BitmapIndexArtifact(
    std::map<T, roaring::Roaring> postings,
    TargetBitmap valid_bitset,
    size_t total_num_rows,
    DataType value_type,
    bool nested)
    : postings_(std::move(postings)),
      valid_bitset_(std::move(valid_bitset)),
      total_num_rows_(total_num_rows),
      value_type_(value_type),
      nested_(nested) {
}

template <typename T>
BitmapIndexArtifact<T>::~BitmapIndexArtifact() = default;

template <typename T>
std::shared_ptr<storage::LoadedArtifact>
BitmapIndexArtifact<T>::OpenReader() const {
    return nullptr;
}

template <typename T>
void
BitmapIndexArtifact<T>::Serialize(storage::FileSink& sink) const {
    // TODO: move existing logic here (see BitmapIndex.cpp:1417-1442
    // WriteEntries, the V3 shape to keep), which rests on
    // GetIndexDataSize (:248-267, +string spec), SerializeIndexData
    // (:269-327, +string spec), SerializeValidBitsetData (:281-294) and
    // SerializeIndexMeta (:296-311).
    //
    // ONE KEY FOR THE NESTED BIT, not two. Today it is written as
    // `"is_nested_index"` in the V2 meta blob (BitmapIndex.cpp:302) and as
    // `"is_nested"` in the V3 meta (:1425) — and the two are read back with
    // DIFFERENT MERGE RULES:
    //   BitmapIndex   ASSIGNS      (BitmapIndex.cpp:418-420, :1454-1455)
    //   ScalarIndexSort ORs        (ScalarIndexSort.cpp:372-379, :719)
    //   StringIndexSort ORs        (StringIndexSort.cpp:400-406, :623)
    // The assignment is a latent bug: a reader constructed as nested that loads
    // a blob lacking the key silently stops being nested — and the flag gates
    // both `rebuild_validity_from_postings` (:642-645, :1456-1459) and the
    // all-valid bitset. With the flag persisted once, by the artifact, and read
    // once, by the loader, the merge question does not arise.
    //
    // NOT MOVED: `Serialize(Config) -> BinarySet` (:347-375), the V1/V2 path,
    // and `Upload` (:377-387), which is service orchestration (§6.2).
}

#define INSTANTIATE_BITMAP_ARTIFACT(T) template class BitmapIndexArtifact<T>;
INSTANTIATE_BITMAP_ARTIFACT(bool)
INSTANTIATE_BITMAP_ARTIFACT(int8_t)
INSTANTIATE_BITMAP_ARTIFACT(int16_t)
INSTANTIATE_BITMAP_ARTIFACT(int32_t)
INSTANTIATE_BITMAP_ARTIFACT(int64_t)
INSTANTIATE_BITMAP_ARTIFACT(float)
INSTANTIATE_BITMAP_ARTIFACT(double)
INSTANTIATE_BITMAP_ARTIFACT(std::string)
#undef INSTANTIATE_BITMAP_ARTIFACT

}  // namespace milvus::index
