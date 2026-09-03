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

#include "index/vector/VectorMemArtifact.h"

#include "index/vector/VectorMemReader.h"

namespace milvus::index {

template <typename T>
VectorMemArtifact<T>::VectorMemArtifact(
    KnowhereEngine engine,
    VectorValidData valid,
    std::vector<size_t> empty_emb_list_offsets)
    : engine_(std::move(engine)),
      valid_(std::move(valid)),
      empty_emb_list_offsets_(std::move(empty_emb_list_offsets)) {
}

template <typename T>
std::shared_ptr<storage::LoadedArtifact>
VectorMemArtifact<T>::OpenReader() const {
    // A handle copy of the knowhere node plus a shared validity mapping — see
    // the note on `KnowhereEngine`'s copy ctor.
    return std::make_shared<VectorMemReader<T>>(engine_, valid_);
}

template <typename T>
void
VectorMemArtifact<T>::Serialize(storage::FileSink& sink) const {
    // TODO: move existing logic here (see VectorMemIndex.cpp:303-325):
    //   - empty embedding-list artifact -> AppendEmptyEmbListOffsetsToBinarySet
    //   - all-null nullable artifact    -> no index payload at all
    //   - otherwise                     -> index_.Serialize(binary_set)
    //   - always                        -> AppendValidDataToBinarySet
    // then one `sink.WriteEntry(name, data, size)` per blob, and
    // `sink.PutMeta(...)` for what the loader must know before opening (index
    // type, knowhere version, dim, metric — today these ride in the load Config
    // that segcore assembles from etcd metadata, which is why
    // `LoadWithoutAssemble` has to be told `DIM_KEY` when the artifact contains
    // only validity, `VectorMemIndex.cpp:340-343`).
    //
    // `Disassemble` — see the header's note on where the slice layer belongs.
}

template class VectorMemArtifact<float>;
template class VectorMemArtifact<bin1>;
template class VectorMemArtifact<float16>;
template class VectorMemArtifact<bfloat16>;
template class VectorMemArtifact<int8>;
template class VectorMemArtifact<sparse_u32_f32>;

}  // namespace milvus::index
