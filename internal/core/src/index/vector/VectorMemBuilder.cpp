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

#include "index/vector/VectorMemBuilder.h"

#include "index/vector/VectorMemArtifact.h"

// SKELETON — bodies are the re-homing map (§11.3). Line references are to the
// pre-W1 tree (master e255009e01).

namespace milvus::index {

template <typename T>
VectorMemBuilder<T>::VectorMemBuilder(DataType elem_type,
                                      IndexType index_type,
                                      MetricType metric_type,
                                      IndexVersion version,
                                      int64_t dim,
                                      knowhere::Json build_params,
                                      bool use_knowhere_build_pool)
    : engine_(elem_type,
              std::move(index_type),
              std::move(metric_type),
              version,
              use_knowhere_build_pool),
      build_params_(std::move(build_params)) {
    engine_.SetDim(dim);
}

template <typename T>
BuilderInputSpec
VectorMemBuilder<T>::InputSpec() const {
    // §6.1.1 form B+ -> `Form::Contiguous`; no second pass; `side_inputs` filled
    // from VEC_OPT_FIELDS when partition-key isolation is on
    // (`VectorMemIndex.cpp:517-523`).
    BuilderInputSpec spec;
    spec.form = BuilderInputSpec::Contiguous;
    spec.needs_second_pass = false;
    return spec;
}

template <typename T>
void
VectorMemBuilder<T>::Add(size_t n, const T* values, const bool* valid) {
    // TODO: move existing logic here (see VectorMemIndex.cpp:558-704) — the
    // per-`FieldData` half of today's `Build`: count rows and valid rows, copy
    // into the single contiguous buffer (dense) or deep-copy the sparse rows,
    // and track the max sparse dim. The `CacheRawDataToMemory` half does NOT
    // move here; the shared materializer drives this method instead (§6.1.2).
}

template <typename T>
void
VectorMemBuilder<T>::SetSideInput(
    std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>>
        scalar_info) {
    scalar_info_ = std::move(scalar_info);
}

template <typename T>
void
VectorMemBuilder<T>::SetEmbListOffsets(std::vector<size_t> offsets) {
    emb_list_offsets_ = std::move(offsets);
}

template <typename T>
storage::ArtifactPtr
VectorMemBuilder<T>::Seal() && {
    // TODO: move existing logic here (see VectorMemIndex.cpp:640-704 plus
    // :487-508 `BuildWithDataset`): GenDataset over the contiguous buffer, attach
    // SCALAR_INFO / EMB_LIST_OFFSET when present, `index_.Build(dataset, conf)`,
    // then seal the validity mapping (`BuildValidData`). The all-null shortcut
    // (`VectorMemIndex.cpp:672-680,692-696`: no vectors at all, only a validity
    // mapping) has to survive the move — it produces a legitimate artifact with
    // an empty index.
    //
    // Returns a `VectorMemArtifact`, NOT a reader: `Seal()` ends the builder
    // (§3 principle 1) and the artifact is the thing that can either be opened
    // in place (`OpenReader`) or serialized (`Serialize(FileSink&)`) — §6.2's
    // two entrances.
    return nullptr;
}

template class VectorMemBuilder<float>;
template class VectorMemBuilder<bin1>;
template class VectorMemBuilder<float16>;
template class VectorMemBuilder<bfloat16>;
template class VectorMemBuilder<int8>;
template class VectorMemBuilder<sparse_u32_f32>;

}  // namespace milvus::index
