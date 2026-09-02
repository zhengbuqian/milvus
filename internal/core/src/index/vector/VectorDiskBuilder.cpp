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

#include "index/vector/VectorDiskBuilder.h"

#include "common/EasyAssert.h"
#include "index/vector/VectorDiskArtifact.h"

// SKELETON — bodies are the re-homing map (§11.3). Line references are to the
// pre-W1 tree (master e255009e01).

namespace milvus::index {

template <typename T>
VectorDiskBuilder<T>::VectorDiskBuilder(DataType elem_type,
                                        IndexType index_type,
                                        MetricType metric_type,
                                        IndexVersion version,
                                        int64_t dim,
                                        knowhere::Json build_params)
    : engine_(elem_type,
              std::move(index_type),
              std::move(metric_type),
              version,
              /*use_knowhere_build_pool=*/true),
      build_params_(std::move(build_params)) {
    engine_.SetDim(dim);
}

template <typename T>
BuilderInputSpec
VectorDiskBuilder<T>::InputSpec() const {
    BuilderInputSpec spec;
    spec.form = BuilderInputSpec::LocalFile;
    spec.needs_second_pass = false;
    return spec;
}

template <typename T>
void
VectorDiskBuilder<T>::Add(size_t n, const T* values, const bool* valid) {
    // A LocalFile-form builder is fed by path, not by value. Reaching here means
    // the caller ignored `InputSpec()`, which is a programming error, not a
    // missing capability — hence an assert rather than
    // `ThrowInfo(Unsupported, ...)` (§3 principle 3 / §10 rule 4 are about
    // capability answers). See the wart note in the header.
    AssertInfo(false,
               "VectorDiskBuilder is a LocalFile-form builder; feed it through "
               "SetSourceFile()");
}

template <typename T>
void
VectorDiskBuilder<T>::SetSourceFile(const std::string& path) {
    raw_data_path_ = path;
}

template <typename T>
void
VectorDiskBuilder<T>::SetEmbListOffsetsFile(const std::string& path) {
    emb_list_offsets_path_ = path;
}

template <typename T>
void
VectorDiskBuilder<T>::SetValidDataFile(const std::string& path) {
    valid_data_path_ = path;
}

template <typename T>
storage::ArtifactPtr
VectorDiskBuilder<T>::Seal() && {
    // TODO: move existing logic here (see VectorDiskIndex.cpp:405-538 —
    // `VectorDiskAnnIndex<T>::Build`), minus the two pieces that belong to other
    // faces:
    //   - `CacheRawDataToDisk<T>` (`:459`) is the SHARED MATERIALIZER's job now;
    //     its output arrives through `SetSourceFile` (§6.1.2).
    //   - `AcquireLocalDirWriteLease` / `AddFile` / `RemoveRawDataFiles`
    //     (`:419,432,453-454`) are file-manager work; local staging and hand-off
    //     move behind `storage::FileSink` at the artifact boundary (§10 rule 2).
    // What stays: build_config assembly, the embedding-list offsets check
    // (`:498-...`), reading back the validity bitmap knowhere wrote
    // (`ReadDiskValidData` at `:441` -> `BuildValidDataFromBitmap`), and the
    // all-null shortcut (`:444-457`).
    return nullptr;
}

template class VectorDiskBuilder<float>;
template class VectorDiskBuilder<float16>;
template class VectorDiskBuilder<bfloat16>;
template class VectorDiskBuilder<bin1>;
template class VectorDiskBuilder<sparse_u32_f32>;
template class VectorDiskBuilder<int8>;

}  // namespace milvus::index
