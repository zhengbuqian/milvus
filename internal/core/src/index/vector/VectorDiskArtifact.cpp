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

#include "index/vector/VectorDiskArtifact.h"

#include "index/vector/VectorDiskReader.h"

namespace milvus::index {

template <typename T>
VectorDiskArtifact<T>::VectorDiskArtifact(KnowhereEngine engine,
                                          VectorValidData valid,
                                          std::string local_index_prefix,
                                          std::vector<std::string> local_files)
    : engine_(std::move(engine)),
      valid_(std::move(valid)),
      local_index_prefix_(std::move(local_index_prefix)),
      local_files_(std::move(local_files)) {
}

template <typename T>
std::shared_ptr<storage::LoadedArtifact>
VectorDiskArtifact<T>::OpenReader() const {
    // Beam width is a LOAD-time knob (`DISK_ANN_QUERY_BEAMWIDTH`, parsed in
    // `update_load_json`, VectorDiskIndex.cpp:926-931); a just-built artifact
    // uses the family default, as today.
    return std::make_shared<VectorDiskReader<T>>(engine_, valid_, 8);
}

template <typename T>
void
VectorDiskArtifact<T>::Serialize(storage::FileSink& sink) const {
    // TODO: move existing logic here (see VectorDiskIndex.cpp:390-407 `Upload`
    // and VectorDiskIndex.h:202-212 `Serialize`), inverted as described in the
    // header: instead of asking the file manager which files it already pushed,
    // hand each local file to `sink.WriteEntryFromLocalFile(name, path)` and let
    // the indexbuilder service upload.
}

template <typename T>
void
VectorDiskArtifact<T>::ReleaseLocalStaging() {
    // TODO: move existing logic here (see VectorDiskIndex.cpp:897-903
    // `CleanLocalData`). Flagged in the header: this has no home in the contract
    // as written.
}

template class VectorDiskArtifact<float>;
template class VectorDiskArtifact<float16>;
template class VectorDiskArtifact<bfloat16>;
template class VectorDiskArtifact<bin1>;
template class VectorDiskArtifact<sparse_u32_f32>;
template class VectorDiskArtifact<int8>;

}  // namespace milvus::index
