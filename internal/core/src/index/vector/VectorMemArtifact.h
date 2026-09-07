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

#pragma once

#include <memory>
#include <vector>

#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorValidData.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"
#include "storage/artifact/LoadedArtifact.h"

// ARTIFACT — the legacy knowhere `BinarySet` materialization form.
//
// See core_refactor/01-scalar-index.md §6 (why `Serialize` lives on the artifact
// and `Open` on the loader), §11.2 rule 1 (the pipeline sinks to L1 and the
// `Index` prefix goes away), and the DESIGN ACCEPTANCE CRITERION in
// storage/artifact/Artifact.h: BinarySet, DiskANN big file and mmap must all fit
// with no special-casing.
//
// FORM 1 OF 3: a set of named in-memory blobs. `knowhere::Index::Serialize`
// fills a `BinarySet` (name -> {shared_ptr<uint8_t[]>, size}), which is a
// literal match for `FileSink::WriteEntry(name, data, size)`. Today's body is
// `VectorMemIndex<T>::Serialize` (`index/VectorMemIndex.cpp:303-325`):
//   index_.Serialize(binary_set)          -> N named blobs
//   AppendValidDataToBinarySet(...)       -> 2 more named blobs
//   Disassemble(binary_set)               -> slices anything over FILE_SLICE_SIZE
//                                            into `name_0..name_k` plus an
//                                            INDEX_FILE_SLICE_META entry
//
// `Disassemble`/`Assemble` (`common/Utils.h`) are a pure byte-level concern —
// nothing about them is index-specific — so they live inside the sink and the
// source. The artifact writes one logical entry per blob and never exposes
// `name_0..name_k` or the slice-meta entry. The baseline has no packed V3 vector
// format, so this artifact rejects a V3 sink instead of inventing one.
//
// !! Line references point at the tree before refactor phase 1 (master
// e255009e01).

namespace milvus::index {

class VectorMemLocalFiles;

template <typename T>
class VectorMemArtifact final : public storage::Artifact {
 public:
    VectorMemArtifact(KnowhereEngine engine,
                      VectorValidData valid,
                      std::vector<size_t> empty_emb_list_offsets = {});

    // Rehydrates an already validated loader generation. The knowhere handle,
    // validity mapping and optional mmap files are immutable shared state; no
    // vector payload or offset array is copied here.
    VectorMemArtifact(KnowhereEngine engine,
                      VectorValidData valid,
                      std::shared_ptr<VectorMemLocalFiles> local_files);

    ~VectorMemArtifact() override = default;

    // Entrance 1 to a reader: use what was just built, with no round trip
    // through storage (§6.2). This is the path `generate_interim_index`
    // (`segcore/ChunkedSegmentSealedImpl.h:1385`) and the growing appender's
    // cold-start build take — see §7 point 3 ("build-in-place is NOT growing")
    // and index/growing/KnowhereGrowingVectorIndex.h.
    std::unique_ptr<storage::LoadedArtifact>
    OpenReader() const override;

    // Hand the bytes to the sink. NO UPLOAD HERE: upload orchestration is the
    // indexbuilder service's (§6.2), which is what removes
    // `VectorMemIndex<T>::Upload` (`VectorMemIndex.cpp:292-302`, today
    // `Serialize` + `file_manager_->AddFile` + `GetRemotePathsToFileSize`) from
    // the index class entirely. The `IndexStats` it used to return is now the
    // sink's `ArtifactStats Finish()`.
    void
    Serialize(storage::FileSink& sink) const override;

 private:
    // Declared before engine_ so the final knowhere handle is destroyed before
    // the loader-created mmap directory is removed.
    std::shared_ptr<VectorMemLocalFiles> local_files_;
    KnowhereEngine engine_;
    VectorValidData valid_;
    // The all-null-nullable and empty-embedding-list artifacts have NO knowhere
    // index inside at all — only the validity mapping and/or the offsets. Both
    // are real, serialized states today (`VectorMemIndex.cpp:306-316`), and both
    // must survive as artifacts, which is a useful check on the model: an
    // artifact is not required to contain an engine. The offsets live in the
    // engine's immutable shared generation so every reader opened from this
    // artifact observes the same state without an O(rows) copy.
};

}  // namespace milvus::index
