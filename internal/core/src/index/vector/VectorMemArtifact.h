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

// ARTIFACT — the knowhere `BinarySet` materialization form.
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
// !! WHERE THE SLICE LAYER GOES IS A REAL DECISION, NOT A DETAIL.
// `Disassemble`/`Assemble` (`common/Utils.h`) are a pure byte-level concern —
// nothing about them is index-specific — so they belong INSIDE the sink and the
// source, and the artifact should write ONE LOGICAL ENTRY PER BLOB. If instead
// the slicing stays here, every family re-implements it (the scalar families
// call the same pair today) and `FileSource::EntryNames()` starts returning
// `name_0..name_k` plus a meta entry, i.e. leaking the physical layout into the
// loader. The contract permits either; only the first one is consistent with
// "storage is the world of bytes and does not know what an index is"
// (README §4). Recorded here because the first `Serialize` implementation has to
// choose. See the report.
//
// !! Line references point at the pre-W1 tree (master e255009e01).

namespace milvus::index {

template <typename T>
class VectorMemArtifact final : public storage::Artifact {
 public:
    VectorMemArtifact(KnowhereEngine engine,
                      VectorValidData valid,
                      std::vector<size_t> empty_emb_list_offsets = {});

    ~VectorMemArtifact() override = default;

    // Entrance 1 to a reader: use what was just built, with no round trip
    // through storage (§6.2). This is the path `generate_interim_index`
    // (`segcore/ChunkedSegmentSealedImpl.h:1385`) and the growing appender's
    // cold-start build take — see §7 point 3 ("build-in-place is NOT growing")
    // and index/growing/KnowhereGrowingVectorIndex.h.
    std::shared_ptr<storage::LoadedArtifact>
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
    KnowhereEngine engine_;
    VectorValidData valid_;
    // The all-null-nullable and empty-embedding-list artifacts have NO knowhere
    // index inside at all — only the validity mapping and/or the offsets. Both
    // are real, serialized states today (`VectorMemIndex.cpp:306-316`), and both
    // must survive as artifacts, which is a useful check on the model: an
    // artifact is not required to contain an engine.
    std::vector<size_t> empty_emb_list_offsets_;
};

}  // namespace milvus::index
