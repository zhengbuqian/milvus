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
#include <string>
#include <vector>

#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorValidData.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"
#include "storage/artifact/LoadedArtifact.h"

// ARTIFACT — the DiskANN materialization form.
//
// See core_refactor/01-scalar-index.md §6, §6.1.1 form D, §11.2 rule 1, and the
// DESIGN ACCEPTANCE CRITERION in storage/artifact/Artifact.h.
//
// FORM 2 OF 3: large local files, streamed, never fully resident. After
// `knowhere::Index::Build`, DiskANN's payload is ALREADY a set of files in the
// local index directory; the index object holds no bytes. `Serialize` is
// therefore literally "hand over the files that are already there" — §6 reason 2
// verbatim — which maps onto `FileSink::WriteEntryFromLocalFile(name, path)`.
//
// Today there is no `Serialize` worth the name at all: `VectorDiskAnnIndex<T>::
// Serialize` (`index/VectorDiskIndex.h:202-212`) is marked `// deprecated` and
// fakes a `BinarySet` by appending zero-length entries with the remote sizes,
// while the real work happens in `Upload` (`VectorDiskIndex.cpp:390-407`), which
// just reads back `file_manager_->GetRemotePathsToFileSize()` — the files were
// pushed to remote storage BY THE FILE MANAGER DURING BUILD, not by any
// serialize step. Under the artifact model that inversion goes away: the builder
// produces local files, the artifact hands them to the sink, the indexbuilder
// service uploads (§6.2).
//
// !! ACCEPTANCE FINDING — `CleanLocalData` HAS NO HOME IN THE PIPELINE.
// DiskANN's local build directory must be deleted after upload; today that is
// `VectorIndex::CleanLocalData` (`index/VectorIndex.h:149-151`, overridden at
// `VectorDiskIndex.cpp:897-903`), called from `indexbuilder/VecIndexCreator.cpp:
// 123` behind the `CleanLocalData` C API (`indexbuilder/index_c.cpp:1043`). It
// is neither a reader concern nor a `FileSink` concern as the contract is
// written (`FileSink` ends at `Finish() -> ArtifactStats`, with no notion of the
// staging directory it drained). Declared family-locally below and flagged: the
// sink either owns staging cleanup, or the artifact keeps an explicit
// `ReleaseLocalStaging()` that the indexbuilder service calls. Contract owner's
// call.
//
// !! Line references point at the tree before refactor phase 1 (master
// e255009e01).

namespace milvus::index {

template <typename T>
class VectorDiskArtifact final : public storage::Artifact {
 public:
    VectorDiskArtifact(KnowhereEngine engine,
                       VectorValidData valid,
                       std::string local_index_prefix,
                       std::vector<std::string> local_files);

    ~VectorDiskArtifact() override = default;

    // In-place use. Legal for DiskANN: after a build the engine is loaded and
    // searchable against the local files. Not a path production takes today
    // (DiskANN is always built in the indexbuilder process and loaded fresh on
    // the query node), but the interface is uniform (§6.2).
    std::shared_ptr<storage::LoadedArtifact>
    OpenReader() const override;

    // One `WriteEntryFromLocalFile` per file in the local index directory, plus
    // the validity file that knowhere wrote during build (VALID_DATA_KEY,
    // `VectorDiskIndex.cpp:434-436`) and, for embedding lists, the offsets file.
    void
    Serialize(storage::FileSink& sink) const override;

    // See the acceptance finding above. Family-local, NOT part of
    // `storage::Artifact`.
    void
    ReleaseLocalStaging();

 private:
    KnowhereEngine engine_;
    VectorValidData valid_;
    std::string local_index_prefix_;
    std::vector<std::string> local_files_;
};

}  // namespace milvus::index
