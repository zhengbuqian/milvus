// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <memory>
#include <string>

#include "common/Types.h"
#include "indexbuilder/BuildDriver.h"
#include "indexbuilder/IndexBuildService.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/ArtifactStats.h"

// A THIN SHELL over `IndexBuildService`, kept only because the C ABI splits one
// build into two calls.
//
// See core_refactor/01-scalar-index.md §9, row "indexbuilder": today
// "`ScalarIndexCreator` calls `CreateIndex`/`Build`/`Serialize`/`Upload`";
// after refactor phase 1 "Builder + `Artifact::Serialize` + a storage sink; the
// Creator becomes a thin shell".
//
// WHY THE SHELL SURVIVES AT ALL: `index_c.cpp` hands Go an opaque `CIndex`
// after `CreateIndex(...)` + `Build()` and only later calls
// `SerializeIndexAndUpLoad(index, result)` (index_c.cpp:288, 1065). Something
// has to hold the finished artifact between those two calls. That is this
// class's ENTIRE remaining job — everything else moved:
//
//   old `CreateIndex` (via `index::IndexFactory`'s God switch)
//        -> `index::BuilderRegistry<T>` through `MakeBuildDriver` (§11.2 item 4)
//   old `Build()`      -> the push feed + `IndexBuilder<T>::Add` (§6.1)
//   old `Serialize()`  -> `storage::Artifact::Serialize(FileSink&)` (§6)
//   old `Upload()`     -> upload orchestration inside `IndexBuildService` (§6.2)
//   old `Load(BinarySet&)` -> DELETED. It was declared "used for test"
//        (indexbuilder/IndexCreatorBase.h:35) and reading a built artifact back
//        is `index::IndexLoader::Open(FileSource&, LoadOptions&)`, which needs
//        no Builder in scope.
//
// NOTE the class name is now inaccurate: the Builder interface is shared by
// both families (§11.3, "Builder: one interface, differences absorbed by
// `BuilderInputSpec`"), so this shell is not scalar-specific. Renaming it is
// left to the same pass that retires `IndexCreatorBase`/`VecIndexCreator`.

namespace milvus::indexbuilder {

class ScalarIndexCreator {
 public:
    ScalarIndexCreator(BuildRequest request,
                       storage::FileManagerContext& file_manager_context);

    // Feed -> Seal. Keeps the artifact for the later `Upload()`.
    //
    // TODO: move existing logic here — see IndexBuildService::Run(). The
    // `DataIsEmpty` handling that used to live at
    // indexbuilder/ScalarIndexCreator.cpp:200-208 becomes "an artifact with no
    // entries", not a caught exception.
    void
    Build();

    // Serialize into a storage sink and upload. Returns what was written.
    //
    // TODO: move existing logic here — indexbuilder/ScalarIndexCreator.cpp:242-254.
    // The `scalar_index_engine_version >= 3 ? UploadUnified : Upload` fork
    // disappears: both are one `Artifact::Serialize(FileSink&)`, and the
    // version only selects which sink the service builds.
    storage::ArtifactStats
    Upload();

 private:
    IndexBuildService service_;
    storage::ArtifactPtr artifact_;
};

using ScalarIndexCreatorPtr = std::unique_ptr<ScalarIndexCreator>;

}  // namespace milvus::indexbuilder
