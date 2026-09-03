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
#include "common/Types.h"
#include "index/Index.h"
#include "storage/FileManager.h"

namespace milvus::indexbuilder {

// RETIRING — REPLACED BY `indexbuilder/IndexBuildService.h`.
//
// See core_refactor/01-scalar-index.md §6.2 and §9 (row "indexbuilder"). Each
// method has a new home:
//   Build(DatasetPtr, valid, len)  -> `index::IndexBuilder<T>::Add(n, values, valid)`
//                                     driven by the push source (§6.1). The
//                                     `DatasetPtr` (a knowhere type) does not
//                                     survive on the scalar path — §11.2 item 5
//                                     bans knowhere from the scalar families.
//   Build()                        -> `IndexBuildService::RunToArtifact()`
//   Serialize() -> BinarySet       -> `storage::Artifact::Serialize(FileSink&)`.
//                                     `BinarySet` is `knowhere::BinarySet`
//                                     (common/Types.h:681), so today even a
//                                     scalar index's serialization currency is a
//                                     knowhere type — §11.2 item 5 confines it
//                                     to the vector family's Loader/Artifact.
//   Load(const BinarySet&)         -> DELETED. Declared "used for test" below;
//                                     reading a built artifact back is
//                                     `index::IndexLoader::Open(FileSource&,
//                                     LoadOptions&)`, no Builder in scope.
//   Upload() -> IndexStatsPtr      -> `IndexBuildService::Publish()`, returning
//                                     `storage::ArtifactStats`. §11.2 item 1
//                                     sinks the whole artifact pipeline to L1
//                                     and drops the `Index` prefix, because
//                                     nothing in it is index-specific.
//
// The base is shared with `VecIndexCreator`, which migrates the same way: §11.3
// keeps the Builder interface UNIFIED across the two families and pushes the
// real difference (input form) into `index::BuilderInputSpec` instead.
class IndexCreatorBase {
 public:
    virtual ~IndexCreatorBase() = default;

    virtual void
    Build(const milvus::DatasetPtr& dataset,
          const bool* valid_data = nullptr,
          const int64_t valid_data_len = 0) = 0;

    virtual void
    Build() = 0;

    virtual milvus::BinarySet
    Serialize() = 0;

    // used for test.
    virtual void
    Load(const milvus::BinarySet&) = 0;

    virtual index::IndexStatsPtr
    Upload() = 0;
};

using IndexCreatorBasePtr = std::unique_ptr<IndexCreatorBase>;

}  // namespace milvus::indexbuilder
