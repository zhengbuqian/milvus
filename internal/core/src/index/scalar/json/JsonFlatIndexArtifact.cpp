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

#include "index/scalar/json/JsonFlatIndexArtifact.h"

#include <utility>

#include "index/Families.h"

namespace milvus::index {

JsonFlatIndexArtifact::JsonFlatIndexArtifact(
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::string local_dir,
    std::vector<size_t> null_offsets,
    std::string nested_path)
    : engine_(std::move(engine)),
      local_dir_(std::move(local_dir)),
      null_offsets_(std::move(null_offsets)),
      nested_path_(std::move(nested_path)) {
}

JsonFlatIndexArtifact::~JsonFlatIndexArtifact() = default;

std::shared_ptr<storage::LoadedArtifact>
JsonFlatIndexArtifact::OpenReader() const {
    return nullptr;
}

void
JsonFlatIndexArtifact::Serialize(storage::FileSink& sink) const {
    // TODO: the tantivy directory plus the null-offset entry — the same shape
    // as the inverted family (InvertedIndexTantivy.cpp:874-918), which this
    // family inherited today.
    //
    // A per-path CAST index additionally persisted a "path does not exist"
    // offset list (INDEX_NON_EXIST_OFFSET_FILE_NAME, index/Meta.h:108-109),
    // written by `JsonScalarIndexWrapper::WriteEntries`
    // (JsonScalarIndexWrapper.h:164-175) and by
    // `JsonHybridScalarIndex::WriteEntries` (JsonHybridScalarIndex.h:154-165).
    // That list belongs to the CAST index's artifact — i.e. to whichever
    // family's artifact the projection fed (inverted / bitmap / sorted) — not
    // here: the flat index answers `Exists` from the index itself
    // (`json_exist_query`) and needs no side list.
    //
    // KNOWN GAP IN THE CODE BEING REPLACED, for whoever ports the cast side:
    // `JsonHybridScalarIndex` overrides only `WriteEntries`/`LoadEntries`, not
    // `Serialize`/`Upload`/`Load`, so on the V1/V2 paths `non_exist_offsets_`
    // is never written or read and `Exists()` silently returns an empty bitmap.
    // That family is V3-only in practice.
}

}  // namespace milvus::index
