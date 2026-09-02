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

#include "index/scalar/inverted/InvertedIndexArtifact.h"

#include <utility>

#include "index/Families.h"

namespace milvus::index {

InvertedIndexArtifact::InvertedIndexArtifact(
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::string local_dir,
    std::vector<size_t> null_offsets,
    DataType value_type,
    bool nested)
    : engine_(std::move(engine)),
      local_dir_(std::move(local_dir)),
      null_offsets_(std::move(null_offsets)),
      value_type_(value_type),
      nested_(nested) {
}

InvertedIndexArtifact::~InvertedIndexArtifact() = default;

std::shared_ptr<storage::LoadedArtifact>
InvertedIndexArtifact::OpenReader() const {
    // TODO: open a reader-mode wrapper over local_dir_ and build the typed
    // InvertedIndexReader<T> that matches value_type_.
    return nullptr;
}

void
InvertedIndexArtifact::Serialize(storage::FileSink& sink) const {
    // TODO: move existing logic here:
    //   - the tantivy directory as entries — InvertedIndexTantivy.cpp:874-918
    //     (WriteEntries, V3) is the shape to keep;
    //   - the null-offset side entry named INDEX_NULL_OFFSET_FILE_NAME
    //     (InvertedIndexTantivy.h:49), written by Serialize at :137-155;
    //   - the tantivy meta json — InvertedIndexTantivy.cpp:867-872
    //     (BuildTantivyMeta);
    //   - families::k*MetaKey, including kCoordDomainMetaKey = nested_ (§4.1,
    //     §5.8). Today the nested bit is written under TWO different names in
    //     two formats — `"is_nested_index"` in the V2 BinarySet and
    //     `"is_nested"` in the V3 meta — which is how it ended up being merged
    //     on load with an `is_nested_index_ || loaded` OR in some families and
    //     a plain assignment in others (see the BitmapIndex note). One key.
    //
    // NOT moved: `Upload` (InvertedIndexTantivy.cpp:157-207) — that is the
    // directory walk plus `disk_file_manager_->AddFile`, i.e. upload
    // orchestration, which belongs to the indexbuilder service (§6.2).
}

}  // namespace milvus::index
