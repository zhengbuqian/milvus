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

#include "index/scalar/spatial/RTreeIndexArtifact.h"

#include <utility>

#include "index/Families.h"
#include "index/scalar/spatial/RTreeIndexReader.h"

namespace milvus::index {

RTreeIndexArtifact::RTreeIndexArtifact(std::string local_dir,
                                       std::vector<size_t> null_offsets,
                                       int64_t total_num_rows)
    : local_dir_(std::move(local_dir)),
      null_offsets_(std::move(null_offsets)),
      total_num_rows_(total_num_rows) {
}

RTreeIndexArtifact::~RTreeIndexArtifact() = default;

std::shared_ptr<storage::LoadedArtifact>
RTreeIndexArtifact::OpenReader() const {
    // TODO: open an RTreeQueryEngine over local_dir_ and wrap it in a reader.
    return nullptr;
}

void
RTreeIndexArtifact::Serialize(storage::FileSink& sink) const {
    // TODO: move existing logic here (see RTreeIndex.cpp:639-689 WriteEntries —
    // the directory listing + per-file entries at :644-677, the "file_names"
    // and "has_null" meta at :666-667, and the "index_null_offset" entry at
    // :679-684), plus the families::k*MetaKey set required by §4.1.
    //
    // NOT moved: `RTreeIndex::Upload` (RTreeIndex.cpp:347-392), which walks the
    // same directory but calls `disk_file_manager_->AddFile` — that is upload
    // orchestration, and it belongs to the indexbuilder service (§6.2).
    //
    // ALSO NOT MOVED: `RTreeIndex::Serialize(Config)` (RTreeIndex.cpp:394-407)
    // serialized ONLY the null offsets into a BinarySet — a V1 remnant that the
    // V3 entry writer already covers.
}

}  // namespace milvus::index
