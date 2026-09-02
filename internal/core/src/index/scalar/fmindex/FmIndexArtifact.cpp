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

#include "index/scalar/fmindex/FmIndexArtifact.h"

#include <utility>

#include "index/Families.h"
#include "index/scalar/fmindex/FmIndexReader.h"

namespace milvus::index {

FmIndexArtifact::FmIndexArtifact(fmindex::FMIndex engine,
                                 TargetBitmap null_bitmap,
                                 int64_t total_rows,
                                 int64_t total_tokens)
    : engine_(std::move(engine)),
      null_bitmap_(std::move(null_bitmap)),
      total_rows_(total_rows),
      total_tokens_(total_tokens) {
}

FmIndexArtifact::~FmIndexArtifact() = default;

std::shared_ptr<storage::LoadedArtifact>
FmIndexArtifact::OpenReader() const {
    // NOTE: FM has no in-place-build call site today (unlike text and the
    // interim vector indexes) — every FM index is built offline and then
    // loaded. The entry point still exists because §6 pairs it with
    // `IndexLoader::OpenIndex` for every family, and because the round-trip
    // test ("open in place == open from bytes") needs both sides.
    return nullptr;
}

void
FmIndexArtifact::Serialize(storage::FileSink& sink) const {
    // TODO: move existing logic here (see FMIndex.cpp:427-529 WriteEntries):
    //   - FMINDEX_BLOB_FILE_NAME entry, streamed from the engine's
    //     `SerializeToFile` (FMIndex.cpp:444-499) or from memory (:500-506);
    //   - FMINDEX_NULL_BITMAP_FILE_NAME entry, only when the field is nullable
    //     (:514-518);
    //   - meta FMINDEX_META_TOTAL_ROWS / FMINDEX_META_NULLABLE (:520-521), via
    //     `sink.PutMeta`, plus the families::k*MetaKey set that every family
    //     writes so `DeriveCaps` can work without opening the index (§4.1).
    //
    // DEAD ON ARRIVAL, do not port: `FMIndex::Serialize(Config)`
    // (FMIndex.cpp:405-412) returns an EMPTY BinarySet — the V1 path was never
    // used for this family. Its disappearance is a deletion, not a migration.
}

}  // namespace milvus::index
