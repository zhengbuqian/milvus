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

#include "index/contracts/IndexLoader.h"

// LOADER INTERFACE — knowhere in-memory index families.
//
// See core_refactor/01-scalar-index.md §6.2 and §11.2 rule 1. One loader per
// family, STATELESS, registered in `LoaderRegistry` (contracts/Registry.h) —
// which is what replaces `IndexFactory`'s God switch and its six
// `*LoadResource` overloads (§11.2 rule 4).
//
// TWO LOAD SHAPES LIVE BEHIND THIS ONE `OpenIndex`, and they are §11.2's
// acceptance forms 1 and 3:
//
//   (a) MATERIALIZE — today `VectorMemIndex<T>::Load(TraceContext, Config)`
//       (`VectorMemIndex.cpp:358-486`): download every index file, reassemble
//       slices, hand knowhere a `BinarySet`, `index_.Deserialize(...)`.
//       Maps to `FileSource::ReadEntry` per entry.
//
//   (b) MMAP — today `VectorMemIndex<T>::LoadFromFile`
//       (`VectorMemIndex.cpp:910-1190`), taken when the config carries
//       MMAP_FILE_PATH: download the same files but STREAM THEM INTO ONE LOCAL
//       FILE through `storage::FileWriter`, then let knowhere mmap that file.
//       Embedding-list sidecars (EMB_LIST_META, EMB_LIST_RAW_INDEX) are written
//       to their OWN local files so knowhere can mmap them separately, and the
//       valid-data entries are pulled out and decoded rather than concatenated.
//       Maps to `LoadOptions::enable_mmap` + `mmap_dir_path`.
//
// !! ACCEPTANCE FINDING (b) DOES NOT FIT `FileSource` AS WRITTEN.
// The mmap path needs "materialize entries e1..ek into ONE local file, in this
// order" — the merged file IS the unit knowhere mmaps. `FileSource` offers
// `ReadEntry` (one entry -> memory), `ReadEntryToLocalFile` (one entry -> one
// file) and `ReadEntriesToLocalDir` (n entries -> n files). None of them is
// n -> 1. It fits only if the SLICE LAYER MOVES INSIDE the source, so that one
// LOGICAL entry (already the concatenation of its slices) can be requested with
// `ReadEntryToLocalFile`. That is the same choice VectorMemArtifact.h flags on
// the write side, and the two must be decided together. Reported, not patched
// around.

namespace milvus::index {

class VectorMemLoader final : public IndexLoader {
 public:
    ~VectorMemLoader() override = default;

    // Registry key. Vector families are keyed by the knowhere index type
    // ("HNSW", "IVF_FLAT", "SPARSE_INVERTED_INDEX", ...) rather than by one
    // "vector" bucket, because that is what the persisted metadata carries and
    // what decides which reader to build (contracts/Registry.h explains why the
    // key is a string and not `knowhere::IndexType`).
    std::string
    Family() const override;

    // §4.1's hard constraint: caps come from LOAD-TIME METADATA, with no index
    // object in existence, so a cold index is not pulled in just to decide the
    // execution path.
    //
    // !! `ReaderCaps` HAS NO VECTOR-SHAPED BIT. Every field in it
    // (contracts/ReaderCaps.h) is a scalar notion — predicate / pattern_match /
    // text_match / ngram_candidates / spatial / nested / value_lookup /
    // cheap_value_lookup / json_paths / exact. A vector reader answers "false to
    // all of them, exact = true", which tells a consumer nothing about the two
    // things it actually asks a vector index at path-decision time: CAN IT
    // RETURN RAW VECTORS (`HasRawData`, consumed by
    // `segcore/SegmentChunkReader.cpp:73,143,291,329`) and IS REFINE ON
    // (`IsIndexRefineEnabled`, consumed via `SegmentInterface.h:305`). Both are
    // load-time-derivable and both are today virtual calls on a live index.
    // Reported, not worked around here.
    ReaderCaps
    DeriveCaps(const Config& index_meta) const override;

    std::shared_ptr<IndexReaderBase>
    OpenIndex(storage::FileSource& source,
              const storage::LoadOptions& opts) override;
};

}  // namespace milvus::index
