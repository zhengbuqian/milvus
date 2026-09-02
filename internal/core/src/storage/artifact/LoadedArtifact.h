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

// Minimal root of the L1 artifact pipeline.
//
// See core_refactor/01-scalar-index.md §11.2 rule 1 and §12.2.
//
// NAMING IS PROVISIONAL. §12.2 records that both the landing place (inside
// `storage/` vs. a new L1 component) and the names (`Artifact` /
// `ArtifactLoader` / `ArtifactStats` / `LoadedArtifact`) are still open: the
// `Index` prefix of today's `IndexArtifact` / `IndexLoader` / `IndexStats` stops
// making sense once the pipeline sinks to L1 (the JSON shredded layout is not an
// index), and the existing `storage/IndexData.h` / `storage/IndexEntry*.h`
// naming family has to be renamed in the same pass or the split is only half
// done. These names are placeholders until §12.2 is closed.

namespace milvus {

// milvus::ResourceUsage — {memory_bytes, file_bytes}.
//
// DECLARED, NOT INCLUDED, ON PURPOSE. Today this type is
// `milvus::cachinglayer::ResourceUsage` (`cachinglayer/Utils.h`) and lives in
// the external milvus-common repo. README §5 rule 4 and 01-scalar-index.md §10
// rule 5 both forbid a cachinglayer type on a contract signature, so §11.2 makes
// "move it to milvus-common's common/ResourceUsage.h, namespace milvus" a
// CROSS-REPO PREREQUISITE that must land BEFORE this pipeline sinks to L1.
// Until it does, this declaration has no definition and the design is not yet
// self-consistent (§12.2, last paragraph, says so explicitly).
struct ResourceUsage;

}  // namespace milvus

namespace milvus::storage {

// The smallest thing the cache layer can hold: something that has been opened
// from bytes and can report what it costs.
//
// Everything else that used to live on `index::IndexBase` — Serialize / Load x2
// / Upload / LoadUnified / UploadUnified — is gone from the root: serialization
// is on `Artifact`, opening is on `ArtifactLoader`, upload orchestration is the
// indexbuilder service's, load orchestration is segcore load's (§6.2).
//
// `index::IndexReaderBase` derives from this (§11.2 rule 1: the lifecycle half
// of the old shared root sinks to L1, the query half stays at L2). The JSON
// shredded layout — an offline-built, optionally-absent, cache-accounted derived
// artifact of a column — is the second intended consumer (§1, §12.2).
class LoadedArtifact {
 public:
    virtual ~LoadedArtifact() = default;

    // Cache accounting hook, replacing `IndexBase::CellByteSize()` /
    // `SetCellSize()` / `ComputeByteSize()`.
    //
    // !! THE UNIT OF THIS NUMBER IS UNDEFINED TODAY. See §12.3. !!
    //
    // `cell_size_` (`index/Index.h:156`) is filled by two different yardsticks
    // depending on the family: most scalar families get
    // `SetCellSize({index_load_info_.index_size, 0})` — the size of the index
    // FILE BEFORE COMPRESSION (`segcore/Types.h:58`) — while TextMatch and
    // FMIndex get `SetCellSize({index->ByteSize(), 0})`, the MEASURED RESIDENT
    // FOOTPRINT. For marisa tries, roaring bitmaps and tantivy the two differ a
    // lot, and cachinglayer uses this number for admission and eviction.
    //
    // §12.3 is explicit that this must be defined — and every family made to
    // fill it by one yardstick — BEFORE the pipeline sinks to L1, because once
    // it is an L1 public contract, changing it moves columnar-format too. This
    // skeleton therefore deliberately does NOT pick a definition; it only
    // records the hole. Do not read the presence of this method as evidence the
    // question is settled.
    virtual ResourceUsage
    CellByteSize() const = 0;
};

using LoadedArtifactPtr = std::shared_ptr<LoadedArtifact>;

}  // namespace milvus::storage
