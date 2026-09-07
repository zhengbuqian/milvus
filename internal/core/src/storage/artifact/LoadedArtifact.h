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

#include "cachinglayer/Utils.h"

// Minimal base class of the L1 artifact pipeline.
//
// See core_refactor/01-scalar-index.md §11.2 rule 1 and §12.2.
//
namespace milvus::storage {

// The smallest thing the cache layer can hold: something that has been opened
// from bytes and can report what it costs.
//
// Serialization is on `Artifact`, opening is on `ArtifactLoader`, upload
// orchestration is the indexbuilder service's, and load orchestration is
// segcore load's.
//
// `index::IndexReaderBase` derives from this (§11.2 rule 1: the lifecycle half
// of the old shared base class sinks to L1, the query half stays at L2). The
// JSON shredded layout — an offline-built, optionally-absent, cache-accounted
// derived artifact of a column — is the second intended consumer (§1, §12.2).
class LoadedArtifact {
 public:
    virtual ~LoadedArtifact() = default;

    // Report resources owned by the opened object. Heap-resident structures go
    // in the memory half; owned mmap/file-backed bytes go in the file half.
    // Pre-load admission estimates are separate translator inputs and must not
    // be returned as resident accounting.
    virtual cachinglayer::ResourceUsage
    CellByteSize() const = 0;
};

using LoadedArtifactPtr = std::unique_ptr<LoadedArtifact>;

}  // namespace milvus::storage
