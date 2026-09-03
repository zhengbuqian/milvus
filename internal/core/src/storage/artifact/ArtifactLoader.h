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

#include "storage/artifact/FileSource.h"
#include "storage/artifact/LoadOptions.h"
#include "storage/artifact/LoadedArtifact.h"

// The read-only direction of the artifact pipeline: with no Builder present,
// turn persisted bytes back into a usable object.
//
// See core_refactor/01-scalar-index.md §6.2 and §11.2 rule 1.
//
// Its only coupling to the Builder is the FORMAT AGREEMENT — the same family's
// Builder writes what its Loader reads. That coupling is held by "same family,
// same directory" plus round-trip tests, not by sharing a class (§6).
//
// Naming provisional, see §12.2.

namespace milvus::storage {

class ArtifactLoader {
 public:
    virtual ~ArtifactLoader() = default;

    // `Open`, not `Deserialize`: under mmap nothing is deserialized (the artifact
    // is never fully materialized), and `Open` pairs with `Artifact::OpenReader()`
    // so the two entrances to a reader read as the isomorphism they are (§6.2).
    //
    // Returns the type-erased L1 base class; each layer downcasts its own
    // product (§11.2 rule 1). `index::IndexLoader` narrows this to
    // `index::IndexReaderBase` — see index/contracts/IndexLoader.h.
    virtual std::shared_ptr<LoadedArtifact>
    Open(FileSource& source, const LoadOptions& opts) = 0;
};

}  // namespace milvus::storage
