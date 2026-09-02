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

#include "index/contracts/IndexReader.h"
#include "index/contracts/ReaderCaps.h"
#include "storage/artifact/ArtifactLoader.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LoadOptions.h"

// The Loader face: one per family, read direction only.
//
// See core_refactor/01-scalar-index.md §6, §6.2, §11.2 rule 1.
//
// HOLDER AND LIFETIME (§3 principle 1): held by segcore load; an INDEPENDENT
// COLLABORATOR (one per family); STATELESS.
//
// Builder and Loader are separated by their INPUT, not by phase order: the
// Builder takes column data and produces an Artifact, the Loader takes persisted
// bytes and produces a Reader. Their only coupling is the FORMAT AGREEMENT,
// held by "same family, same directory" plus round-trip tests (§6).
//
// `Serialize` / `Load` / `Upload` / `LoadUnified` / `UploadUnified` are removed
// from index classes entirely: serialization is on the Artifact, opening is
// here, upload orchestration is the indexbuilder service's, load orchestration
// is segcore load's. IO arrives as an injected `storage::FileSource`; §10 rule 2
// forbids `FileManagerContext` / `DiskFileManagerImpl` anywhere but a family's
// Loader/Artifact IMPLEMENTATION file.

namespace milvus::index {

class IndexLoader : public storage::ArtifactLoader {
 public:
    ~IndexLoader() override = default;

    // "inverted" / "bitmap" / "stl_sort" / "marisa" / "text" / "ngram" /
    // "json_flat" / "rtree" / "fmindex" / vector families...
    // This is the registry key; see Registry.h.
    virtual std::string
    Family() const = 0;

    // Derive `ReaderCaps` from LOAD-TIME METADATA ALONE, with no index object in
    // existence — family plus build parameters, e.g. "inverted on a VARCHAR" =>
    // predicate + pattern_match + value_lookup.
    //
    // This is what makes §4.1's hard constraint achievable: caps must be
    // readable WITHOUT PINNING, so the inventory calls this at load time and
    // stores the result as pure data. `IndexReaderBase::Caps()` is then only a
    // consistency check against this value (§4.1, §4.3, §10 rule 3b).
    //
    // `index_meta` is the family-specific metadata bag; see `BuildParams` in
    // Registry.h for why it is not a shared struct.
    virtual ReaderCaps
    DeriveCaps(const Config& index_meta) const = 0;

    // Open persisted bytes into a reader.
    //
    // NAMING: `Open`, not `Deserialize` — under mmap nothing is deserialized
    // (the artifact is never fully materialized), and it pairs with
    // `storage::Artifact::OpenReader()`, making the two entrances to a reader
    // (from persisted bytes / from a freshly built artifact) read as the
    // isomorphism they are (§6.2).
    //
    // !! SPELLED `OpenIndex`, NOT `Open`, FOR A C++ REASON, NOT A DESIGN ONE.
    // §6.2's signature is `Open(storage::FileSource&, const LoadOptions&) ->
    // shared_ptr<IndexReaderBase>`, i.e. the L2 narrowing of
    // `ArtifactLoader::Open`, which returns `shared_ptr<LoadedArtifact>` (§11.2
    // rule 1). C++ covariant return types apply to raw pointers and references
    // only, never to `shared_ptr`, so an override cannot narrow the return type
    // and an overload cannot differ by return type alone. The narrowing is
    // therefore a second virtual plus a `final` forwarder below. Behaviour and
    // pairing are exactly as §6.2 describes.
    virtual std::shared_ptr<IndexReaderBase>
    OpenIndex(storage::FileSource& source,
              const storage::LoadOptions& opts) = 0;

    // Implemented once, here, in terms of `OpenIndex`. Families override
    // `OpenIndex`.
    std::shared_ptr<storage::LoadedArtifact>
    Open(storage::FileSource& source,
         const storage::LoadOptions& opts) final {
        return OpenIndex(source, opts);
    }
};

using IndexLoaderPtr = std::shared_ptr<IndexLoader>;

}  // namespace milvus::index
