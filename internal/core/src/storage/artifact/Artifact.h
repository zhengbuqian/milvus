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

#include "storage/artifact/FileSink.h"
#include "storage/artifact/LoadedArtifact.h"

// A built artifact: the thing a Builder's `Seal()` produced, before it is either
// used in place or handed to storage.
//
// See core_refactor/01-scalar-index.md §6.1 (the `IndexArtifact` snippet), §6
// (why `Serialize` is here), §11.2 rule 1 (why it is at L1 with the `Index`
// prefix dropped).
//
// WHY `Serialize` LIVES ON THE ARTIFACT WHILE `Open` LIVES ON THE LOADER.
// This asymmetry is deliberate (§6), for three reasons:
//
//  1. The write direction has no independent call site. Serialization always
//     happens immediately after `Seal()`, never on its own; deserialization
//     happens at another time, usually in another process, where no Builder
//     exists. A symmetric Codec offering both would be a class no call site ever
//     uses both halves of — symmetry in form only.
//  2. For the file-shaped families the write direction cannot be split further.
//     The tantivy families (inverted / text / ngram / json flat / bson) write
//     their bytes to a local directory *during* the build; the byte layout is
//     the builder's internal business, and their `Serialize` is in substance
//     just "hand over the files that are already on disk". Interposing an
//     encoder there is a fictional abstraction.
//  3. The Artifact knows its own materialized form, so implementing `Serialize`
//     on it costs one dispatch; implementing it outside costs a second dispatch
//     by family.
//
// DESIGN ACCEPTANCE CRITERION (§11.2 rule 1): all three materialized forms must
// fit through this interface without special-casing —
//   - knowhere `BinarySet`: a set of named in-memory blobs        -> FileSink::WriteEntry
//   - DiskANN: a large local file, streamed, never fully resident -> FileSink::WriteEntryFromLocalFile
//   - mmap: opened by mapping, nothing deserialized               -> ArtifactLoader::Open + LoadOptions::enable_mmap
// If a first implementation finds that the pipeline must know "this is a tantivy
// directory / a knowhere BinarySet / a DiskANN big file" in order to work, then
// it is not a resident of the pure byte world and the landing place has to move
// (§12.2, option 2: a separate L1 component instead of `storage/`). That check
// is meant to be run on the day this interface is first implemented.
//
// Naming provisional, see §12.2.

namespace milvus::storage {

class Artifact {
 public:
    virtual ~Artifact() = default;

    // Use the just-built artifact in place, without a round trip through
    // storage. This is one of the two entrances to a reader; the other is
    // `ArtifactLoader::Open` (§6.2), which opens the same thing from bytes.
    //
    // Returns the type-erased L1 root; each layer downcasts its own product
    // (§11.2 rule 1 — the same trick as the L2 type-erased root in §4.2, one
    // layer down). At L2 the consumer downcasts to `index::IndexReaderBase`.
    virtual std::shared_ptr<LoadedArtifact>
    OpenReader() const = 0;

    // Hand the materialized bytes to the sink. No upload here: the sink decides
    // where bytes go, and upload orchestration is the indexbuilder service's
    // (§6.2). `IndexBase::Upload` / `UploadUnified` disappear with this.
    virtual void
    Serialize(FileSink& sink) const = 0;
};

using ArtifactPtr = std::unique_ptr<Artifact>;

}  // namespace milvus::storage
