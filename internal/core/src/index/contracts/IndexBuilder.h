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

#include <cstddef>
#include <string>
#include <vector>

#include "common/Types.h"
#include "storage/artifact/Artifact.h"

// The Builder face.
//
// See core_refactor/01-scalar-index.md §6 (Builder vs Loader), §6.1 (the face),
// §6.1.1 (five input forms), §6.1.2 (`BuilderInputSpec`), §6.3 (hybrid becomes a
// builder strategy), §11.3 (the face is shared with the vector family).
//
// HOLDER AND LIFETIME (§3 principle 1): held by the indexbuilder service and by
// segcore's build-in-place path; an INDEPENDENT COLLABORATOR, not an interface
// on the index object; ONE-SHOT, terminated by `Seal()`.
//
// BUILDER AND APPENDER ARE TWO FACES, NOT ONE. The Builder is one-shot,
// exclusive, and ends by producing an Artifact; the Appender
// (`GrowingScalarIndex<T>` / `GrowingVectorIndex`, see GrowingIndex.h) is
// long-lived, concurrent with reads, and only produces watermarked snapshots —
// growing's persistence goes through the flush path. Merging the two is exactly
// what produced `TextMatchIndex`'s four constructors (§2.2, §7 point 3).
//
// ONE FACE FOR BOTH FAMILIES. `T` is the VALUE TYPE, not the family: variable
// length values are expressed with view types (`std::string_view`, an array
// view, a sparse-row view) and dense vectors use `float` with `dim` known at
// construction. Hence no Scalar/Vector prefix (§6.1). Per-family builders — the
// text builder's tokenizer configuration, the json builder's path
// configuration, the vector builder's metric/dim — are IMPLEMENTATIONS OF THIS
// FACE with those knobs as constructor arguments, not new faces (§6.1 last line,
// §11.3 "Builder: same face for both families").
//
// WHY THERE IS NO `Consume(ScanCursor&)` (A PULL FACE).
// The two build paths have different sources and different modes, and neither
// wants pull:
//   - OFFLINE BUILD (indexbuilder, the production main path) sources remote
//     manifest/binlog and IS ALREADY PUSH — `IterateFieldDataFromManifest(...,
//     const std::function<void(FieldDataPtr)>& consumer, max_inflight_bytes)`
//     (`storage/Util.h:352`) calls back batch by batch, decodes on a background
//     pool and throttles on input bytes. There is no segment, no column object
//     and no `ScanCursor` anywhere near it.
//   - BUILD-IN-PLACE is the only one whose source is a loaded column
//     (`generate_interim_index` takes a `ChunkedColumnInterface`,
//     `ChunkedSegmentSealedImpl.h:1385`).
// Flattening pull into push costs the caller a loop; wrapping push into pull
// costs a thread/coroutine or a buffer inversion. So: push.
//
// COROLLARY — THE BUILDER'S INPUT CURRENCY IS A RAW ARRAY, not any component's
// object. It knows nothing of columns, cursors or storage formats, so
// `index -> columnar-format` is zero on the builder side; it is zero on the
// reader side too (§5.8: element->row folding is not the index's), which is why
// that edge disappears entirely once the four faces land.

namespace milvus::index {

// Static self-description: how this family wants to be fed (§6.1.2).
//
// The trick is the same one `ReaderCaps` plays on the query side — MOVE THE
// DIFFERENCE OUT OF THE INTERFACE AND INTO DATA. There is no per-family Builder
// interface and no single signature that all families are forced into.
struct BuilderInputSpec {
    // §6.1.1's five measured input forms collapse to three materialization
    // strategies, implemented ONCE by a shared materializer rather than copied
    // per family:
    //   Streaming  (form A) - fed slice by slice, the index buffers nothing.
    //                         tantivy families: `wrapper_->add_data<T>(ptr, n,
    //                         offset)` (`InvertedIndexTantivy.cpp:686`).
    //   Contiguous (forms B and B+) - the whole input must be resident to take
    //                         shape; B+ additionally requires ONE CONTIGUOUS
    //                         BLOCK. B: marisa fills a keyset then `trie_.build()`
    //                         then walks it AGAIN to fill `str_ids_`
    //                         (`StringIndexMarisa.cpp:173-205`); FMIndex
    //                         concatenates all docs for libsais
    //                         (`FMIndex.cpp:172`); RTree bulk-loads
    //                         (`RTreeIndex.cpp:366`). B+: knowhere in-memory
    //                         indexes assemble a single tensor for one
    //                         `index_.Build(dataset)`
    //                         (`VectorMemIndex.cpp:533-600`).
    //   LocalFile  (form D) - the raw data is materialized to a local file and
    //                         delivered BY PATH. DiskANN: `CacheRawDataToDisk<T>`
    //                         -> `DISK_ANN_RAW_DATA_PATH`
    //                         (`VectorDiskIndex.cpp:460-462`). It explicitly does
    //                         NOT want the data in memory.
    enum Form { Streaming, Contiguous, LocalFile };

    Form form = Streaming;

    // Form C: the family must look at the data before it can choose, so it needs
    // TWO PASSES — `ScanCursor::Seek(0)` must be usable for the re-scan, at the
    // cost of a second IO on a cold column. `HybridScalarIndex` is the case:
    // `SelectBuildTypeForPrimitiveType` first scans for cardinality (WITH AN
    // EARLY EXIT — it breaks as soon as distinct values hit the cap,
    // `HybridScalarIndex.cpp:167`), then builds the chosen one.
    //
    // Multi-pass MUST be in the contract; it cannot be an implementation secret,
    // because the caller drives the feeding.
    bool needs_second_pass = false;

    // NOT A PLACEHOLDER. `VectorMemIndex::Build` reads `VEC_OPT_FIELDS` and calls
    // `CacheOptFieldToMemory` (`VectorMemIndex.cpp:539-547`): partition-key
    // isolation needs ANOTHER FIELD's data as a build input. A single-cursor
    // signature cannot express that — a builder's input is not always "this
    // column".
    std::vector<FieldId> side_inputs;
};

template <typename T>
class IndexBuilder {
 public:
    virtual ~IndexBuilder() = default;

    // Static self-description; the caller decides how to feed based on it.
    virtual BuilderInputSpec
    InputSpec() const = 0;

    // THE ONLY DATA INPUT: push, driven by the caller.
    //
    // `Add` expresses an INPUT CHANNEL, not "this family can build
    // incrementally". How much the implementation buffers internally, and
    // whether it needs a second pass, are implementation details declared
    // through `InputSpec()`.
    virtual void
    Add(size_t n, const T* values, const bool* valid) = 0;

    // Only for families whose form == LocalFile (DiskANN): the data has already
    // been materialized to a local file by the shared materializer.
    //
    // The empty default body is NOT a capability-by-exception (§3 principle 3
    // forbids that): calling it on a family that did not declare
    // `Form::LocalFile` is a caller bug, and a family that did declare it
    // overrides. There is deliberately no `ThrowInfo(Unsupported)` here.
    virtual void
    SetSourceFile(const std::string& path) {
    }

    // Rvalue-qualified: sealing CONSUMES the builder. This is the one-shot
    // lifetime of §3 principle 1 expressed in the type system — after `Seal()`
    // there is no builder, only the artifact.
    //
    // Returns `storage::ArtifactPtr`, not an `IndexArtifactPtr`: §11.2 rule 1
    // sinks the artifact pipeline to L1 and drops the `Index` prefix, because
    // nothing in "serialize -> upload -> download -> open -> account" is
    // index-specific (naming provisional, §12.2). `storage::Artifact::OpenReader()`
    // returns the L1 type-erased root; index consumers downcast to
    // `IndexReaderBase`.
    //
    // §6.3: HYBRID BECOMES A BUILD-TIME STRATEGY. "Pick bitmap or inverted by
    // cardinality" is a build-time decision — the builder chooses at `Seal()`
    // and records the choice in the artifact metadata, and `IndexLoader::Open`
    // returns the chosen concrete reader directly. The runtime forwarding class
    // `HybridScalarIndex` is deleted.
    virtual storage::ArtifactPtr
    Seal() && = 0;
};

}  // namespace milvus::index
