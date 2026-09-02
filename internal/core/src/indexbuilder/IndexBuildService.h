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
#include <optional>
#include <string>
#include <vector>

#include "common/Types.h"
#include "index/contracts/Registry.h"
#include "indexbuilder/BuildDriver.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/ArtifactStats.h"
#include "storage/artifact/FileSink.h"

// The index-build service: the L5 orchestration that turns "a field in a
// manifest" into "uploaded index files".
//
// See core_refactor/01-scalar-index.md §6, §6.1, §6.2, §9 (row "indexbuilder")
// and README.md §4 (app layer) / §6.3 (what keeps the app layer from becoming
// a junk drawer).
//
// -------------------------------------------------------------------------
// THE PIPELINE, AND WHO OWNS EACH STEP
//
//   source (push)        storage : `IterateFieldDataFromManifest` calls back
//                                  batch by batch (storage/Util.h:353)
//   feed                 THIS    : forwards each batch to the driver
//   build                index   : `IndexBuilder<T>::Add` accumulates
//   Seal()               index   : -> `storage::ArtifactPtr`
//   Serialize(FileSink&) artifact: the artifact knows its own materialised
//                                  form (in-memory structure vs a local file
//                                  set) and writes itself into the sink
//   upload ORCHESTRATION THIS    : chooses the sink, sequences the writes,
//                                  handles cancellation/errors, turns the
//                                  resulting `ArtifactStats` into the C-ABI
//                                  result. The BYTES move inside storage.
//
// §6.2 states the split verbatim: "`Serialize`/`Load`/`Upload`/`LoadUnified`/
// `UploadUnified` are removed from the index classes entirely: serialization is
// on the Artifact, opening is on the Loader, UPLOAD ORCHESTRATION IS IN THE
// INDEXBUILDER SERVICE, and load orchestration is in segcore load."
//
// WHY `Serialize` SITS ON THE ARTIFACT AND NOT ON A SYMMETRIC CODEC (§6, three
// reasons, worth keeping because the asymmetry looks like an oversight):
//   1. the write direction has no independent call site — serialization always
//      immediately follows `Seal()`, while deserialization happens at another
//      time and usually in another process, where no Builder exists. A
//      symmetric Serialize/Deserialize codec would be a class NO CALL SITE EVER
//      USES BOTH HALVES OF;
//   2. for the file-based families (the tantivy ones) the write direction
//      cannot be split further — they write bytes into a local directory DURING
//      the build, so their `Serialize` is just handing over an already-written
//      file set;
//   3. the artifact knows its own materialised form, so it dispatches once;
//      an external codec would have to dispatch by family a second time.
//
// -------------------------------------------------------------------------
// WHAT THIS REPLACES: `indexbuilder::ScalarIndexCreator` (and eventually
// `VecIndexCreator`). §2.2 records that the creator only ever used four methods
// of the whole `IndexBase` surface — `CreateIndex` / `Build` / `Serialize` /
// `Upload` (indexbuilder/ScalarIndexCreator.cpp:188-243) — while exec used only
// `In`/`Range`/the pattern family and segcore load used only `Load` plus cache
// accounting. Three classes of caller, each using one face, all handed the full
// surface. That is the observation the four-face split comes from.
//
// -------------------------------------------------------------------------
// THIS FILE MUST NOT GROW AN ALGORITHM. README §6.3 gives the executable test
// for the app layer: THE MOMENT "choose the algorithm from the shape of the
// data" (`if has_index then X else Y`) appears here, that logic belongs one
// layer down — that is what a capability descriptor is for. Sequencing, error
// handling, leases, cancellation and tracing are what belongs here. The
// cardinality-based bitmap-vs-inverted choice, for one, is the BUILDER's (§6.3).

namespace milvus::storage {
class FileManagerContext;
}  // namespace milvus::storage

namespace milvus::indexbuilder {

// Native request. Deliberately NOT a proto: README §5 rule 2 keeps pb out of
// everything except the explicitly named adapter files and capi. The
// `proto::indexcgo::BuildIndexInfo` -> `BuildRequest` translation is the first
// of capi's three steps (`index_c.cpp`).
struct BuildRequest {
    // Which family to build and with what parameters. Replaces the
    // `index::CreateIndexInfo` parameter bag, which §11.2 item 4 breaks up.
    index::IndexFamily family;
    index::BuildParams params;

    // The value type the builder is templated on (element type for ARRAY,
    // cast target for a JSON path index).
    DataType value_type{DataType::NONE};

    FieldId field_id;

    // Where the input comes from. Exactly one is set.
    std::string manifest_path;              // storage-v2 / loon
    std::vector<std::string> insert_files;  // storage-v1 binlogs

    // Non-empty for a per-path JSON cast index; keyed as `(field, path)` in the
    // reader inventory (§5.7).
    std::string json_path;
};

class IndexBuildService {
 public:
    IndexBuildService(BuildRequest request,
                      storage::FileManagerContext& file_manager_context);

    // Phase 1: source -> driver -> `Seal()`.
    //
    // Body (to write):
    //   auto driver = MakeBuildDriver(req_.value_type, req_.family, req_.params);
    //   Feed(*driver);                                  // push, see below
    //   return std::move(*driver).Seal();
    storage::ArtifactPtr
    RunToArtifact();

    // Phase 2: artifact -> sink -> uploaded files. This IS the upload
    // orchestration §6.2 assigns to this service.
    //
    // Body (to write):
    //   auto sink = MakeSink();                         // storage-provided
    //   artifact.Serialize(*sink);
    //   return sink->Finish();                          // -> ArtifactStats
    storage::ArtifactStats
    Publish(const storage::Artifact& artifact);

    // Both phases. The two are separate because the C ABI splits them across
    // `CreateIndex` and `SerializeIndexAndUpLoad` (index_c.cpp:288, 1065) and
    // hands Go an opaque handle in between.
    //
    // TODO: move existing logic here — `ScalarIndexCreator::Build()` +
    // `Serialize()` + `Upload()` (indexbuilder/ScalarIndexCreator.cpp:212-254),
    // including the two behaviours that must not be lost in the move:
    //   - the `DataIsEmpty` special case: `Build()` catches it, sets
    //     `skip_empty_`, and `Upload()` then returns `IndexStats::New(0, {})`
    //     rather than failing the task (`:203-207,244-246`). In the new shape
    //     that is "an artifact with no entries", not an exception;
    //   - the engine-version fork: `version >= 3` calls `UploadUnified`,
    //     otherwise `Upload` (`:247-253`). Both collapse into one
    //     `Artifact::Serialize(FileSink&)` — the sink is what differs.
    storage::ArtifactStats
    Run();

 private:
    // Drives the push source. For the manifest path this is literally
    //   storage::IterateFieldDataFromManifest(
    //       manifest_path, props, field_meta, data_type, dim, element_type,
    //       column_mapping,
    //       [&](FieldDataPtr batch) { driver.Feed(batch); },
    //       max_inflight_bytes);
    // and for `InputSpec().needs_second_pass` (form C) it runs twice — §6.1.2
    // requires multi-pass to be part of the contract, with the cost being a
    // second IO pass over a cold column, and the first pass allowed to exit
    // early.
    //
    // For `InputSpec().form == LocalFile` (form D) it materialises to a local
    // file and calls `driver.SetSourceFile(path)` instead of feeding batches.
    //
    // TODO: move existing logic here — the three near-synonymous entry points
    // that live inside the index families today,
    // `storage::CacheRawDataAndFillMissing` (scalar) /
    // `CacheRawDataToMemory` (vector, in-memory) / `CacheRawDataToDisk`
    // (DiskANN), ALL of which are bolted to `FileManager`. §6.1.2's
    // "declaration + one shared materialiser" replaces the three, and that is
    // exactly what removes `FileManagerContext` from every builder (§3
    // principle 6, §10 rule 2). Note today's common entry point pulls the WHOLE
    // field into memory before any family sees it, which is why "every family
    // looks like it needs the full column resident" is an artefact of the
    // entry point rather than a property of the families (§6.1.1).
    void
    Feed(BuildDriver& driver);

    // The sink is `storage`'s: local staging plus remote write, encryption and
    // compression included. This service only chooses and sequences it —
    // `FileManagerContext` appears HERE and in the per-family Loader/Artifact
    // implementations, and NOWHERE on a Reader/Builder/Appender signature
    // (§10 rule 2).
    std::unique_ptr<storage::FileSink>
    MakeSink();

    BuildRequest req_;
};

}  // namespace milvus::indexbuilder
