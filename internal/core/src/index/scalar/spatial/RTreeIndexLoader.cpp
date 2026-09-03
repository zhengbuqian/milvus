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

#include "index/scalar/spatial/RTreeIndexLoader.h"

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/spatial/RTreeIndexReader.h"

namespace milvus::index {

std::string
RTreeIndexLoader::Family() const {
    return families::kRTree;
}

ReaderCaps
RTreeIndexLoader::DeriveCaps(const Config& index_meta) const {
    return ReaderCaps{.spatial = true, .exact = false};
}

std::shared_ptr<IndexReaderBase>
RTreeIndexLoader::OpenIndex(storage::FileSource& source,
                            const storage::LoadOptions& opts) {
    // TODO: move existing logic here (see RTreeIndex.cpp:691-757 LoadEntries):
    // meta read (:695-696), materialize entries into a local directory
    // (:709-710 -> `source.ReadEntriesToLocalDir`), null offsets (:712-718),
    // base-path resolution over the .bgi / .meta.json pair (:720-741), then
    // RTreeQueryEngine::Load.
    //
    // ONE OF THE TWO LOAD PATHS DIES HERE. `RTreeIndex::Load(TraceContext,
    // Config)` (RTreeIndex.cpp:132-277) is 146 lines of remote-prefix fixup and
    // slice reassembly that `LoadEntries` re-does in 67; §6.2's single `Open`
    // entry point forces the choice that the V2/V3 coexistence has been
    // deferring. Keep the V3 shape.
    return nullptr;
}

namespace {

const bool kRTreeLoaderRegistered = [] {
    LoaderRegistry::Instance().Register(families::kRTree,
                                        std::make_shared<RTreeIndexLoader>());
    return true;
}();

}  // namespace

}  // namespace milvus::index
