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

#include "index/scalar/fmindex/FmIndexLoader.h"

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/fmindex/FmIndexReader.h"

namespace milvus::index {

FmIndexLoader::FmIndexLoader(double cost_ratio) : cost_ratio_(cost_ratio) {
}

std::string
FmIndexLoader::Family() const {
    return families::kFmIndex;
}

ReaderCaps
FmIndexLoader::DeriveCaps(const Config& index_meta) const {
    return ReaderCaps{.pattern_match = true};
}

std::shared_ptr<IndexReaderBase>
FmIndexLoader::OpenIndex(storage::FileSource& source,
                         const storage::LoadOptions& opts) {
    // TODO: move existing logic here (see FMIndex.cpp:531-720 LoadEntries):
    //   meta read (:543-558), mmap staging + engine LoadView (:575-639) or
    //   in-memory Deserialize (:640-646), structural validation (:647-658),
    //   null-bitmap unpack incl. the tail-bit check (:666-701).
    //
    // WHAT CHANGES: `ENABLE_MMAP` (FMIndex.cpp:569) and `LOAD_PRIORITY`
    // (:570-573) stop being Config keys read inside the index — mmap comes from
    // `opts.enable_mmap` / `opts.mmap_dir_path` and warmup from `opts.warmup`
    // (storage/artifact/LoadOptions.h). And `SetCellSize` (:708-715)
    // disappears: the reader reports, the load-side translator charges
    // (§4.2, §10 rule 5).
    //
    // ALSO GONE: `FMIndex::Load(TraceContext, Config)` (FMIndex.cpp:422-425)
    // was a one-line delegate to `ScalarIndex::LoadUnified`; with a single
    // opening entry point the V1/V2/V3 fan-out on this family collapses to one.
    return nullptr;
}

namespace {

const bool kFmLoaderRegistered = [] {
    // TODO: the cost ratio must be supplied by whoever constructs the registry
    // entry (segcore, from `queryNode.fmindexCostRatio`). A default-constructed
    // loader here keeps the static registration honest but leaves the injection
    // point visible instead of reaching back into segcore for it.
    LoaderRegistry::Instance().Register(families::kFmIndex,
                                        std::make_shared<FmIndexLoader>());
    return true;
}();

}  // namespace

}  // namespace milvus::index
