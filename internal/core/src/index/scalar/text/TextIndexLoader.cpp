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

#include "index/scalar/text/TextIndexLoader.h"

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/text/TextIndexReader.h"

namespace milvus::index {

std::string
TextIndexLoader::Family() const {
    return families::kText;
}

ReaderCaps
TextIndexLoader::DeriveCaps(const Config& index_meta) const {
    return ReaderCaps{.text_match = true};
}

std::shared_ptr<IndexReaderBase>
TextIndexLoader::OpenIndex(storage::FileSource& source,
                           const storage::LoadOptions& opts) {
    // TODO: move existing logic here (see TextMatchIndex.cpp:181-253 Load —
    // the .v3-suffix sniff at :190-199 and the V2 branch at :201-252 — plus
    // InvertedIndexTantivy.cpp:920-966 LoadEntries and :255-314 LoadIndexMetas
    // for the null-offset side entry).
    //
    // WHAT CHANGES, beyond moving:
    //   - `CacheTextLogToDisk` / `DiskFileManagerImpl` / `STATS_BASE_PATH_KEY`
    //     disappear from the index side. Materializing remote entries into a
    //     local directory is `source.ReadEntriesToLocalDir(...)`
    //     (storage/artifact/FileSource.h); §10 rule 2 forbids a FileManager
    //     here at all.
    //   - mmap and warmup come from `opts` (storage/artifact/LoadOptions.h),
    //     not from Config keys `MMAP_FILE_PATH` / `ENABLE_MMAP` / `WARMUP`.
    //   - `TextMatchIndexHolder` (TextMatchIndex.h:119-152) does NOT come
    //     along: it calls cachinglayer's Manager from inside index/, which
    //     §10 rule 5 forbids. The reader reports `CellByteSize()`; segcore's
    //     translator charges it.
    //
    // KNOWN BUG THIS MOVE SHOULD NOT CARRY OVER: the V2 text load path never
    // calls `ComputeByteSize()` (only InvertedIndexTantivy.cpp:252/:641/:958
    // do), so `TextMatchIndexHolder` can charge the cache a stale or zero size.
    // Opening through one entry point makes the size well-defined by
    // construction. Related open question: §12.3 (`cell_size_` has no defined
    // unit across families).
    return nullptr;
}

namespace {

const bool kTextLoaderRegistered = [] {
    LoaderRegistry::Instance().Register(families::kText,
                                        std::make_shared<TextIndexLoader>());
    return true;
}();

}  // namespace

}  // namespace milvus::index
