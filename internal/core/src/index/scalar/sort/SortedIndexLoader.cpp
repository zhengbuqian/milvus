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

#include "index/scalar/sort/SortedIndexLoader.h"

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/sort/SortedIndexReader.h"

namespace milvus::index {

std::string
SortedIndexLoader::Family() const {
    return families::kSort;
}

ReaderCaps
SortedIndexLoader::DeriveCaps(const Config& index_meta) const {
    // TODO: pattern_match = (value type is VARCHAR) — `StringIndexSort`
    // supports the LIKE family (StringIndexSort.h:130-133), the numeric one
    // does not. See the §8 deviation noted in SortedIndexReader.h.
    return ReaderCaps{.predicate = true,
                      .value_lookup = true,
                      .cheap_value_lookup = true};
}

std::shared_ptr<IndexReaderBase>
SortedIndexLoader::OpenIndex(storage::FileSource& source,
                             const storage::LoadOptions& opts) {
    // TODO: move existing logic here.
    //   numeric: ScalarIndexSort.cpp:713-905 LoadEntries — mmap-with-meta
    //            (:818-863), memory-with-meta (:864-885) and the
    //            backward-compat recompute (:886-897); mmap staging at
    //            :310-361 SetupMmapFromData.
    //   string : StringIndexSort.cpp:609-767 LoadEntries — mapped (:658-743)
    //            and heap (:744-762, which uses the mapped layout over an owned
    //            buffer); the parse/validate step is :86-139 ParseBinaryData.
    //
    // The mmap-vs-heap choice becomes a LOADER decision driven by
    // `opts.enable_mmap` / `opts.mmap_dir_path`, and the reader receives one
    // already-decided layout. That is why neither reader has an `is_mmap_`
    // member (§3 principle 6).
    //
    // Superseded here: `Load(BinarySet)`, `Load(TraceContext, Config)` and
    // `LoadWithoutAssemble` on both classes — six entry points for one
    // operation (ScalarIndexSort.cpp:430,437,363; StringIndexSort.cpp:345,351,374).
    return nullptr;
}

namespace {

const bool kSortedLoaderRegistered = [] {
    LoaderRegistry::Instance().Register(families::kSort,
                                        std::make_shared<SortedIndexLoader>());
    return true;
}();

}  // namespace

}  // namespace milvus::index
