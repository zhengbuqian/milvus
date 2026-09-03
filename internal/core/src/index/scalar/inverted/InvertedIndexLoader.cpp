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

#include "index/scalar/inverted/InvertedIndexLoader.h"

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/inverted/InvertedIndexReader.h"

namespace milvus::index {

std::string
InvertedIndexLoader::Family() const {
    return families::kInverted;
}

ReaderCaps
InvertedIndexLoader::DeriveCaps(const Config& index_meta) const {
    // TODO: read families::kValueTypeMetaKey and kCoordDomainMetaKey and set
    //   predicate      = true
    //   pattern_match  = (value type is VARCHAR/STRING)
    //   nested         = (coord domain is element)
    return ReaderCaps{.predicate = true};
}

std::shared_ptr<IndexReaderBase>
InvertedIndexLoader::OpenIndex(storage::FileSource& source,
                               const storage::LoadOptions& opts) {
    // TODO: move existing logic here:
    //   InvertedIndexTantivy.cpp:920-966 (LoadEntries, V3) — the shape to keep;
    //   :255-314 (LoadIndexMetas) for the null-offset side entry, including the
    //       sliced/unsliced variants;
    //   :316-336 (RetainTantivyIndexFiles) for filtering non-tantivy entries;
    //   :217-253 (Load, V2) is the older duplicate of the same job.
    //
    // WHAT CHANGES: entry materialization becomes
    // `source.ReadEntriesToLocalDir(...)` instead of `DiskFileManagerImpl`
    // (§10 rule 2); mmap/warmup come from `opts` instead of the MMAP_FILE_PATH
    // / ENABLE_MMAP / WARMUP config keys; and the returned object is the typed
    // reader chosen from the persisted value type, so the caller never
    // downcasts (§9: "dynamic_cast to a concrete index reaches zero — the
    // refactor phase 1 exit criterion").
    return nullptr;
}

namespace {

const bool kInvertedLoaderRegistered = [] {
    LoaderRegistry::Instance().Register(
        families::kInverted, std::make_shared<InvertedIndexLoader>());
    return true;
}();

}  // namespace

}  // namespace milvus::index
