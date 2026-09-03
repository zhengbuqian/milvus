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

#include "index/scalar/ngram/NgramIndexLoader.h"

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/ngram/NgramIndexReader.h"

namespace milvus::index {

std::string
NgramIndexLoader::Family() const {
    return families::kNgram;
}

ReaderCaps
NgramIndexLoader::DeriveCaps(const Config& index_meta) const {
    return ReaderCaps{.ngram_candidates = true, .exact = false};
}

std::shared_ptr<IndexReaderBase>
NgramIndexLoader::OpenIndex(storage::FileSource& source,
                            const storage::LoadOptions& opts) {
    // TODO: move existing logic here (see NgramInvertedIndex.cpp:229-240
    // LoadEntries and :242-272 LoadIndexMetas for the avg-row-size entry with
    // its kDefaultAvgRowSize fallback at :268; :274-289
    // RetainTantivyIndexFiles; :291-323 Load is the older V2 duplicate).
    //
    // GONE FROM THE SIGNATURE SIDE: `NgramParams::loading_index`
    // (index/IndexInfo.h:24) — a bool that told ONE constructor whether it was
    // building or loading (NgramInvertedIndex.cpp:92). Two interfaces, no flag.
    return nullptr;
}

namespace {

const bool kNgramLoaderRegistered = [] {
    LoaderRegistry::Instance().Register(families::kNgram,
                                        std::make_shared<NgramIndexLoader>());
    return true;
}();

}  // namespace

}  // namespace milvus::index
