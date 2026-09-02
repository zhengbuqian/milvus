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

#include "index/scalar/marisa/MarisaIndexLoader.h"

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/marisa/MarisaIndexReader.h"

namespace milvus::index {

std::string
MarisaIndexLoader::Family() const {
    return families::kMarisa;
}

ReaderCaps
MarisaIndexLoader::DeriveCaps(const Config& index_meta) const {
    // Constant for this family: marisa is VARCHAR-only and always has raw data.
    return ReaderCaps{.predicate = true,
                      .pattern_match = true,
                      .value_lookup = true,
                      .cheap_value_lookup = true};
}

std::shared_ptr<IndexReaderBase>
MarisaIndexLoader::OpenIndex(storage::FileSource& source,
                             const storage::LoadOptions& opts) {
    // TODO: move existing logic here (see StringIndexMarisa.cpp:873-1133
    // LoadEntries — the trie, the str_ids array in mapped (:924-962) or heap
    // (:963-981) form, and the CSR in mapped (:1038-1085) or heap (:1086-1122)
    // form, plus the backward-compat `fill_offsets()` rebuild at :1123-1126 for
    // artifacts written before the CSR was persisted).
    //
    // Superseded: `Load(BinarySet)` (:339-343), `Load(TraceContext, Config)`
    // (:345-363) and `LoadWithoutAssemble` (:287-337).
    return nullptr;
}

namespace {

const bool kMarisaLoaderRegistered = [] {
    LoaderRegistry::Instance().Register(families::kMarisa,
                                        std::make_shared<MarisaIndexLoader>());
    return true;
}();

}  // namespace

}  // namespace milvus::index
