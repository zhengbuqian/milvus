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

#include "index/scalar/json/JsonFlatIndexLoader.h"

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/json/JsonFlatIndexReader.h"

namespace milvus::index {

std::string
JsonFlatIndexLoader::Family() const {
    return families::kJsonFlat;
}

ReaderCaps
JsonFlatIndexLoader::DeriveCaps(const Config& index_meta) const {
    return ReaderCaps{.json_paths = true};
}

std::shared_ptr<IndexReaderBase>
JsonFlatIndexLoader::OpenIndex(storage::FileSource& source,
                               const storage::LoadOptions& opts) {
    // TODO: the same tantivy load path as the inverted family
    // (InvertedIndexTantivy.cpp:920-966 LoadEntries plus :255-314
    // LoadIndexMetas), which JsonFlatIndex inherited verbatim.
    //
    // FOR THE PER-PATH CAST INDEX, the piece that needs care is
    // `JsonScalarIndexWrapper::LoadIndexMetas` (JsonScalarIndexWrapper.h:233-300):
    // exact-name lookup, sliced-entry reassembly, and a v2.5.x compatibility
    // fallback at :298 (`non_exist_offsets_ = null_offset_`). Note that three of
    // that wrapper's methods HIDE rather than override their base
    // (`LoadIndexMetas` :234, `BuildTantivyMeta` :303,
    // `RetainTantivyIndexFiles` :317 — the last two ARE virtual in the base but
    // the overrides lack `override` and are not marked virtual), so which body
    // runs depends on the static type at the call site. Any port must decide
    // that on purpose rather than inherit the ambiguity.
    return nullptr;
}

namespace {

const bool kJsonFlatLoaderRegistered = [] {
    LoaderRegistry::Instance().Register(
        families::kJsonFlat, std::make_shared<JsonFlatIndexLoader>());
    return true;
}();

}  // namespace

}  // namespace milvus::index
