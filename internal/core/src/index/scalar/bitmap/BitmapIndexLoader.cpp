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

#include "index/scalar/bitmap/BitmapIndexLoader.h"

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/bitmap/BitmapIndexReader.h"

namespace milvus::index {

std::string
BitmapIndexLoader::Family() const {
    return families::kBitmap;
}

ReaderCaps
BitmapIndexLoader::DeriveCaps(const Config& index_meta) const {
    // TODO: from families::kValueTypeMetaKey and kCoordDomainMetaKey:
    //   predicate          = true
    //   pattern_match      = (value type is VARCHAR)   <- see the §8 deviation
    //                                                     noted in the reader
    //   value_lookup       = (indexed field is not ARRAY)
    //   cheap_value_lookup = (offset cache enabled for this load)
    //   nested             = (coord domain is element)
    return ReaderCaps{.predicate = true, .value_lookup = true};
}

std::shared_ptr<IndexReaderBase>
BitmapIndexLoader::OpenIndex(storage::FileSource& source,
                             const storage::LoadOptions& opts) {
    // TODO: move existing logic here (see BitmapIndex.cpp:1444-1550
    // LoadEntries, the V3 shape) plus its helpers: DeserializeIndexMeta
    // (:407-432, JSON with a YAML fallback), DeserializeIndexData (:446-472,
    // +string spec :509-539), DeserializeValidBitsetData (:329-345),
    // ParseKey (:541-562), ChooseIndexLoadMode (:434-444) and MMapIndexData
    // (:564-628).
    //
    // `ChooseIndexLoadMode` becomes an honest loader decision: it picks the
    // BitmapLayout and whether the postings are mapped or heap-backed, from
    // `opts` plus the persisted length — and hands the reader the result. The
    // reader has no mmap state and no layout branch beyond which posting map is
    // populated.
    //
    // KEEP OR RETIRE DELIBERATELY, do not transcribe blindly:
    //   - the YAML V2 meta fallback (:422-431);
    //   - `rebuild_validity_from_postings` (:644-652, :1458-1469), documented
    //     as LOSSY for empty ARRAY rows at BitmapIndex.h:363-373.
    //
    // Superseded: `Load(BinarySet)` (:389-394), `Load(TraceContext, Config)`
    // (:699-718) and `LoadWithoutAssemble` (:630-697) — three entry points for
    // one operation.
    return nullptr;
}

namespace {

const bool kBitmapLoaderRegistered = [] {
    LoaderRegistry::Instance().Register(families::kBitmap,
                                        std::make_shared<BitmapIndexLoader>());
    return true;
}();

}  // namespace

}  // namespace milvus::index
