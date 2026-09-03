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

#include "index/scalar/auto/AutoIndexLoader.h"

#include "index/Families.h"
#include "index/contracts/Registry.h"

namespace milvus::index {

std::string
AutoIndexLoader::Family() const {
    return families::kAuto;
}

ReaderCaps
AutoIndexLoader::DeriveCaps(const Config& index_meta) const {
    // TODO: read the recorded concrete family out of `index_meta` and forward
    // to `LoaderRegistry::Instance().Lookup(family)->DeriveCaps(index_meta)`.
    return ReaderCaps{};
}

std::shared_ptr<IndexReaderBase>
AutoIndexLoader::OpenIndex(storage::FileSource& source,
                           const storage::LoadOptions& opts) {
    // TODO: read the recorded family, look up its loader, forward. What this
    // replaces is three format-specific ways of recovering one byte:
    //   V1: `DeserializeIndexType` from a BinarySet entry
    //       (HybridScalarIndex.cpp:377-389 -> :351-359)
    //   V2: find the remote file whose basename is "index_type", download it,
    //       reassemble slices, then decode (:391-421 -> :361-375)
    //   V3: `reader.GetMeta<uint8_t>("index_type")` (:438-453)
    //
    // The V2 path has a defect worth not reproducing: after recovering the
    // type it hands the SAME `index_files` list to the internal index
    // (:417), still containing the `index_type` entry — only
    // `InvertedIndexTantivy::RetainTantivyIndexFiles` filters it out, so the
    // other internal families receive a file they do not understand.
    return nullptr;
}

namespace {

const bool kAutoLoaderRegistered = [] {
    LoaderRegistry::Instance().Register(families::kAuto,
                                        std::make_shared<AutoIndexLoader>());
    return true;
}();

}  // namespace

}  // namespace milvus::index
