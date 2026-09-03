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

#include "index/vector/VectorMemLoader.h"

#include "index/vector/VectorMemReader.h"

namespace milvus::index {

std::string
VectorMemLoader::Family() const {
    // One loader instance per knowhere index type; see the header.
    return {};
}

ReaderCaps
VectorMemLoader::DeriveCaps(const Config& index_meta) const {
    // Derived from family + build params alone, WITHOUT opening anything
    // (§4.1/§4.3). See the header for why the current `ReaderCaps` shape has
    // nothing useful for a vector family to fill in.
    return {};
}

std::shared_ptr<IndexReaderBase>
VectorMemLoader::OpenIndex(storage::FileSource& source,
                           const storage::LoadOptions& opts) {
    // TODO: move existing logic here.
    //   opts.enable_mmap == false -> VectorMemIndex.cpp:358-486 (`Load`) plus
    //                                :326-350 (`LoadWithoutAssemble`)
    //   opts.enable_mmap == true  -> VectorMemIndex.cpp:910-1190 (`LoadFromFile`)
    // Both end at the same place: a built `KnowhereEngine` plus a
    // `VectorValidData`, wrapped in a `VectorMemReader<T>`.
    //
    // WHAT MUST NOT COME ALONG: `file_manager_`, `LOAD_PRIORITY` fished out of a
    // `Config`, and the `std::filesystem::create_directories` calls. IO arrives
    // as the injected `source`; priority and cancellation ride on
    // `opts.op_ctx` (`storage/artifact/LoadOptions.h`); the local staging
    // directory is `opts.mmap_dir_path`.
    //
    // THE `T` PROBLEM: the reader is a template but this loader is not, because
    // `IndexLoader` is not templated (it cannot be — `LoaderRegistry` stores one
    // instance per family and the load path is type-erased by design, §3
    // principle 4: "templates on the hot path, type erasure on the management
    // interface"). So the element type has to be recovered from load metadata
    // here and dispatched over (the scalar side writes it as
    // `families::kValueTypeMetaKey`, `index/Families.h`), exactly as
    // `IndexFactory::CreateVectorIndex` does today with its `switch` on
    // `DataType`. That switch is the ONE piece of the
    // God factory that legitimately survives, and it lives in this file rather
    // than in a global one.
    return nullptr;
}

}  // namespace milvus::index
