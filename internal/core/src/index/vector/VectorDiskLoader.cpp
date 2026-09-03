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

#include "index/vector/VectorDiskLoader.h"

#include "index/vector/VectorDiskReader.h"

namespace milvus::index {

std::string
VectorDiskLoader::Family() const {
    // knowhere::IndexEnum::INDEX_DISKANN and its siblings.
    return {};
}

ReaderCaps
VectorDiskLoader::DeriveCaps(const Config& index_meta) const {
    return {};
}

std::shared_ptr<IndexReaderBase>
VectorDiskLoader::OpenIndex(storage::FileSource& source,
                            const storage::LoadOptions& opts) {
    // TODO: move existing logic here (see VectorDiskIndex.cpp:308-386 `Load` and
    // :904-940 `update_load_json`). The four steps are listed in the header,
    // together with the two places the contract does not reach.
    //
    // WHAT DOES NOT COME ALONG: `file_manager_` and
    // `GetLocalIndexObjectPrefix()` (the staging directory becomes
    // `opts.mmap_dir_path`), and the `LOAD_PRIORITY` dig out of `Config`
    // (`opts.op_ctx` carries priority and cancellation).
    //
    // `search_beamwidth_` is parsed here from the load params and INJECTED into
    // the reader's constructor instead of being mutated onto the index object
    // during load, which is what `update_load_json` does today
    // (VectorDiskIndex.cpp:929-931) — a load-time write into a "reader" that
    // §5 requires to be immutable once opened.
    return nullptr;
}

}  // namespace milvus::index
