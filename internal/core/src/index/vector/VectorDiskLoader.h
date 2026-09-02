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

#pragma once

#include <memory>
#include <string>

#include "index/contracts/IndexLoader.h"

// LOADER FACE — DiskANN.
//
// See core_refactor/01-scalar-index.md §6.2, §11.2 rule 1, and the acceptance
// criterion in storage/artifact/Artifact.h.
//
// FORM 2 of the acceptance list, read direction. Today:
// `VectorDiskAnnIndex<T>::Load(TraceContext, Config)`
// (`index/VectorDiskIndex.cpp:308-386`) does
//   1. pick which remote files to cache locally:
//        GetCacheFilesForDiskIndexLoad(index_files, index_.LoadIndexWithStream())
//   2. `file_manager_->CacheIndexToDisk(cache_files, load_priority)` — download
//      into the LOCAL DIRECTORY named by `file_manager_->
//      GetLocalIndexObjectPrefix()`
//   3. read back the validity file and the empty-embedding-list offsets file
//      from that directory by convention
//   4. `index_.Deserialize(knowhere::BinarySet(), load_config)` — AN EMPTY
//      BINARY SET. knowhere opens the files itself, using
//      `load_config[DISK_ANN_PREFIX_PATH]` (`VectorDiskIndex.cpp:908`).
//
// Step 2 + step 4 map onto `FileSource::ReadEntriesToLocalDir(names, dir)` plus
// `LoadOptions::mmap_dir_path`, PROVIDED the local file names the source writes
// match the basenames DiskANN expects. The contract does not currently promise
// that (`ReadEntriesToLocalDir` only promises "their local paths in the order
// requested"), so this is a promise the first implementation has to add.
//
// !! ACCEPTANCE FINDING — STEP 1 IS A SHAPE `FileSource` CANNOT EXPRESS.
// When `index_.LoadIndexWithStream()` is true, `GetCacheFilesForDiskIndexLoad`
// (`index/vector/VectorIndexValidDataUtils.h:96-102`) narrows the download list
// to the VALID-DATA SLICES ONLY. Everything else is never fetched by Milvus at
// all: the ENGINE does its own IO against remote storage, driven by the load
// config. That is a fourth materialization form beyond §11.2's three —
// "the bytes never pass through our pipeline" — and no arrangement of
// `ReadEntry` / `ReadEntryToLocalFile` / `ReadEntriesToLocalDir` describes it,
// because the pipeline is not the reader of those bytes. It fits only if the
// contract gains a way to say "these entries are the engine's to fetch; give me
// their remote locators, not their bytes". Whether any knowhere index returns
// true for `LoadIndexWithStream()` is engine-side and version-dependent — the
// Milvus-side branch is unconditional. Reported, not patched around.

namespace milvus::index {

class VectorDiskLoader final : public IndexLoader {
 public:
    ~VectorDiskLoader() override = default;

    std::string
    Family() const override;

    ReaderCaps
    DeriveCaps(const Config& index_meta) const override;

    std::shared_ptr<IndexReaderBase>
    OpenIndex(storage::FileSource& source,
              const storage::LoadOptions& opts) override;
};

}  // namespace milvus::index
