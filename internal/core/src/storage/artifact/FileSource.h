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

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

// The narrow read side of the artifact pipeline.
//
// See core_refactor/01-scalar-index.md §6.2 and §3 principle 6.
//
// Same rule as `FileSink`: this is what an `ArtifactLoader` is handed instead of
// a `FileManagerContext` (§10 rule 2). It resolves entry names to bytes or to
// local files; it knows nothing about indexes, segments or queries — the
// storage component's one-line definition (README §4).
//
// The three materialization forms of §11.2 map onto the three read shapes here:
//   - in-memory blob set (knowhere `BinarySet`, bitmap/sort/marisa) -> ReadEntry
//   - local big file, streamed (DiskANN)                            -> ReadEntryToLocalFile
//   - mmap (and the tantivy directory families)                     -> ReadEntriesToLocalDir
//
// Naming provisional, see §12.2.

namespace milvus::storage {

class FileSource {
 public:
    virtual ~FileSource() = default;

    virtual std::vector<std::string>
    EntryNames() const = 0;

    virtual bool
    HasEntry(std::string_view name) const = 0;

    virtual int64_t
    EntrySize(std::string_view name) const = 0;

    // Fully materialize one entry in memory.
    virtual std::vector<uint8_t>
    ReadEntry(std::string_view name) = 0;

    // Materialize one entry as a local file without a full in-memory copy.
    // This is the DiskANN path.
    virtual void
    ReadEntryToLocalFile(std::string_view name,
                         const std::string& local_path) = 0;

    // Materialize a set of entries into a local directory, returning their local
    // paths in the order requested. This is the mmap path and the tantivy
    // directory path: `Open` under mmap does not deserialize anything (§6.2), it
    // maps these files.
    virtual std::vector<std::string>
    ReadEntriesToLocalDir(const std::vector<std::string>& names,
                          const std::string& local_dir) = 0;

    // Metadata written through `FileSink::PutMeta`. Readable WITHOUT reading any
    // payload entry — this is what lets a Loader decide which concrete reader to
    // build (§6.3 hybrid) and what lets the inventory derive `ReaderCaps` from
    // load-time metadata alone, before anything is pinned (§4.1/§4.3).
    virtual std::optional<std::string>
    GetMeta(std::string_view key) const = 0;
};

}  // namespace milvus::storage
