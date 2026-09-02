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

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "storage/artifact/ArtifactStats.h"

// The narrow write side of the artifact pipeline.
//
// See core_refactor/01-scalar-index.md §6.2 and §3 principle 6.
//
// THIS IS WHAT REPLACES `storage::FileManagerContext` ON INDEX CLASSES. Today 10+
// index headers include `FileManager.h` / `DiskFileManagerImpl.h` and carry a
// `FileManagerContext` member (§2.1); §10 rule 2 makes that a lint failure —
// `FileManagerContext` / `DiskFileManagerImpl` may appear only inside a family's
// Loader/Artifact implementation file and in the indexbuilder upload
// orchestration, never on a Reader / Builder / Appender signature or member.
//
// It is a *sink*, not a file manager: it accepts named entries and finishes.
// It does not upload — upload orchestration belongs to the indexbuilder service
// (§6.2) — and it does not know what an index is.
//
// The shape below is the entry model that `storage/IndexEntryWriter.h` already
// implements (named entries + a meta blob), so this is a narrowing of something
// that exists rather than a new format.
//
// Naming provisional, see §12.2.

namespace milvus::storage {

class FileSink {
 public:
    virtual ~FileSink() = default;

    // An entry that is already a buffer in memory.
    // Covers: knowhere `BinarySet` (a set of named blobs) and every scalar
    // family whose serialized form is a set of named buffers (bitmap, sort,
    // marisa, RTree).
    virtual void
    WriteEntry(std::string_view name, const void* data, size_t size) = 0;

    // An entry that is already a file on local disk, handed over by path.
    // Covers: the tantivy families, which write their own directory during
    // Build and whose `Serialize` is only "hand over the files that are already
    // there" (§6 reason 2), and DiskANN, whose artifact is a large local file
    // that must never be fully resident (§6.1.1 form D).
    virtual void
    WriteEntryFromLocalFile(std::string_view name,
                            const std::string& local_path) = 0;

    // Small non-payload metadata the Loader needs before or while opening.
    // Examples: hybrid's build-time choice of bitmap vs. inverted (§6.3), the
    // `is_nested` bit that today rides in the `BinarySet` (§5.8), the tantivy
    // index version.
    virtual void
    PutMeta(std::string_view key, const std::string& value) = 0;

    // Close the artifact and report what was written.
    virtual ArtifactStats
    Finish() = 0;
};

}  // namespace milvus::storage
