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
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "nlohmann/json_fwd.hpp"
#include "storage/artifact/FileSink.h"
#include "storage/artifact/LoadOptions.h"

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
//   - neutral named in-memory buffers (bitmap/sort/marisa) -> ReadEntry
//   - local big file, streamed (DiskANN)                   -> ReadEntryToLocalFile
//   - mmap (and tantivy directory families)                -> ReadEntriesToLocalDir
//
namespace milvus::storage {

struct FileManagerContext;

// V1/V2 has two physical conventions. Legacy named-buffer objects use
// SLICE_META to identify slices. Directory artifacts name every
// physical slice `<basename>_<N>` without SLICE_META. The caller knows the
// family and must select the convention; guessing from a numeric suffix would
// reinterpret legitimate named-buffer entry names.
enum class V1SourceLayout {
    MemoryEntries,
    DiskFiles,
};

class FileSource {
 public:
    virtual ~FileSource() = default;

    virtual Generation
    Gen() const = 0;

    virtual std::vector<std::string>
    EntryNames() const = 0;

    virtual bool
    HasEntry(std::string_view name) const = 0;

    // Returns a known logical length when the transport carries one. On legacy
    // layouts without length metadata this may materialize exactly this entry
    // as load-time I/O. It is not a metadata-only caps/admission API.
    virtual int64_t
    EntrySize(std::string_view name) const = 0;

    // Fully materialize one entry in memory.
    virtual std::vector<uint8_t>
    ReadEntry(std::string_view name) = 0;

    // Materialize one entry as a local file without a full in-memory copy.
    // This is the DiskANN path. Publication uses a checked same-directory
    // staging file and atomic replacement, so failure preserves an existing
    // destination.
    virtual void
    ReadEntryToLocalFile(std::string_view name,
                         const std::string& local_path) = 0;

    // Copy one exact physical V1/V2 DiskFiles object to a local file without
    // applying the named-entry codec. This is deliberately narrower than
    // ReadEntryToLocalFile: grouped slices and logical sidecars must continue
    // through the decoded entry path. Implementations that do not expose the
    // legacy raw-file transport reject this operation. Publication is staged
    // beside the destination and atomic, so failure preserves an existing
    // destination.
    virtual void
    ReadRawEntryToLocalFile(std::string_view name,
                            const std::string& local_path) = 0;

    // Materialize several logical entries into one file in exactly the order
    // supplied. This is the vector mmap concatenation contract.
    virtual void
    ReadEntriesToLocalFile(const std::vector<std::string>& names,
                           const std::string& local_path) = 0;

    // Materialize a set of entries into a local directory, returning their
    // local paths in the order requested. This is the mmap path and the tantivy
    // directory path. Destinations must have no concurrent writers; all entries
    // are staged before publication and existing files are restored if the
    // publication fails.
    virtual std::vector<std::string>
    ReadEntriesToLocalDir(const std::vector<std::string>& names,
                          const std::string& local_dir) = 0;

    // Metadata written through `FileSink::PutMeta`. Readable WITHOUT reading any
    // payload entry — this is what lets a Loader decide which concrete reader to
    // build (§6.3 hybrid) and what lets the inventory derive `ReaderCaps` from
    // load-time metadata alone, before anything is pinned (§4.1/§4.3).
    virtual std::optional<nlohmann::json>
    GetMeta(std::string_view key) const = 0;

    // Position-bearing access for DiskANN streaming. The loader may pass the
    // FileManager context to knowhere without copying index bytes.
    virtual const FileManagerContext&
    Context() const = 0;

    virtual const std::vector<std::string>&
    RemotePaths() const = 0;
};

class V1RemoteSource final : public FileSource {
 public:
    V1RemoteSource(
        const FileManagerContext& context,
        std::vector<std::string> remote_paths,
        LoadOptions options = {},
        ArtifactStoragePath storage_path = ArtifactStoragePath::Index,
        V1SourceLayout layout = V1SourceLayout::MemoryEntries);
    ~V1RemoteSource() override;

    Generation
    Gen() const override;
    std::vector<std::string>
    EntryNames() const override;
    bool
    HasEntry(std::string_view name) const override;
    int64_t
    EntrySize(std::string_view name) const override;
    std::vector<uint8_t>
    ReadEntry(std::string_view name) override;
    void
    ReadEntryToLocalFile(std::string_view name,
                         const std::string& local_path) override;
    void
    ReadRawEntryToLocalFile(std::string_view name,
                            const std::string& local_path) override;
    void
    ReadEntriesToLocalFile(const std::vector<std::string>& names,
                           const std::string& local_path) override;
    std::vector<std::string>
    ReadEntriesToLocalDir(const std::vector<std::string>& names,
                          const std::string& local_dir) override;
    std::optional<nlohmann::json>
    GetMeta(std::string_view key) const override;
    const FileManagerContext&
    Context() const override;
    const std::vector<std::string>&
    RemotePaths() const override;

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

class V3PackedSource final : public FileSource {
 public:
    V3PackedSource(
        const FileManagerContext& context,
        std::vector<std::string> remote_paths,
        LoadOptions options = {},
        ArtifactStoragePath storage_path = ArtifactStoragePath::Index);
    ~V3PackedSource() override;

    Generation
    Gen() const override;
    std::vector<std::string>
    EntryNames() const override;
    bool
    HasEntry(std::string_view name) const override;
    int64_t
    EntrySize(std::string_view name) const override;
    std::vector<uint8_t>
    ReadEntry(std::string_view name) override;
    void
    ReadEntryToLocalFile(std::string_view name,
                         const std::string& local_path) override;
    void
    ReadRawEntryToLocalFile(std::string_view name,
                            const std::string& local_path) override;
    void
    ReadEntriesToLocalFile(const std::vector<std::string>& names,
                           const std::string& local_path) override;
    std::vector<std::string>
    ReadEntriesToLocalDir(const std::vector<std::string>& names,
                          const std::string& local_dir) override;
    std::optional<nlohmann::json>
    GetMeta(std::string_view key) const override;
    const FileManagerContext&
    Context() const override;
    const std::vector<std::string>&
    RemotePaths() const override;

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

class NamedBufferSource final : public FileSource {
 public:
    explicit NamedBufferSource(const NamedBufferSet& buffers);
    ~NamedBufferSource() override;

    Generation
    Gen() const override;
    std::vector<std::string>
    EntryNames() const override;
    bool
    HasEntry(std::string_view name) const override;
    int64_t
    EntrySize(std::string_view name) const override;
    std::vector<uint8_t>
    ReadEntry(std::string_view name) override;
    void
    ReadEntryToLocalFile(std::string_view name,
                         const std::string& local_path) override;
    void
    ReadRawEntryToLocalFile(std::string_view name,
                            const std::string& local_path) override;
    void
    ReadEntriesToLocalFile(const std::vector<std::string>& names,
                           const std::string& local_path) override;
    std::vector<std::string>
    ReadEntriesToLocalDir(const std::vector<std::string>& names,
                          const std::string& local_dir) override;
    std::optional<nlohmann::json>
    GetMeta(std::string_view key) const override;
    const FileManagerContext&
    Context() const override;
    const std::vector<std::string>&
    RemotePaths() const override;

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace milvus::storage
