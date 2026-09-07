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
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "nlohmann/json_fwd.hpp"
#include "storage/artifact/ArtifactStats.h"
#include "storage/artifact/NamedBuffer.h"

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
// It accepts named entries and finishes the selected physical transport.
// Upload sequencing belongs to the indexbuilder service; remote naming,
// slicing, encryption and byte writes belong to the concrete sink.
//
// The shape below is the entry model that `storage/IndexEntryWriter.h` already
// implements (named entries + a meta blob), so this is a narrowing of something
// that exists rather than a new format.
//
namespace milvus::storage {

struct FileManagerContext;

enum class Generation {
    V1V2,
    V3,
};

// Selects the existing remote namespace. This affects location only and is not
// written into an artifact.
enum class ArtifactStoragePath {
    Index,
    TextLog,
};

class FileSink {
 public:
    virtual ~FileSink() = default;

    virtual Generation
    Gen() const = 0;

    // An entry that is already a buffer in memory.
    // Covers neutral named-buffer sets and every scalar family whose serialized
    // form is a set of named buffers (bitmap, sort, marisa, RTree).
    virtual void
    WriteEntry(std::string_view name, const void* data, size_t size) = 0;

    // An entry that is already a file on local disk, borrowed by path. The
    // sink may read or copy it but never removes or mutates the caller's file.
    // Covers: the tantivy families, which write their own directory during
    // Build and whose `Serialize` is only "hand over the files that are already
    // there" (§6 reason 2), and DiskANN, whose artifact is a large local file
    // that must never be fully resident (§6.1.1 form D).
    virtual void
    WriteEntryFromLocalFile(std::string_view name,
                            const std::string& local_path) = 0;

    // A legacy engine object that must remain one raw, unsliced remote file.
    // The local path is borrowed exactly like WriteEntryFromLocalFile. This is
    // intentionally a separate operation: transport selection is explicit and
    // must never be inferred from an entry name.
    virtual void
    WriteRawEntryFromLocalFile(std::string_view name,
                               const std::string& local_path) = 0;

    // Existing V3 non-payload metadata the Loader needs while opening. Values
    // retain their JSON type. Serializers must not introduce new persistent
    // keys through this interface.
    virtual void
    PutMeta(std::string_view key, const nlohmann::json& value) = 0;

    // Close the artifact and report what was written.
    virtual ArtifactStats
    Finish() = 0;

    // Release staging owned by this sink after the caller has consumed the
    // published artifact. Implementations are idempotent and never remove a
    // path supplied to WriteEntryFromLocalFile.
    virtual void
    ReleaseLocalStaging() = 0;
};

class V1MemSink final : public FileSink {
 public:
    explicit V1MemSink(
        const FileManagerContext& context,
        ArtifactStoragePath storage_path = ArtifactStoragePath::Index);
    ~V1MemSink() override;

    Generation
    Gen() const override;
    void
    WriteEntry(std::string_view name, const void* data, size_t size) override;
    void
    WriteEntryFromLocalFile(std::string_view name,
                            const std::string& local_path) override;
    void
    WriteRawEntryFromLocalFile(std::string_view name,
                               const std::string& local_path) override;
    void
    PutMeta(std::string_view key, const nlohmann::json& value) override;
    ArtifactStats
    Finish() override;
    void
    ReleaseLocalStaging() override;

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

class V1DiskSink final : public FileSink {
 public:
    explicit V1DiskSink(
        const FileManagerContext& context,
        ArtifactStoragePath storage_path = ArtifactStoragePath::Index);
    ~V1DiskSink() override;

    Generation
    Gen() const override;
    void
    WriteEntry(std::string_view name, const void* data, size_t size) override;
    void
    WriteEntryFromLocalFile(std::string_view name,
                            const std::string& local_path) override;
    void
    WriteRawEntryFromLocalFile(std::string_view name,
                               const std::string& local_path) override;
    void
    PutMeta(std::string_view key, const nlohmann::json& value) override;
    ArtifactStats
    Finish() override;
    void
    ReleaseLocalStaging() override;

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

class V3PackedSink final : public FileSink {
 public:
    V3PackedSink(const FileManagerContext& context,
                 std::string file_name,
                 ArtifactStoragePath storage_path = ArtifactStoragePath::Index);
    ~V3PackedSink() override;

    Generation
    Gen() const override;
    void
    WriteEntry(std::string_view name, const void* data, size_t size) override;
    void
    WriteEntryFromLocalFile(std::string_view name,
                            const std::string& local_path) override;
    void
    WriteRawEntryFromLocalFile(std::string_view name,
                               const std::string& local_path) override;
    void
    PutMeta(std::string_view key, const nlohmann::json& value) override;
    ArtifactStats
    Finish() override;
    void
    ReleaseLocalStaging() override;

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

class NamedBufferSink final : public FileSink {
 public:
    NamedBufferSink();
    ~NamedBufferSink() override;

    Generation
    Gen() const override;
    void
    WriteEntry(std::string_view name, const void* data, size_t size) override;
    void
    WriteEntryFromLocalFile(std::string_view name,
                            const std::string& local_path) override;
    void
    WriteRawEntryFromLocalFile(std::string_view name,
                               const std::string& local_path) override;
    void
    PutMeta(std::string_view key, const nlohmann::json& value) override;
    ArtifactStats
    Finish() override;
    void
    ReleaseLocalStaging() override;

    // The physical named buffers are valid after Finish(). Large logical
    // entries retain the existing sliced names and slice-meta entry.
    const NamedBufferSet&
    Data() const;

    NamedBufferSet
    Take();

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace milvus::storage
