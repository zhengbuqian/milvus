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
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/Types.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"

namespace milvus::tantivy {
struct TantivyIndexWrapper;
}

// The ARTIFACT of the text family: what `TextIndexBuilder::Seal()` produces.
//
// See 01-scalar-index.md §6 (why serialization lives on the artifact and not on
// a symmetric codec) and §11.2 rule 1 (the artifact pipeline sinks to L1, which
// is why the base class is `storage::Artifact` and not an `IndexArtifact`).
//
// Persisted text indexes are FILE-SHAPED: the tantivy writer has already put
// bytes into a local directory by the time `Seal()` returns, so `Serialize` is
// not an encoding step — it hands the existing file set to the sink. The
// sealed RAM interim mode is intentionally reader-only and cannot be exported.

namespace milvus::index {

// Owns one unique child below a caller-supplied parent. The parent is borrowed
// and is never removed. Artifact and mmap reader share this object so Tantivy's
// mapped files outlive every reader handle.
class TextIndexDirectory final {
 public:
    static std::shared_ptr<TextIndexDirectory>
    Create(const std::string& parent, std::string_view unique_id);

    ~TextIndexDirectory();

    TextIndexDirectory(const TextIndexDirectory&) = delete;
    TextIndexDirectory&
    operator=(const TextIndexDirectory&) = delete;

    const std::string&
    Path() const;

    size_t
    ByteSize() const;

    // Known C++ heap ownership only: this object plus any non-SSO path
    // allocation. Tantivy's reader/analyzer heap remains engine-owned and is
    // not exposed exactly by the wrapper.
    size_t
    HeapBytes() const;

 private:
    explicit TextIndexDirectory(std::string path);

    std::string path_;
    bool created_{false};
};

class TextIndexArtifact final : public storage::Artifact {
 public:
    TextIndexArtifact(
        std::shared_ptr<TextIndexDirectory> directory,
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::vector<size_t> null_offsets,
        int64_t count,
        DataType value_type,
        bool reader_file_backed,
        size_t payload_bytes);

    // Loaded rewrite path. null_offsets must already have been validated
    // against count by TextIndexLoader; this overload does not rescan them.
    TextIndexArtifact(
        std::shared_ptr<TextIndexDirectory> directory,
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::shared_ptr<const std::vector<size_t>> null_offsets,
        int64_t count,
        DataType value_type,
        bool reader_file_backed,
        size_t payload_bytes);

    ~TextIndexArtifact() override;

    // In-place open, without a round trip through storage — the sealed interim
    // build path (`ChunkedSegmentSealedImpl.cpp:5374`) uses this and never
    // serializes at all. Paired with `TextIndexLoader::OpenIndex`, which is the
    // same reader reached from bytes (§6.2: "the method is named Open, not
    // Deserialize, and pairs with Artifact::OpenReader").
    std::unique_ptr<storage::LoadedArtifact>
    OpenReader() const override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    // Declared before engine_ so the engine is destroyed before its backing
    // directory when this artifact is the last owner.
    std::shared_ptr<TextIndexDirectory> directory_;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::shared_ptr<const std::vector<size_t>> null_offsets_;
    int64_t count_{0};
    DataType value_type_{DataType::NONE};
    // Whether readers depend on directory_. A loaded RAM rewrite keeps the
    // directory only for publication while its readers share the RAM engine.
    bool reader_file_backed_{false};
    // Managed Tantivy payload bytes. File-backed payload is charged to the
    // file tier; RAM-directory payload is charged to the memory tier.
    size_t payload_bytes_{0};
};

}  // namespace milvus::index
