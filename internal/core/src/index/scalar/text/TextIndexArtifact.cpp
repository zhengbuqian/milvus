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

#include "index/scalar/text/TextIndexArtifact.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "index/scalar/text/TextIndexReader.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

constexpr std::string_view kNullOffsetsEntry = "index_null_offset";
constexpr std::string_view kFileNamesMeta = "file_names";
constexpr std::string_view kHasNullMeta = "has_null";

std::string
SafePrefix(std::string_view unique_id) {
    std::string prefix;
    prefix.reserve(std::min<size_t>(unique_id.size(), 40));
    for (const auto value : unique_id) {
        if (prefix.size() == 40) {
            break;
        }
        const auto byte = static_cast<unsigned char>(value);
        prefix.push_back(
            std::isalnum(byte) || value == '-' || value == '_' ? value : '_');
    }
    return prefix.empty() ? "index" : prefix;
}

std::vector<std::filesystem::path>
IndexFiles(const std::string& directory) {
    std::vector<std::filesystem::path> files;
    std::error_code error;
    for (std::filesystem::directory_iterator it(directory, error), end;
         !error && it != end;
         it.increment(error)) {
        if (it->is_regular_file(error)) {
            files.push_back(it->path());
        } else if (error) {
            break;
        }
    }
    if (error) {
        ThrowInfo(FileReadFailed,
                  "failed to enumerate text index directory {}: {}",
                  directory,
                  error.message());
    }
    std::sort(files.begin(), files.end());
    return files;
}

void
ValidateNullOffsets(const std::vector<size_t>& offsets, int64_t count) {
    size_t previous = 0;
    bool first = true;
    for (const auto offset : offsets) {
        if ((!first && offset <= previous) ||
            offset >= static_cast<size_t>(count)) {
            ThrowInfo(DataFormatBroken,
                      "invalid text null offset {} for count {}",
                      offset,
                      count);
        }
        previous = offset;
        first = false;
    }
}

void
ValidateArtifactState(
    const std::shared_ptr<TextIndexDirectory>& directory,
    const std::shared_ptr<milvus::tantivy::TantivyIndexWrapper>& engine,
    const std::shared_ptr<const std::vector<size_t>>& null_offsets,
    int64_t count,
    bool reader_file_backed) {
    AssertInfo(engine != nullptr, "text artifact requires a reader engine");
    AssertInfo(null_offsets != nullptr,
               "text artifact requires immutable null offsets");
    AssertInfo(count >= 0, "text artifact count must be non-negative");
    AssertInfo(!reader_file_backed || directory != nullptr,
               "file-backed text artifact reader requires a directory owner");
}

}  // namespace

TextIndexDirectory::TextIndexDirectory(std::string path)
    : path_(std::move(path)) {
}

std::shared_ptr<TextIndexDirectory>
TextIndexDirectory::Create(const std::string& parent,
                           std::string_view unique_id) {
    std::error_code error;
    auto root = parent.empty() ? std::filesystem::temp_directory_path(error)
                               : std::filesystem::path(parent);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to locate temporary directory for text index: {}",
                  error.message());
    }
    std::filesystem::create_directories(root, error);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to create text index staging root {}: {}",
                  root.string(),
                  error.message());
    }

    auto pattern =
        (root / ("text_" + SafePrefix(unique_id) + "_XXXXXX")).string();
    // Allocate the lifetime owner before acquiring the directory. Once
    // mkdtemp succeeds, no allocation is needed to make cleanup effective.
    auto result = std::shared_ptr<TextIndexDirectory>(
        new TextIndexDirectory(std::move(pattern)));
    if (::mkdtemp(result->path_.data()) == nullptr) {
        ThrowInfo(FileCreateFailed,
                  "failed to create text index staging directory in {}: {}",
                  root.string(),
                  std::strerror(errno));
    }
    result->created_ = true;
    return result;
}

TextIndexDirectory::~TextIndexDirectory() {
    if (created_ && !path_.empty()) {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
}

const std::string&
TextIndexDirectory::Path() const {
    return path_;
}

size_t
TextIndexDirectory::ByteSize() const {
    size_t total = 0;
    for (const auto& file : IndexFiles(path_)) {
        std::error_code error;
        const auto bytes = std::filesystem::file_size(file, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to inspect text index entry {}: {}",
                      file.string(),
                      error.message());
        }
        if (bytes > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken,
                      "text index directory byte size overflows size_t");
        }
        total += static_cast<size_t>(bytes);
    }
    return total;
}

size_t
TextIndexDirectory::HeapBytes() const {
    const auto inline_capacity = std::string{}.capacity();
    const auto path_bytes =
        path_.capacity() > inline_capacity ? path_.capacity() + 1 : 0;
    return sizeof(TextIndexDirectory) + path_bytes;
}

TextIndexArtifact::TextIndexArtifact(
    std::shared_ptr<TextIndexDirectory> directory,
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::vector<size_t> null_offsets,
    int64_t count,
    DataType value_type,
    bool reader_file_backed,
    size_t payload_bytes)
    : directory_(std::move(directory)),
      engine_(std::move(engine)),
      null_offsets_(
          std::make_shared<const std::vector<size_t>>(std::move(null_offsets))),
      count_(count),
      value_type_(value_type),
      reader_file_backed_(reader_file_backed),
      payload_bytes_(payload_bytes) {
    ValidateArtifactState(
        directory_, engine_, null_offsets_, count_, reader_file_backed_);
    ValidateNullOffsets(*null_offsets_, count_);
}

TextIndexArtifact::TextIndexArtifact(
    std::shared_ptr<TextIndexDirectory> directory,
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    int64_t count,
    DataType value_type,
    bool reader_file_backed,
    size_t payload_bytes)
    : directory_(std::move(directory)),
      engine_(std::move(engine)),
      null_offsets_(std::move(null_offsets)),
      count_(count),
      value_type_(value_type),
      reader_file_backed_(reader_file_backed),
      payload_bytes_(payload_bytes) {
    ValidateArtifactState(
        directory_, engine_, null_offsets_, count_, reader_file_backed_);
}

TextIndexArtifact::~TextIndexArtifact() = default;

std::shared_ptr<storage::LoadedArtifact>
TextIndexArtifact::OpenReader() const {
    return std::make_shared<TextIndexReader>(
        reader_file_backed_ ? directory_ : nullptr,
        engine_,
        count_,
        value_type_,
        reader_file_backed_,
        payload_bytes_);
}

void
TextIndexArtifact::Serialize(storage::FileSink& sink) const {
    // The baseline sealed RAM interim path is never exported, and the wrapper
    // exposes no RAM-directory serialization API.
    AssertInfo(directory_ != nullptr,
               "sealed RAM text artifacts cannot be serialized");

    const auto files = IndexFiles(directory_->Path());
    AssertInfo(!files.empty(), "text artifact has no Tantivy engine files");
    if (sink.Gen() == storage::Generation::V3) {
        std::vector<std::string> file_names;
        file_names.reserve(files.size());
        for (const auto& file : files) {
            file_names.push_back(file.filename().string());
        }
        // Exact inherited V3 metadata contract. Text adds no family, type, or
        // count markers of its own.
        sink.PutMeta(kFileNamesMeta, nlohmann::json(file_names));
        sink.PutMeta(kHasNullMeta, nlohmann::json(!null_offsets_->empty()));
    }

    for (const auto& file : files) {
        sink.WriteEntryFromLocalFile(file.filename().string(), file.string());
    }
    if (!null_offsets_->empty()) {
        AssertInfo(null_offsets_->size() <=
                       std::numeric_limits<size_t>::max() / sizeof(size_t),
                   "text null-offset byte size overflows size_t");
        sink.WriteEntry(kNullOffsetsEntry,
                        null_offsets_->data(),
                        null_offsets_->size() * sizeof(size_t));
    }
}

}  // namespace milvus::index
