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

#include "index/scalar/ngram/NgramIndexArtifact.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <utility>

#include "common/EasyAssert.h"
#include "index/Utils.h"
#include "index/scalar/ngram/NgramIndexReader.h"
#include "nlohmann/json.hpp"
#include "tantivy-wrapper.h"

namespace milvus::index {
namespace {

constexpr std::string_view kNullOffsetsEntry = "index_null_offset";
constexpr std::string_view kAvgRowSizeEntry = "ngram_avg_row_size";
constexpr std::string_view kFileNamesMeta = "file_names";
constexpr std::string_view kHasNullMeta = "has_null";

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
                  "failed to enumerate NGRAM directory {}: {}",
                  directory,
                  error.message());
    }
    std::sort(files.begin(), files.end());
    return files;
}

}  // namespace

NgramIndexDirectory::NgramIndexDirectory(std::string path)
    : path_(std::move(path)) {
}

std::shared_ptr<NgramIndexDirectory>
NgramIndexDirectory::Create(const std::string& parent) {
    std::error_code error;
    auto root = parent.empty() ? std::filesystem::temp_directory_path(error)
                               : std::filesystem::path(parent);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to locate temporary directory for NGRAM: {}",
                  error.message());
    }
    std::filesystem::create_directories(root, error);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to create NGRAM staging root {}: {}",
                  root.string(),
                  error.message());
    }

    auto result = std::shared_ptr<NgramIndexDirectory>(
        new NgramIndexDirectory((root / "ngram_XXXXXX").string()));
    if (::mkdtemp(result->path_.data()) == nullptr) {
        ThrowInfo(FileCreateFailed,
                  "failed to create NGRAM staging directory in {}: {}",
                  root.string(),
                  std::strerror(errno));
    }
    result->created_ = true;
    return result;
}

NgramIndexDirectory::~NgramIndexDirectory() {
    if (created_ && !path_.empty()) {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
}

const std::string&
NgramIndexDirectory::Path() const {
    return path_;
}

size_t
NgramIndexDirectory::ByteSize() const {
    size_t total = 0;
    for (const auto& file : IndexFiles(path_)) {
        std::error_code error;
        const auto bytes = std::filesystem::file_size(file, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to inspect NGRAM entry {}: {}",
                      file.string(),
                      error.message());
        }
        if (bytes > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken,
                      "NGRAM directory byte size overflows size_t");
        }
        total += static_cast<size_t>(bytes);
    }
    return total;
}

size_t
NgramIndexDirectory::HeapBytes() const {
    const auto inline_capacity = std::string{}.capacity();
    const auto path_bytes =
        path_.capacity() > inline_capacity ? path_.capacity() + 1 : 0;
    return sizeof(NgramIndexDirectory) + path_bytes;
}

NgramIndexArtifact::NgramIndexArtifact(
    std::shared_ptr<NgramIndexDirectory> directory,
    std::vector<size_t> null_offsets,
    DataType value_type,
    uintptr_t min_gram,
    uintptr_t max_gram,
    size_t avg_row_size)
    : NgramIndexArtifact(
          std::move(directory),
          std::make_shared<const std::vector<size_t>>(std::move(null_offsets)),
          value_type,
          min_gram,
          max_gram,
          avg_row_size) {
}

NgramIndexArtifact::NgramIndexArtifact(
    std::shared_ptr<NgramIndexDirectory> directory,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    DataType value_type,
    uintptr_t min_gram,
    uintptr_t max_gram,
    size_t avg_row_size)
    : directory_(std::move(directory)),
      null_offsets_(std::move(null_offsets)),
      value_type_(value_type),
      min_gram_(min_gram),
      max_gram_(max_gram),
      avg_row_size_(avg_row_size) {
    AssertInfo(directory_ != nullptr,
               "NGRAM artifact requires an owned directory");
    AssertInfo(null_offsets_ != nullptr,
               "NGRAM artifact requires immutable null offsets");
}

NgramIndexArtifact::~NgramIndexArtifact() = default;

std::shared_ptr<storage::LoadedArtifact>
NgramIndexArtifact::OpenReader() const {
    auto engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
        directory_->Path().c_str(), true, SetBitsetSealed);
    return std::make_shared<NgramIndexReader>(directory_,
                                              std::move(engine),
                                              null_offsets_,
                                              value_type_,
                                              min_gram_,
                                              max_gram_,
                                              avg_row_size_,
                                              true,
                                              directory_->ByteSize());
}

void
NgramIndexArtifact::Serialize(storage::FileSink& sink) const {
    const auto files = IndexFiles(directory_->Path());
    AssertInfo(!files.empty(), "NGRAM artifact has no Tantivy engine files");

    if (sink.Gen() == storage::Generation::V3) {
        std::vector<std::string> file_names;
        file_names.reserve(files.size());
        for (const auto& file : files) {
            file_names.push_back(file.filename().string());
        }
        // Exact inherited V3 Tantivy metadata. min/max gram, type, path and
        // count remain runtime-only just as in the baseline format.
        sink.PutMeta(kFileNamesMeta, nlohmann::json(file_names));
        sink.PutMeta(kHasNullMeta, nlohmann::json(!null_offsets_->empty()));
    }

    for (const auto& file : files) {
        sink.WriteEntryFromLocalFile(file.filename().string(), file.string());
    }
    if (!null_offsets_->empty()) {
        if (null_offsets_->size() >
            std::numeric_limits<size_t>::max() / sizeof(size_t)) {
            ThrowInfo(DataFormatBroken,
                      "NGRAM null-offset byte size overflows");
        }
        sink.WriteEntry(kNullOffsetsEntry,
                        null_offsets_->data(),
                        null_offsets_->size() * sizeof(size_t));
    }
    // The baseline V1/V2 upload never emitted this sidecar and its loader used
    // the 5000-byte default. V3 has always written one native size_t.
    if (sink.Gen() == storage::Generation::V3) {
        sink.WriteEntry(
            kAvgRowSizeEntry, &avg_row_size_, sizeof(avg_row_size_));
    }
}

}  // namespace milvus::index
