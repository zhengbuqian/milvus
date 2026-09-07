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

#include "index/scalar/inverted/InvertedIndexArtifact.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <limits>
#include <unistd.h>
#include <utility>

#include "common/EasyAssert.h"
#include "index/Utils.h"
#include "index/scalar/inverted/InvertedIndexReader.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

constexpr std::string_view kNullOffsetsEntry = "index_null_offset";
constexpr std::string_view kFileNamesMeta = "file_names";
constexpr std::string_view kHasNullMeta = "has_null";

class EmptyDirectoryGuard {
 public:
    explicit EmptyDirectoryGuard(const char* path) : path_(path) {
    }

    ~EmptyDirectoryGuard() {
        if (path_ != nullptr) {
            rmdir(path_);
        }
    }

    void
    Release() {
        path_ = nullptr;
    }

 private:
    const char* path_;
};

size_t
DirectoryBytes(const std::string& directory) {
    size_t total = 0;
    std::error_code error;
    for (std::filesystem::directory_iterator it(directory, error), end;
         !error && it != end;
         it.increment(error)) {
        if (!it->is_regular_file(error)) {
            if (error) {
                break;
            }
            continue;
        }
        const auto bytes = it->file_size(error);
        if (error) {
            break;
        }
        if (bytes > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken,
                      "inverted directory byte size overflows size_t");
        }
        total += static_cast<size_t>(bytes);
    }
    if (error) {
        ThrowInfo(FileReadFailed,
                  "failed to inspect inverted directory {}: {}",
                  directory,
                  error.message());
    }
    return total;
}

std::vector<std::filesystem::path>
IndexFiles(const std::string& directory) {
    std::vector<std::filesystem::path> files;
    std::error_code error;
    for (std::filesystem::directory_iterator it(directory, error), end;
         !error && it != end;
         it.increment(error)) {
        if (!it->is_directory(error)) {
            if (error) {
                break;
            }
            files.push_back(it->path());
        }
    }
    if (error) {
        ThrowInfo(FileReadFailed,
                  "failed to enumerate inverted directory {}: {}",
                  directory,
                  error.message());
    }
    std::sort(files.begin(), files.end());
    return files;
}

std::shared_ptr<storage::LoadedArtifact>
MakeReader(std::shared_ptr<InvertedIndexDirectory> directory,
           std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
           std::shared_ptr<const std::vector<size_t>> null_offsets,
           DataType value_type,
           bool nested,
           bool mmap,
           size_t engine_bytes) {
    const auto engine_path_bytes = directory->PathHeapBytes();
    const auto make = [&]<typename T>() {
        return std::make_shared<InvertedIndexReader<T>>(directory,
                                                        engine,
                                                        null_offsets,
                                                        value_type,
                                                        nested,
                                                        mmap,
                                                        engine_bytes,
                                                        engine_path_bytes);
    };
    switch (value_type) {
        case DataType::BOOL:
            return make.template operator()<bool>();
        case DataType::INT8:
            return make.template operator()<int8_t>();
        case DataType::INT16:
            return make.template operator()<int16_t>();
        case DataType::INT32:
            return make.template operator()<int32_t>();
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return make.template operator()<int64_t>();
        case DataType::FLOAT:
            return make.template operator()<float>();
        case DataType::DOUBLE:
            return make.template operator()<double>();
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
            return make.template operator()<std::string_view>();
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported inverted value type {}",
                      static_cast<int>(value_type));
    }
}

}  // namespace

InvertedIndexDirectory::InvertedIndexDirectory(std::string path)
    : path_(std::move(path)) {
}

std::shared_ptr<InvertedIndexDirectory>
InvertedIndexDirectory::Create(const std::string& parent) {
    std::error_code error;
    auto root = parent.empty() ? std::filesystem::temp_directory_path(error)
                               : std::filesystem::path(parent);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to locate temporary directory for inverted index: "
                  "{}",
                  error.message());
    }
    std::filesystem::create_directories(root, error);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to create inverted staging root {}: {}",
                  root.string(),
                  error.message());
    }
    auto pattern = (root / "inverted_XXXXXX").string();
    std::vector<char> mutable_pattern(pattern.begin(), pattern.end());
    mutable_pattern.push_back('\0');
    if (mkdtemp(mutable_pattern.data()) == nullptr) {
        ThrowInfo(FileCreateFailed,
                  "failed to create inverted staging directory in {}: {}",
                  root.string(),
                  std::strerror(errno));
    }
    EmptyDirectoryGuard guard(mutable_pattern.data());
    auto result = std::shared_ptr<InvertedIndexDirectory>(
        new InvertedIndexDirectory(mutable_pattern.data()));
    guard.Release();
    return result;
}

InvertedIndexDirectory::~InvertedIndexDirectory() {
    if (!path_.empty()) {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }
}

const std::string&
InvertedIndexDirectory::Path() const {
    return path_;
}

size_t
InvertedIndexDirectory::PathHeapBytes() const {
    const auto inline_capacity = std::string{}.capacity();
    if (path_.capacity() <= inline_capacity) {
        return 0;
    }
    if (path_.capacity() == std::numeric_limits<size_t>::max()) {
        ThrowInfo(DataFormatBroken,
                  "inverted directory path memory size overflows");
    }
    return path_.capacity() + 1;
}

size_t
InvertedIndexDirectory::HeapBytes() const {
    const auto path_bytes = PathHeapBytes();
    if (path_bytes >
        std::numeric_limits<size_t>::max() - sizeof(InvertedIndexDirectory)) {
        ThrowInfo(DataFormatBroken, "inverted directory memory size overflows");
    }
    return sizeof(InvertedIndexDirectory) + path_bytes;
}

InvertedIndexArtifact::InvertedIndexArtifact(
    std::shared_ptr<InvertedIndexDirectory> directory,
    std::vector<size_t> null_offsets,
    DataType value_type,
    bool nested)
    : InvertedIndexArtifact(
          std::move(directory),
          std::make_shared<const std::vector<size_t>>(std::move(null_offsets)),
          value_type,
          nested) {
}

InvertedIndexArtifact::InvertedIndexArtifact(
    std::shared_ptr<InvertedIndexDirectory> directory,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    DataType value_type,
    bool nested)
    : directory_(std::move(directory)),
      null_offsets_(std::move(null_offsets)),
      value_type_(value_type),
      nested_(nested) {
    AssertInfo(directory_ != nullptr,
               "inverted artifact requires an owned directory");
    AssertInfo(null_offsets_ != nullptr,
               "inverted artifact requires immutable null offsets");
}

InvertedIndexArtifact::~InvertedIndexArtifact() = default;

std::shared_ptr<storage::LoadedArtifact>
InvertedIndexArtifact::OpenReader() const {
    auto engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
        directory_->Path().c_str(), true, SetBitsetSealed);
    return MakeReader(directory_,
                      std::move(engine),
                      null_offsets_,
                      value_type_,
                      nested_,
                      true,
                      DirectoryBytes(directory_->Path()));
}

void
InvertedIndexArtifact::Serialize(storage::FileSink& sink) const {
    const auto files = IndexFiles(directory_->Path());
    if (sink.Gen() == storage::Generation::V3) {
        std::vector<std::string> file_names;
        file_names.reserve(files.size());
        for (const auto& file : files) {
            file_names.push_back(file.filename().string());
        }
        sink.PutMeta(kFileNamesMeta, nlohmann::json(file_names));
        sink.PutMeta(kHasNullMeta, nlohmann::json(!null_offsets_->empty()));
    }

    for (const auto& file : files) {
        sink.WriteEntryFromLocalFile(file.filename().string(), file.string());
    }
    if (!null_offsets_->empty()) {
        sink.WriteEntry(kNullOffsetsEntry,
                        null_offsets_->data(),
                        null_offsets_->size() * sizeof(size_t));
    }
}

}  // namespace milvus::index
