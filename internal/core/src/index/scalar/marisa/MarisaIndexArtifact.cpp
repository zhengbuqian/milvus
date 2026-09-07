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

#include "index/scalar/marisa/MarisaIndexArtifact.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <limits>
#include <string_view>
#include <unistd.h>
#include <utility>

#include "common/EasyAssert.h"
#include "index/Meta.h"
#include "index/scalar/marisa/MarisaIndexReader.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

constexpr uint32_t kCsrFormatVersion = 1;
constexpr std::string_view kCsrFormatVersionMeta = "marisa_csr_format_version";
constexpr std::string_view kCsrNumKeysMeta = "csr_num_keys";

class LocalFileGuard {
 public:
    explicit LocalFileGuard(std::string path) : path_(std::move(path)) {
    }

    LocalFileGuard(const LocalFileGuard&) = delete;
    LocalFileGuard&
    operator=(const LocalFileGuard&) = delete;

    LocalFileGuard(LocalFileGuard&& other) noexcept
        : path_(std::exchange(other.path_, {})) {
    }

    ~LocalFileGuard() {
        if (!path_.empty()) {
            unlink(path_.c_str());
        }
    }

    const std::string&
    Path() const {
        return path_;
    }

 private:
    std::string path_;
};

std::pair<int, LocalFileGuard>
CreateTrieFile() {
    auto pattern =
        (std::filesystem::temp_directory_path() / "marisa_trie_XXXXXX")
            .string();
    std::vector<char> mutable_pattern(pattern.begin(), pattern.end());
    mutable_pattern.push_back('\0');
    const auto fd = mkstemp(mutable_pattern.data());
    if (fd == -1) {
        ThrowInfo(FileCreateFailed,
                  "failed to create marisa trie file: {}",
                  std::strerror(errno));
    }
    return {fd, LocalFileGuard(mutable_pattern.data())};
}

size_t
CheckedBytes(size_t count, size_t width, std::string_view entry) {
    AssertInfo(
        width == 0 || count <= std::numeric_limits<size_t>::max() / width,
        "marisa entry {} byte size overflows size_t",
        entry);
    return count * width;
}

void
ValidateStorage(const MarisaIndexStorage& storage) {
    AssertInfo(storage.trie != nullptr, "marisa artifact requires a trie");
    AssertInfo(storage.value_type == DataType::STRING ||
                   storage.value_type == DataType::VARCHAR ||
                   storage.value_type == DataType::TEXT,
               "marisa artifact requires STRING, VARCHAR, or TEXT");
    AssertInfo(storage.csr_num_keys == storage.trie->num_keys(),
               "marisa artifact CSR key count mismatch");
    AssertInfo(storage.str_ids_size == 0 || storage.str_ids != nullptr,
               "marisa artifact is missing row ids");
    AssertInfo(storage.csr_index != nullptr,
               "marisa artifact is missing CSR index");
    AssertInfo(storage.csr_index[storage.csr_num_keys] == 0 ||
                   storage.csr_offsets != nullptr,
               "marisa artifact is missing CSR offsets");
}

std::shared_ptr<const MarisaIndexStorage>
MakeBuilderStorage(std::shared_ptr<marisa::Trie> trie,
                   std::vector<int64_t> str_ids,
                   std::vector<uint32_t> csr_index,
                   std::vector<uint32_t> csr_offsets,
                   DataType value_type) {
    auto storage = std::make_shared<MarisaIndexStorage>();
    storage->str_ids_owner =
        std::make_shared<const std::vector<int64_t>>(std::move(str_ids));
    storage->csr_index_owner =
        std::make_shared<const std::vector<uint32_t>>(std::move(csr_index));
    storage->csr_offsets_owner =
        std::make_shared<const std::vector<uint32_t>>(std::move(csr_offsets));
    storage->trie = std::move(trie);
    storage->str_ids = storage->str_ids_owner->data();
    storage->str_ids_size = storage->str_ids_owner->size();
    storage->csr_index = storage->csr_index_owner->data();
    storage->csr_offsets = storage->csr_offsets_owner->data();
    storage->csr_num_keys =
        storage->trie == nullptr ? 0 : storage->trie->num_keys();
    storage->value_type = value_type;
    AssertInfo(storage->csr_num_keys < std::numeric_limits<size_t>::max(),
               "marisa artifact CSR index count overflows size_t");
    AssertInfo(storage->csr_index_owner->size() == storage->csr_num_keys + 1,
               "invalid marisa artifact CSR index size");
    ValidateStorage(*storage);
    AssertInfo(storage->csr_offsets_owner->size() ==
                   storage->csr_index[storage->csr_num_keys],
               "invalid marisa artifact CSR offsets size");
    return storage;
}

}  // namespace

MarisaIndexArtifact::MarisaIndexArtifact(std::shared_ptr<marisa::Trie> trie,
                                         std::vector<int64_t> str_ids,
                                         std::vector<uint32_t> csr_index,
                                         std::vector<uint32_t> csr_offsets,
                                         DataType value_type)
    : MarisaIndexArtifact(MakeBuilderStorage(std::move(trie),
                                             std::move(str_ids),
                                             std::move(csr_index),
                                             std::move(csr_offsets),
                                             value_type)) {
}

MarisaIndexArtifact::MarisaIndexArtifact(
    std::shared_ptr<const MarisaIndexStorage> storage)
    : storage_(std::move(storage)) {
    AssertInfo(storage_ != nullptr, "marisa artifact requires shared storage");
    ValidateStorage(*storage_);
}

MarisaIndexArtifact::~MarisaIndexArtifact() = default;

std::shared_ptr<storage::LoadedArtifact>
MarisaIndexArtifact::OpenReader() const {
    return std::make_shared<MarisaIndexReader>(storage_);
}

void
MarisaIndexArtifact::Serialize(storage::FileSink& sink) const {
    auto [fd, file] = CreateTrieFile();
    try {
        storage_->trie->write(fd);
    } catch (...) {
        close(fd);
        throw;
    }
    if (close(fd) != 0) {
        ThrowInfo(FileWriteFailed,
                  "failed to close marisa trie file {}: {}",
                  file.Path(),
                  std::strerror(errno));
    }
    sink.WriteEntryFromLocalFile(MARISA_TRIE_INDEX, file.Path());

    sink.WriteEntry(
        MARISA_STR_IDS,
        storage_->str_ids,
        CheckedBytes(storage_->str_ids_size, sizeof(int64_t), MARISA_STR_IDS));

    // V1/V2 is exactly the historical two-entry BinarySet. CSR was introduced
    // only in the existing V3 entry format and must be rebuilt by legacy loads.
    if (sink.Gen() == storage::Generation::V1V2) {
        return;
    }

    AssertInfo(storage_->csr_num_keys < std::numeric_limits<size_t>::max(),
               "marisa CSR index count overflows size_t");
    const auto csr_index_count = storage_->csr_num_keys + 1;
    const auto csr_offsets_count =
        static_cast<size_t>(storage_->csr_index[storage_->csr_num_keys]);
    sink.WriteEntry(
        MARISA_CSR_INDEX,
        storage_->csr_index,
        CheckedBytes(csr_index_count, sizeof(uint32_t), MARISA_CSR_INDEX));
    sink.WriteEntry(
        MARISA_CSR_OFFSETS,
        storage_->csr_offsets,
        CheckedBytes(csr_offsets_count, sizeof(uint32_t), MARISA_CSR_OFFSETS));
    sink.PutMeta(kCsrFormatVersionMeta, nlohmann::json(kCsrFormatVersion));
    sink.PutMeta(kCsrNumKeysMeta, nlohmann::json(storage_->csr_num_keys));
}

}  // namespace milvus::index
