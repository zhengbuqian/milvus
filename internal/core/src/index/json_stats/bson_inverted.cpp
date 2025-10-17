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

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <tuple>

#include "common/ScopedTimer.h"
#include "monitor/Monitor.h"
#include "index/json_stats/bson_inverted.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/FileWriter.h"
#include <cstring>
namespace milvus::index {

BsonInvertedIndex::BsonInvertedIndex(const std::string& path,
                                     int64_t field_id,
                                     bool is_load,
                                     const storage::FileManagerContext& ctx,
                                     int64_t tantivy_index_version)
    : is_load_(is_load),
      field_id_(field_id),
      tantivy_index_version_(tantivy_index_version) {
    disk_file_manager_ =
        std::make_shared<milvus::storage::DiskFileManagerImpl>(ctx);
    if (is_load_) {
        auto prefix = disk_file_manager_->GetLocalJsonStatsSharedIndexPrefix();
        path_ = prefix;
        LOG_INFO("bson inverted index load path:{}", path_);
    } else {
        path_ = path;
        LOG_INFO("bson inverted index build path:{}", path_);
    }
}

BsonInvertedIndex::~BsonInvertedIndex() {
    if (wrapper_) {
        wrapper_->free();
    }
    if (!is_load_) {
        auto local_chunk_manager =
            milvus::storage::LocalChunkManagerSingleton::GetInstance()
                .GetChunkManager();
        auto prefix = path_;
        LOG_INFO("bson inverted index remove path:{}", path_);
        local_chunk_manager->RemoveDir(prefix);
    }
}

void
BsonInvertedIndex::AddRecord(const std::string& key,
                             uint32_t row_id,
                             uint32_t offset) {
    if (inverted_index_map_.find(key) == inverted_index_map_.end()) {
        inverted_index_map_[key] = {EncodeInvertedIndexValue(row_id, offset)};
    } else {
        inverted_index_map_[key].push_back(
            EncodeInvertedIndexValue(row_id, offset));
    }
}

void
BsonInvertedIndex::BuildIndex() {
    if (wrapper_ == nullptr) {
        if (tantivy_index_exist(path_.c_str())) {
            ThrowInfo(IndexBuildError,
                      "build inverted index temp dir:{} not empty",
                      path_);
        }
        auto field_name = std::to_string(field_id_) + "_" + "shared";
        wrapper_ = std::make_shared<TantivyIndexWrapper>(
            field_name.c_str(), path_.c_str(), tantivy_index_version_);
        LOG_INFO("build bson inverted index for field id:{} with dir:{}",
                 field_id_,
                 path_);
    }
    std::vector<const char*> keys;
    std::vector<const int64_t*> json_offsets;
    std::vector<uintptr_t> json_offsets_lens;
    for (const auto& [key, offsets] : inverted_index_map_) {
        keys.push_back(key.c_str());
        json_offsets.push_back(offsets.data());
        json_offsets_lens.push_back(offsets.size());
    }
    wrapper_->add_json_key_stats_data_by_batch(keys.data(),
                                               json_offsets.data(),
                                               json_offsets_lens.data(),
                                               keys.size());
}

void
BsonInvertedIndex::LoadIndex(const std::vector<std::string>& index_files,
                             milvus::proto::common::LoadPriority priority) {
    if (is_load_) {
        // If bundle present, stream and unpack; else fallback to caching files.
        auto bundle_it = std::find_if(
            index_files.begin(), index_files.end(), [](const std::string& f) {
                return boost::filesystem::path(f).filename().string() ==
                       TANTIVY_BUNDLE_FILE_NAME;
            });
        if (bundle_it != index_files.end()) {
            auto local_bundle_path =
                (boost::filesystem::path(path_) / TANTIVY_BUNDLE_FILE_NAME)
                    .string();
            // Download
            {
                auto remote_is =
                    disk_file_manager_->OpenInputStream(local_bundle_path);
                storage::FileWriter fw(local_bundle_path,
                                       storage::io::Priority::HIGH);
                const size_t buf_size = 1 << 20;
                std::vector<uint8_t> buf(buf_size);
                size_t total = remote_is->Size();
                size_t copied = 0;
                while (copied < total) {
                    size_t chunk = std::min(buf_size, total - copied);
                    auto n = remote_is->ReadAt(buf.data(), copied, chunk);
                    AssertInfo(n == chunk,
                               "failed to read remote bundle stream");
                    fw.Write(buf.data(), n);
                    copied += n;
                }
                fw.Finish();
            }
            // Unpack
            auto local_cm = milvus::storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
            const size_t buf_size = 1 << 20;
            auto read_exact = [&](uint64_t off, void* dst, size_t n) {
                local_cm->Read(local_bundle_path, off, dst, n);
            };
            uint64_t off = 0;
            char magic[8];
            read_exact(off, magic, sizeof(magic));
            off += sizeof(magic);
            AssertInfo(std::memcmp(magic, "TANTIVYB", 8) == 0,
                       "invalid tantivy bundle magic");
            uint32_t ver = 0;
            read_exact(off, &ver, sizeof(ver));
            off += sizeof(ver);
            AssertInfo(ver == TANTIVY_BUNDLE_FORMAT_VERSION,
                       "unsupported tantivy bundle version: {}",
                       ver);
            uint32_t file_cnt = 0;
            read_exact(off, &file_cnt, sizeof(file_cnt));
            off += sizeof(file_cnt);
            struct Header { std::string name; uint64_t offset; uint64_t size; };
            std::vector<Header> headers;
            headers.reserve(file_cnt);
            for (uint32_t i = 0; i < file_cnt; ++i) {
                uint32_t name_len = 0;
                read_exact(off, &name_len, sizeof(name_len));
                off += sizeof(name_len);
                std::string name;
                name.resize(name_len);
                if (name_len > 0) {
                    read_exact(off, name.data(), name_len);
                }
                off += name_len;
                uint64_t data_off = 0;
                read_exact(off, &data_off, sizeof(data_off));
                off += sizeof(data_off);
                uint64_t size = 0;
                read_exact(off, &size, sizeof(size));
                off += sizeof(size);
                headers.push_back({std::move(name), data_off, size});
            }
            for (auto& h : headers) {
                auto out_path =
                    (boost::filesystem::path(path_) / h.name).string();
                storage::FileWriter fw(out_path, storage::io::Priority::HIGH);
                uint64_t remaining = h.size;
                uint64_t cur = 0;
                std::vector<uint8_t> buf(buf_size);
                while (remaining > 0) {
                    auto to_read = static_cast<uint64_t>(
                        std::min<uint64_t>(buf_size, remaining));
                    local_cm->Read(local_bundle_path, h.offset + cur, buf.data(),
                                   to_read);
                    fw.Write(buf.data(), to_read);
                    remaining -= to_read;
                    cur += to_read;
                }
                fw.Finish();
            }
        } else {
            // Legacy path: cache shared_key_index files
            std::vector<std::string> remote_files;
            for (auto& file : index_files) {
                auto remote_prefix =
                    disk_file_manager_->GetRemoteJsonStatsLogPrefix();
                remote_files.emplace_back(remote_prefix + "/" + file);
            }
            disk_file_manager_->CacheJsonStatsSharedIndexToDisk(remote_files,
                                                                priority);
        }
        AssertInfo(tantivy_index_exist(path_.c_str()),
                   "index dir not exist: {}",
                   path_);
        wrapper_ = std::make_shared<TantivyIndexWrapper>(
            path_.c_str(), false, milvus::index::SetBitsetUnused);
        LOG_INFO("load json shared key index done for field id:{} with dir:{}",
                 field_id_,
                 path_);
    }
}

IndexStatsPtr
BsonInvertedIndex::UploadIndex() {
    AssertInfo(!is_load_, "upload index is not supported for load index");
    AssertInfo(wrapper_ != nullptr,
               "bson inverted index wrapper is not initialized");
    wrapper_->finish();

    // Pack and upload single object (bundle)
    const std::string bundle_local_path =
        (boost::filesystem::path(path_) / TANTIVY_BUNDLE_FILE_NAME).string();
    struct Entry { std::string name; uint64_t size; };
    std::vector<Entry> entries;
    {
        boost::filesystem::path p(path_);
        boost::filesystem::directory_iterator end_iter;
        for (boost::filesystem::directory_iterator iter(p); iter != end_iter;
             ++iter) {
            if (boost::filesystem::is_directory(*iter)) {
                continue;
            }
            auto filename = iter->path().filename().string();
            if (filename == TANTIVY_BUNDLE_FILE_NAME) {
                continue;
            }
            auto sz = boost::filesystem::file_size(*iter);
            entries.push_back({filename, static_cast<uint64_t>(sz)});
        }
        storage::FileWriter writer(bundle_local_path,
                                   storage::io::Priority::MIDDLE);
        const char magic[8] = {'T','A','N','T','I','V','Y','B'};
        writer.Write(magic, sizeof(magic));
        uint32_t ver = TANTIVY_BUNDLE_FORMAT_VERSION;
        writer.Write(&ver, sizeof(ver));
        uint32_t file_count = static_cast<uint32_t>(entries.size());
        writer.Write(&file_count, sizeof(file_count));
        uint64_t header_bytes = 0;
        for (auto& e : entries) {
            header_bytes += sizeof(uint32_t) + e.name.size() + sizeof(uint64_t) * 2;
        }
        uint64_t data_off = sizeof(magic) + sizeof(ver) + sizeof(file_count) + header_bytes;
        uint64_t cur = 0;
        for (auto& e : entries) {
            uint32_t name_len = static_cast<uint32_t>(e.name.size());
            writer.Write(&name_len, sizeof(name_len));
            writer.Write(e.name.data(), name_len);
            uint64_t off = data_off + cur;
            writer.Write(&off, sizeof(off));
            writer.Write(&e.size, sizeof(e.size));
            cur += e.size;
        }
        auto local_cm = milvus::storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
        const size_t buf_size = 1 << 20;
        std::vector<uint8_t> buf(buf_size);
        for (auto& e : entries) {
            auto file_path = (boost::filesystem::path(path_) / e.name).string();
            uint64_t remaining = e.size; uint64_t o = 0;
            while (remaining > 0) {
                auto to_read = static_cast<uint64_t>(std::min<uint64_t>(buf_size, remaining));
                local_cm->Read(file_path, o, buf.data(), to_read);
                writer.Write(buf.data(), to_read);
                remaining -= to_read; o += to_read;
            }
        }
        writer.Finish();
    }
    // upload
    uint64_t bundle_size = 0;
    {
        auto local_cm = milvus::storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
        bundle_size = local_cm->Size(bundle_local_path);
        auto remote_os = disk_file_manager_->OpenOutputStream(bundle_local_path);
        const size_t buf_size = 1 << 20; std::vector<uint8_t> buf(buf_size);
        uint64_t remaining = bundle_size; uint64_t o = 0;
        while (remaining > 0) {
            auto to_read = static_cast<uint64_t>(std::min<uint64_t>(buf_size, remaining));
            local_cm->Read(bundle_local_path, o, buf.data(), to_read);
            remote_os->Write(buf.data(), to_read);
            remaining -= to_read; o += to_read;
        }
    }
    disk_file_manager_->AddFileMeta(FileMeta{bundle_local_path, static_cast<int64_t>(bundle_size)});

    auto remote_paths_to_size = disk_file_manager_->GetRemotePathsToFileSize();

    std::vector<SerializedIndexFileInfo> index_files;
    index_files.reserve(remote_paths_to_size.size());
    for (auto& file : remote_paths_to_size) {
        index_files.emplace_back(file.first, file.second);
    }
    return IndexStats::New(disk_file_manager_->GetAddedTotalFileSize(),
                           std::move(index_files));
}

void
BsonInvertedIndex::TermQuery(
    const std::string& path,
    const std::function<void(const uint32_t* row_id_array,
                             const uint32_t* offset_array,
                             const int64_t array_len)>& visitor) {
    AssertInfo(wrapper_ != nullptr,
               "bson inverted index wrapper is not initialized");
    auto start = std::chrono::steady_clock::now();
    auto array = wrapper_->term_query_i64(path);
    auto end = std::chrono::steady_clock::now();
    LOG_TRACE("term query time:{}",
              std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                  .count());
    auto array_len = array.array_.len;
    LOG_DEBUG("json stats shared column filter size:{} with path:{}",
              array_len,
              path);

    std::vector<uint32_t> row_id_array(array_len);
    std::vector<uint32_t> offset_array(array_len);

    for (int64_t i = 0; i < array_len; i++) {
        auto value = array.array_.array[i];
        auto [row_id, offset] = DecodeInvertedIndexValue(value);
        row_id_array[i] = row_id;
        offset_array[i] = offset;
    }

    visitor(row_id_array.data(), offset_array.data(), array_len);
}

void
BsonInvertedIndex::TermQueryEach(
    const std::string& path,
    const std::function<void(uint32_t row_id, uint32_t offset)>& each) {
    AssertInfo(wrapper_ != nullptr,
               "bson inverted index wrapper is not initialized");
    auto start = std::chrono::steady_clock::now();
    auto array = wrapper_->term_query_i64(path);
    auto end = std::chrono::steady_clock::now();
    LOG_TRACE("term query time:{}",
              std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                  .count());
    auto array_len = array.array_.len;
    LOG_TRACE("json stats shared column filter size:{} with path:{}",
              array_len,
              path);

    for (int64_t i = 0; i < array_len; i++) {
        auto value = array.array_.array[i];
        auto [row_id, offset] = DecodeInvertedIndexValue(value);
        each(row_id, offset);
    }
}

}  // namespace milvus::index