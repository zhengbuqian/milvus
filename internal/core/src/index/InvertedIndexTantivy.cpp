// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "tantivy-binding.h"
#include "common/Slice.h"
#include "common/RegexQuery.h"
#include "common/Tracer.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "index/InvertedIndexTantivy.h"
#include "index/InvertedIndexUtil.h"
#include "log/Log.h"
#include "index/Utils.h"
#include "storage/Util.h"
#include "storage/FileWriter.h"
#include <cstring>

#include <algorithm>
#include <boost/filesystem.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <cstddef>
#include <shared_mutex>
#include <type_traits>
#include <vector>
#include "InvertedIndexTantivy.h"

namespace milvus::index {
inline TantivyDataType
get_tantivy_data_type(const proto::schema::FieldSchema& schema) {
    switch (schema.data_type()) {
        case proto::schema::Array:
            return get_tantivy_data_type(schema.element_type());
        default:
            return get_tantivy_data_type(schema.data_type());
    }
}

template <typename T>
void
InvertedIndexTantivy<T>::InitForBuildIndex() {
    auto field =
        std::to_string(disk_file_manager_->GetFieldDataMeta().field_id);
    path_ = disk_file_manager_->GetLocalTempIndexObjectPrefix();
    boost::filesystem::create_directories(path_);
    d_type_ = get_tantivy_data_type(schema_);
    if (tantivy_index_exist(path_.c_str())) {
        ThrowInfo(IndexBuildError,
                  "build inverted index temp dir:{} not empty",
                  path_);
    }
    wrapper_ =
        std::make_shared<TantivyIndexWrapper>(field.c_str(),
                                              d_type_,
                                              path_.c_str(),
                                              tantivy_index_version_,
                                              inverted_index_single_segment_,
                                              user_specified_doc_id_);
}

template <typename T>
InvertedIndexTantivy<T>::InvertedIndexTantivy(
    uint32_t tantivy_index_version,
    const storage::FileManagerContext& ctx,
    bool inverted_index_single_segment,
    bool user_specified_doc_id)
    : ScalarIndex<T>(INVERTED_INDEX_TYPE),
      schema_(ctx.fieldDataMeta.field_schema),
      tantivy_index_version_(tantivy_index_version),
      inverted_index_single_segment_(inverted_index_single_segment),
      user_specified_doc_id_(user_specified_doc_id) {
    mem_file_manager_ = std::make_shared<MemFileManager>(ctx);
    disk_file_manager_ = std::make_shared<DiskFileManager>(ctx);
    // push init wrapper to load process
    if (ctx.for_loading_index) {
        return;
    }
    InitForBuildIndex();
}

template <typename T>
InvertedIndexTantivy<T>::~InvertedIndexTantivy() {
    if (wrapper_) {
        wrapper_->free();
    }
    if (path_.empty()) {
        return;
    }
    auto local_chunk_manager =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    auto prefix = path_;
    LOG_INFO("inverted index remove path:{}", path_);
    local_chunk_manager->RemoveDir(prefix);
}

template <typename T>
void
InvertedIndexTantivy<T>::finish() {
    wrapper_->finish();
}

template <typename T>
BinarySet
InvertedIndexTantivy<T>::Serialize(const Config& config) {
    std::shared_lock<folly::SharedMutex> lock(mutex_);
    auto index_valid_data_length = null_offset_.size() * sizeof(size_t);
    std::shared_ptr<uint8_t[]> index_valid_data(
        new uint8_t[index_valid_data_length]);
    memcpy(
        index_valid_data.get(), null_offset_.data(), index_valid_data_length);
    lock.unlock();
    BinarySet res_set;
    if (index_valid_data_length > 0) {
        res_set.Append(INDEX_NULL_OFFSET_FILE_NAME,
                       index_valid_data,
                       index_valid_data_length);
    }
    milvus::Disassemble(res_set);
    return res_set;
}

template <typename T>
IndexStatsPtr
InvertedIndexTantivy<T>::Upload(const Config& config) {
    finish();

    // Pack all files under path_ into a single bundle file.
    // Bundle format:
    // [magic:"TANTIVYB"] [format_ver:u32] [file_count:u32]
    // repeated file_count times: [name_len:u32][name_bytes][offset:u64][size:u64]
    // followed by concatenated file contents in the same order
    const std::string bundle_local_path = (boost::filesystem::path(path_) / TANTIVY_BUNDLE_FILE_NAME).string();
    {
        // Build table of files
        struct Entry { std::string name; uint64_t size; };
        std::vector<Entry> entries;
        uint64_t total_payload = 0;
        boost::filesystem::path p(path_);
        boost::filesystem::directory_iterator end_iter;
        for (boost::filesystem::directory_iterator iter(p); iter != end_iter; ++iter) {
            if (boost::filesystem::is_directory(*iter)) {
                LOG_WARN("{} is a directory", iter->path().string());
                continue;
            }
            auto filename = iter->path().filename().string();
            if (filename == TANTIVY_BUNDLE_FILE_NAME) {
                continue; // skip previous bundle if exists
            }
            auto sz = boost::filesystem::file_size(*iter);
            entries.push_back({filename, static_cast<uint64_t>(sz)});
            total_payload += sz;
        }

        // Write bundle file
        storage::FileWriter writer(bundle_local_path, storage::io::Priority::MIDDLE);
        const char magic[8] = {'T','A','N','T','I','V','Y','B'};
        writer.Write(magic, sizeof(magic));
        uint32_t ver = TANTIVY_BUNDLE_FORMAT_VERSION;
        writer.Write(&ver, sizeof(ver));
        uint32_t file_count = static_cast<uint32_t>(entries.size());
        writer.Write(&file_count, sizeof(file_count));

        // Compute offsets
        uint64_t header_table_bytes = 0;
        for (auto& e : entries) {
            header_table_bytes += sizeof(uint32_t); // name_len
            header_table_bytes += static_cast<uint32_t>(e.name.size());
            header_table_bytes += sizeof(uint64_t); // offset
            header_table_bytes += sizeof(uint64_t); // size
        }
        // Reserve header table area by writing placeholders, then come back? Simpler: accumulate and write inline.
        // We'll compute the running data_offset as we append files; record the same in header before contents.

        uint64_t data_offset = sizeof(magic) + sizeof(ver) + sizeof(file_count) + header_table_bytes;

        // First write header with calculated offsets
        uint64_t current_offset = 0;
        for (auto& e : entries) {
            uint32_t name_len = static_cast<uint32_t>(e.name.size());
            writer.Write(&name_len, sizeof(name_len));
            writer.Write(e.name.data(), name_len);
            uint64_t off = data_offset + current_offset;
            writer.Write(&off, sizeof(off));
            writer.Write(&e.size, sizeof(e.size));
            current_offset += e.size;
        }

        // Then append file payloads in same order
        auto local_cm = storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
        for (auto& e : entries) {
            auto file_path = (boost::filesystem::path(path_) / e.name).string();
            // Stream copy using a small buffer
            const size_t buf_size = 1 << 20; // 1MB
            std::vector<uint8_t> buf(buf_size);
            uint64_t remaining = e.size;
            uint64_t offset = 0;
            while (remaining > 0) {
                auto to_read = static_cast<uint64_t>(std::min<uint64_t>(buf_size, remaining));
                local_cm->Read(file_path, offset, buf.data(), to_read);
                writer.Write(buf.data(), to_read);
                remaining -= to_read;
                offset += to_read;
            }
        }
        writer.Finish();
    }

    // Upload single bundle file via V2 API (no slicing), record its meta.
    // Read bundle file locally and stream to remote
    uint64_t bundle_size = 0;
    {
        auto local_cm = storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
        bundle_size = local_cm->Size(bundle_local_path);
        auto remote_os = disk_file_manager_->OpenOutputStream(bundle_local_path);
        const size_t buf_size = 1 << 20;
        std::vector<uint8_t> buf(buf_size);
        uint64_t remaining = bundle_size;
        uint64_t offset = 0;
        while (remaining > 0) {
            auto to_read = static_cast<uint64_t>(std::min<uint64_t>(buf_size, remaining));
            local_cm->Read(bundle_local_path, offset, buf.data(), to_read);
            remote_os->Write(buf.data(), to_read);
            remaining -= to_read;
            offset += to_read;
        }
    }
    // Record meta for remote path and size
    {
        auto local_file_name = disk_file_manager_->GetFileName(bundle_local_path);
        // DiskFileManagerImpl::AddFileMeta will record the V2 remote path mapping
        disk_file_manager_->AddFileMeta(FileMeta{bundle_local_path, static_cast<int64_t>(bundle_size)});
    }

    auto remote_paths_to_size = disk_file_manager_->GetRemotePathsToFileSize();

    auto binary_set = Serialize(config);
    mem_file_manager_->AddFile(binary_set);
    auto remote_mem_path_to_size =
        mem_file_manager_->GetRemotePathsToFileSize();

    std::vector<SerializedIndexFileInfo> index_files;
    index_files.reserve(remote_paths_to_size.size() +
                        remote_mem_path_to_size.size());
    for (auto& file : remote_paths_to_size) {
        index_files.emplace_back(file.first, file.second);
    }
    for (auto& file : remote_mem_path_to_size) {
        index_files.emplace_back(file.first, file.second);
    }
    return IndexStats::New(mem_file_manager_->GetAddedTotalMemSize() +
                               disk_file_manager_->GetAddedTotalFileSize(),
                           std::move(index_files));
}

template <typename T>
void
InvertedIndexTantivy<T>::Build(const Config& config) {
    auto field_datas =
        storage::CacheRawDataAndFillMissing(mem_file_manager_, config);
    BuildWithFieldData(field_datas);
}

template <typename T>
void
InvertedIndexTantivy<T>::Load(milvus::tracer::TraceContext ctx,
                              const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, INDEX_FILES);
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load disk ann index data");
    auto inverted_index_files = index_files.value();

    // Always load index metas (e.g., null offsets) first from memory files
    LoadIndexMetas(inverted_index_files, config);

    // Detect if bundle file present; if so, download and unpack bundle locally, then bypass CacheIndexToDisk for tantivy files.
    auto bundle_it = std::find_if(
        inverted_index_files.begin(), inverted_index_files.end(), [&](const std::string& f) {
            return boost::filesystem::path(f).filename().string() == TANTIVY_BUNDLE_FILE_NAME;
        });
    if (bundle_it != inverted_index_files.end()) {
        auto prefix = disk_file_manager_->GetLocalIndexObjectPrefix();
        boost::filesystem::create_directories(prefix);
        path_ = prefix;

        // Download bundle from remote using stream API
        auto remote_is = disk_file_manager_->OpenInputStream((boost::filesystem::path(prefix) / TANTIVY_BUNDLE_FILE_NAME).string());
        auto local_bundle_path = (boost::filesystem::path(prefix) / TANTIVY_BUNDLE_FILE_NAME).string();
        {
            storage::FileWriter fw(local_bundle_path, storage::io::Priority::HIGH);
            const size_t buf_size = 1 << 20;
            std::vector<uint8_t> buf(buf_size);
            size_t total = remote_is->Size();
            size_t copied = 0;
            while (copied < total) {
                size_t chunk = std::min(buf_size, total - copied);
                auto n = remote_is->ReadAt(buf.data(), copied, chunk);
                AssertInfo(n == chunk, "failed to read remote bundle stream");
                fw.Write(buf.data(), n);
                copied += n;
            }
            fw.Finish();
        }

        // Unpack bundle contents into prefix directory
        auto local_cm = storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
        const size_t buf_size = 1 << 20;
        auto read_exact = [&](uint64_t off, void* dst, size_t n) {
            local_cm->Read(local_bundle_path, off, dst, n);
        };
        uint64_t off = 0;
        char magic[8];
        read_exact(off, magic, sizeof(magic)); off += sizeof(magic);
        AssertInfo(std::memcmp(magic, "TANTIVYB", 8) == 0, "invalid tantivy bundle magic");
        uint32_t ver = 0; read_exact(off, &ver, sizeof(ver)); off += sizeof(ver);
        AssertInfo(ver == TANTIVY_BUNDLE_FORMAT_VERSION, "unsupported tantivy bundle version: {}", ver);
        uint32_t file_cnt = 0; read_exact(off, &file_cnt, sizeof(file_cnt)); off += sizeof(file_cnt);
        struct Header { std::string name; uint64_t offset; uint64_t size; };
        std::vector<Header> headers; headers.reserve(file_cnt);
        for (uint32_t i = 0; i < file_cnt; ++i) {
            uint32_t name_len = 0; read_exact(off, &name_len, sizeof(name_len)); off += sizeof(name_len);
            std::string name; name.resize(name_len);
            if (name_len > 0) { read_exact(off, name.data(), name_len); }
            off += name_len;
            uint64_t data_off = 0; read_exact(off, &data_off, sizeof(data_off)); off += sizeof(data_off);
            uint64_t size = 0; read_exact(off, &size, sizeof(size)); off += sizeof(size);
            headers.push_back({std::move(name), data_off, size});
        }
        for (auto& h : headers) {
            auto out_path = (boost::filesystem::path(prefix) / h.name).string();
            storage::FileWriter fw(out_path, storage::io::Priority::HIGH);
            uint64_t remaining = h.size;
            uint64_t cur = 0;
            std::vector<uint8_t> buf(buf_size);
            while (remaining > 0) {
                auto to_read = static_cast<uint64_t>(std::min<uint64_t>(buf_size, remaining));
                local_cm->Read(local_bundle_path, h.offset + cur, buf.data(), to_read);
                fw.Write(buf.data(), to_read);
                remaining -= to_read;
                cur += to_read;
            }
            fw.Finish();
        }

        auto load_in_mmap =
            GetValueFromConfig<bool>(config, ENABLE_MMAP).value_or(true);
        wrapper_ = std::make_shared<TantivyIndexWrapper>(
            prefix.c_str(), load_in_mmap, milvus::index::SetBitsetSealed);
        if (!load_in_mmap) {
            disk_file_manager_->RemoveIndexFiles();
        }
        return;
    }

    RetainTantivyIndexFiles(inverted_index_files);
    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);
    disk_file_manager_->CacheIndexToDisk(inverted_index_files, load_priority);
    auto prefix = disk_file_manager_->GetLocalIndexObjectPrefix();
    path_ = prefix;
    auto load_in_mmap =
        GetValueFromConfig<bool>(config, ENABLE_MMAP).value_or(true);
    wrapper_ = std::make_shared<TantivyIndexWrapper>(
        prefix.c_str(), load_in_mmap, milvus::index::SetBitsetSealed);

    if (!load_in_mmap) {
        // the index is loaded in ram, so we can remove files in advance
        disk_file_manager_->RemoveIndexFiles();
    }
}

template <typename T>
void
InvertedIndexTantivy<T>::LoadIndexMetas(
    const std::vector<std::string>& index_files, const Config& config) {
    auto fill_null_offsets = [&](const uint8_t* data, int64_t size) {
        null_offset_.resize((size_t)size / sizeof(size_t));
        memcpy(null_offset_.data(), data, (size_t)size);
    };
    auto null_offset_file_itr = std::find_if(
        index_files.begin(), index_files.end(), [&](const std::string& file) {
            return boost::filesystem::path(file).filename().string() ==
                   INDEX_NULL_OFFSET_FILE_NAME;
        });
    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);

    if (null_offset_file_itr != index_files.end()) {
        // null offset file is not sliced
        auto index_datas = mem_file_manager_->LoadIndexToMemory(
            {*null_offset_file_itr}, load_priority);
        auto null_offset_data =
            std::move(index_datas.at(INDEX_NULL_OFFSET_FILE_NAME));
        fill_null_offsets(null_offset_data->PayloadData(),
                          null_offset_data->PayloadSize());
        return;
    }
    std::vector<std::string> null_offset_files;
    for (auto& file : index_files) {
        auto file_name = boost::filesystem::path(file).filename().string();
        if (file_name.find(INDEX_NULL_OFFSET_FILE_NAME) != std::string::npos) {
            null_offset_files.push_back(file);
        }
    }
    if (null_offset_files.size() > 0) {
        // null offset file is sliced
        auto index_datas = mem_file_manager_->LoadIndexToMemory(
            null_offset_files, load_priority);

        auto null_offsets_data = CompactIndexDatas(index_datas);
        auto null_offsets_data_codecs =
            std::move(null_offsets_data.at(INDEX_NULL_OFFSET_FILE_NAME));
        for (auto&& null_offsets_codec : null_offsets_data_codecs.codecs_) {
            fill_null_offsets(null_offsets_codec->PayloadData(),
                              null_offsets_codec->PayloadSize());
        }
    }
}

template <typename T>
void
InvertedIndexTantivy<T>::RetainTantivyIndexFiles(
    std::vector<std::string>& index_files) {
    index_files.erase(
        std::remove_if(
            index_files.begin(),
            index_files.end(),
            [&](const std::string& file) {
                auto file_name =
                    boost::filesystem::path(file).filename().string();
                return file_name == "index_type" ||
                       file_name.find(INDEX_NULL_OFFSET_FILE_NAME) !=
                           std::string::npos;
            }),
        index_files.end());
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::In(size_t n, const T* values) {
    tracer::AutoSpan span("InvertedIndexTantivy::In", tracer::GetRootSpan());
    TargetBitmap bitset(Count());
    wrapper_->terms_query(values, n, &bitset);
    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::IsNull() {
    tracer::AutoSpan span("InvertedIndexTantivy::IsNull",
                          tracer::GetRootSpan());
    int64_t count = Count();
    TargetBitmap bitset(count);

    auto fill_bitset = [this, count, &bitset]() {
        auto end =
            std::lower_bound(null_offset_.begin(), null_offset_.end(), count);
        for (auto iter = null_offset_.begin(); iter != end; ++iter) {
            bitset.set(*iter);
        }
    };

    if (is_growing_) {
        std::shared_lock<folly::SharedMutex> lock(mutex_);
        fill_bitset();
    } else {
        fill_bitset();
    }

    return bitset;
}

template <typename T>
TargetBitmap
InvertedIndexTantivy<T>::IsNotNull() {
    tracer::AutoSpan span("InvertedIndexTantivy::IsNotNull",
                          tracer::GetRootSpan());
    int64_t count = Count();
    TargetBitmap bitset(count, true);

    auto fill_bitset = [this, count, &bitset]() {
        auto end =
            std::lower_bound(null_offset_.begin(), null_offset_.end(), count);
        for (auto iter = null_offset_.begin(); iter != end; ++iter) {
            bitset.reset(*iter);
        }
    };

    if (is_growing_) {
        std::shared_lock<folly::SharedMutex> lock(mutex_);
        fill_bitset();
    } else {
        fill_bitset();
    }

    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::InApplyFilter(
    size_t n, const T* values, const std::function<bool(size_t)>& filter) {
    tracer::AutoSpan span("InvertedIndexTantivy::InApplyFilter",
                          tracer::GetRootSpan());
    TargetBitmap bitset(Count());
    wrapper_->terms_query(values, n, &bitset);
    // todo(SpadeA): could push-down the filter to tantivy query
    apply_hits_with_filter(bitset, filter);
    return bitset;
}

template <typename T>
void
InvertedIndexTantivy<T>::InApplyCallback(
    size_t n, const T* values, const std::function<void(size_t)>& callback) {
    tracer::AutoSpan span("InvertedIndexTantivy::InApplyCallback",
                          tracer::GetRootSpan());
    TargetBitmap bitset(Count());
    wrapper_->terms_query(values, n, &bitset);
    // todo(SpadeA): could push-down the callback to tantivy query
    apply_hits_with_callback(bitset, callback);
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::NotIn(size_t n, const T* values) {
    tracer::AutoSpan span("InvertedIndexTantivy::NotIn", tracer::GetRootSpan());
    int64_t count = Count();
    TargetBitmap bitset(count);
    wrapper_->terms_query(values, n, &bitset);
    // The expression is "not" in, so we flip the bit.
    bitset.flip();

    auto fill_bitset = [this, count, &bitset]() {
        auto end =
            std::lower_bound(null_offset_.begin(), null_offset_.end(), count);
        for (auto iter = null_offset_.begin(); iter != end; ++iter) {
            bitset.reset(*iter);
        }
    };

    if (is_growing_) {
        std::shared_lock<folly::SharedMutex> lock(mutex_);
        fill_bitset();
    } else {
        fill_bitset();
    }

    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::Range(T value, OpType op) {
    tracer::AutoSpan span("InvertedIndexTantivy::Range", tracer::GetRootSpan());
    TargetBitmap bitset(Count());

    switch (op) {
        case OpType::LessThan: {
            wrapper_->upper_bound_range_query(value, false, &bitset);
        } break;
        case OpType::LessEqual: {
            wrapper_->upper_bound_range_query(value, true, &bitset);
        } break;
        case OpType::GreaterThan: {
            wrapper_->lower_bound_range_query(value, false, &bitset);
        } break;
        case OpType::GreaterEqual: {
            wrapper_->lower_bound_range_query(value, true, &bitset);
        } break;
        default:
            ThrowInfo(OpTypeInvalid,
                      fmt::format("Invalid OperatorType: {}", op));
    }

    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::Range(T lower_bound_value,
                               bool lb_inclusive,
                               T upper_bound_value,
                               bool ub_inclusive) {
    tracer::AutoSpan span("InvertedIndexTantivy::RangeWithBounds",
                          tracer::GetRootSpan());
    TargetBitmap bitset(Count());
    wrapper_->range_query(lower_bound_value,
                          upper_bound_value,
                          lb_inclusive,
                          ub_inclusive,
                          &bitset);
    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::PrefixMatch(const std::string_view prefix) {
    tracer::AutoSpan span("InvertedIndexTantivy::PrefixMatch",
                          tracer::GetRootSpan());
    TargetBitmap bitset(Count());
    std::string s(prefix);
    wrapper_->prefix_query(s, &bitset);
    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::Query(const DatasetPtr& dataset) {
    return ScalarIndex<T>::Query(dataset);
}

template <>
const TargetBitmap
InvertedIndexTantivy<std::string>::Query(const DatasetPtr& dataset) {
    tracer::AutoSpan span("InvertedIndexTantivy::Query", tracer::GetRootSpan());
    auto op = dataset->Get<OpType>(OPERATOR_TYPE);
    if (op == OpType::PrefixMatch) {
        auto prefix = dataset->Get<std::string>(MATCH_VALUE);
        return PrefixMatch(prefix);
    }
    return ScalarIndex<std::string>::Query(dataset);
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::RegexQuery(const std::string& regex_pattern) {
    tracer::AutoSpan span("InvertedIndexTantivy::RegexQuery",
                          tracer::GetRootSpan());
    TargetBitmap bitset(Count());
    wrapper_->regex_query(regex_pattern, &bitset);
    return bitset;
}

template <typename T>
void
InvertedIndexTantivy<T>::BuildWithRawDataForUT(size_t n,
                                               const void* values,
                                               const Config& config) {
    if constexpr (std::is_same_v<bool, T>) {
        schema_.set_data_type(proto::schema::DataType::Bool);
    }
    if constexpr (std::is_same_v<int8_t, T>) {
        schema_.set_data_type(proto::schema::DataType::Int8);
    }
    if constexpr (std::is_same_v<int16_t, T>) {
        schema_.set_data_type(proto::schema::DataType::Int16);
    }
    if constexpr (std::is_same_v<int32_t, T>) {
        schema_.set_data_type(proto::schema::DataType::Int32);
    }
    if constexpr (std::is_same_v<int64_t, T>) {
        schema_.set_data_type(proto::schema::DataType::Int64);
    }
    if constexpr (std::is_same_v<float, T>) {
        schema_.set_data_type(proto::schema::DataType::Float);
    }
    if constexpr (std::is_same_v<double, T>) {
        schema_.set_data_type(proto::schema::DataType::Double);
    }
    if constexpr (std::is_same_v<std::string, T>) {
        schema_.set_data_type(proto::schema::DataType::VarChar);
    }
    if (!wrapper_) {
        boost::uuids::random_generator generator;
        auto uuid = generator();
        auto prefix = boost::uuids::to_string(uuid);
        path_ = fmt::format("/tmp/{}", prefix);
        boost::filesystem::create_directories(path_);
        d_type_ = get_tantivy_data_type(schema_);
        std::string field = "test_inverted_index";
        inverted_index_single_segment_ =
            GetValueFromConfig<int32_t>(
                config, milvus::index::SCALAR_INDEX_ENGINE_VERSION)
                .value_or(1) == 0;
        tantivy_index_version_ =
            GetValueFromConfig<int32_t>(config,
                                        milvus::index::TANTIVY_INDEX_VERSION)
                .value_or(milvus::index::TANTIVY_INDEX_LATEST_VERSION);
        wrapper_ = std::make_shared<TantivyIndexWrapper>(
            field.c_str(),
            d_type_,
            path_.c_str(),
            tantivy_index_version_,
            inverted_index_single_segment_);
    }
    if (!inverted_index_single_segment_) {
        if (config.find("is_array") != config.end()) {
            // only used in ut.
            auto arr = static_cast<const boost::container::vector<T>*>(values);
            for (size_t i = 0; i < n; i++) {
                wrapper_->template add_array_data(
                    arr[i].data(), arr[i].size(), i);
            }
        } else {
            wrapper_->add_data<T>(static_cast<const T*>(values), n, 0);
        }
    } else {
        if (config.find("is_array") != config.end()) {
            // only used in ut.
            auto arr = static_cast<const boost::container::vector<T>*>(values);
            for (size_t i = 0; i < n; i++) {
                wrapper_->template add_array_data_by_single_segment_writer(
                    arr[i].data(), arr[i].size());
            }
        } else {
            wrapper_->add_data_by_single_segment_writer<T>(
                static_cast<const T*>(values), n);
        }
    }
    wrapper_->create_reader(milvus::index::SetBitsetSealed);
    finish();
    wrapper_->reload();
}

template <typename T>
void
InvertedIndexTantivy<T>::BuildWithFieldData(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    if (schema_.nullable()) {
        int64_t total = 0;
        for (const auto& data : field_datas) {
            total += data->get_null_count();
        }
        null_offset_.reserve(total);
    }
    switch (schema_.data_type()) {
        case proto::schema::DataType::Bool:
        case proto::schema::DataType::Int8:
        case proto::schema::DataType::Int16:
        case proto::schema::DataType::Int32:
        case proto::schema::DataType::Int64:
        case proto::schema::DataType::Float:
        case proto::schema::DataType::Double:
        case proto::schema::DataType::String:
        case proto::schema::DataType::VarChar: {
            // Generally, we will not build inverted index with single segment except for building index
            // for query node with older version(2.4). See more comments above `inverted_index_single_segment_`.
            if (!inverted_index_single_segment_) {
                int64_t offset = 0;
                if (schema_.nullable()) {
                    for (const auto& data : field_datas) {
                        auto n = data->get_num_rows();
                        for (int i = 0; i < n; i++) {
                            if (!data->is_valid(i)) {
                                null_offset_.push_back(offset);
                            }
                            wrapper_->add_array_data<T>(
                                static_cast<const T*>(data->RawValue(i)),
                                data->is_valid(i),
                                offset++);
                        }
                    }
                } else {
                    for (const auto& data : field_datas) {
                        auto n = data->get_num_rows();
                        wrapper_->add_data<T>(
                            static_cast<const T*>(data->Data()), n, offset);
                        offset += n;
                    }
                }
            } else {
                for (const auto& data : field_datas) {
                    auto n = data->get_num_rows();
                    if (schema_.nullable()) {
                        for (int i = 0; i < n; i++) {
                            if (!data->is_valid(i)) {
                                null_offset_.push_back(i);
                            }
                            wrapper_
                                ->add_array_data_by_single_segment_writer<T>(
                                    static_cast<const T*>(data->RawValue(i)),
                                    data->is_valid(i));
                        }
                        continue;
                    }
                    wrapper_->add_data_by_single_segment_writer<T>(
                        static_cast<const T*>(data->Data()), n);
                }
            }
            break;
        }

        case proto::schema::DataType::Array: {
            build_index_for_array(field_datas);
            break;
        }

        case proto::schema::DataType::JSON: {
            build_index_for_json(field_datas);
            break;
        }

        default:
            ThrowInfo(ErrorCode::NotImplemented,
                      fmt::format("Inverted index not supported on {}",
                                  schema_.data_type()));
    }
}

template <typename T>
void
InvertedIndexTantivy<T>::build_index_for_array(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    using ElementType = std::conditional_t<std::is_same<T, int8_t>::value ||
                                               std::is_same<T, int16_t>::value,
                                           int32_t,
                                           T>;
    int64_t offset = 0;
    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        auto array_column = static_cast<const Array*>(data->Data());
        for (int64_t i = 0; i < n; i++) {
            if (schema_.nullable() && !data->is_valid(i)) {
                null_offset_.push_back(offset);
            }
            auto length = data->is_valid(i) ? array_column[i].length() : 0;
            if (!inverted_index_single_segment_) {
                wrapper_->template add_array_data(
                    reinterpret_cast<const ElementType*>(
                        array_column[i].data()),
                    length,
                    offset++);
            } else {
                wrapper_->template add_array_data_by_single_segment_writer(
                    reinterpret_cast<const ElementType*>(
                        array_column[i].data()),
                    length);
                offset++;
            }
        }
    }
}

template <>
void
InvertedIndexTantivy<std::string>::build_index_for_array(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    int64_t offset = 0;
    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        auto array_column = static_cast<const Array*>(data->Data());
        for (int64_t i = 0; i < n; i++) {
            if (schema_.nullable() && !data->is_valid(i)) {
                null_offset_.push_back(offset);
            } else {
                Assert(IsStringDataType(array_column[i].get_element_type()));
                Assert(IsStringDataType(
                    static_cast<DataType>(schema_.element_type())));
            }
            std::vector<std::string> output;
            for (int64_t j = 0; j < array_column[i].length(); j++) {
                output.push_back(
                    array_column[i].template get_data<std::string>(j));
            }
            auto length = data->is_valid(i) ? output.size() : 0;
            if (!inverted_index_single_segment_) {
                wrapper_->template add_array_data(
                    output.data(), length, offset++);
            } else {
                wrapper_->template add_array_data_by_single_segment_writer(
                    output.data(), length);
            }
        }
    }
}

template class InvertedIndexTantivy<bool>;
template class InvertedIndexTantivy<int8_t>;
template class InvertedIndexTantivy<int16_t>;
template class InvertedIndexTantivy<int32_t>;
template class InvertedIndexTantivy<int64_t>;
template class InvertedIndexTantivy<float>;
template class InvertedIndexTantivy<double>;
template class InvertedIndexTantivy<std::string>;
}  // namespace milvus::index
