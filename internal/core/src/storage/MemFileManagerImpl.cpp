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

#include "storage/MemFileManagerImpl.h"

#include <atomic>
#include <exception>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Pack.h"
#include "common/Types.h"
#include "glog/logging.h"
#include "log/Log.h"
#include "storage/Util.h"
#include "storage/FileManager.h"
#include "storage/PluginLoader.h"
#include "storage/loon_ffi/ffi_reader_c.h"
#include "storage/RemoteInputStream.h"
#include "storage/RemoteOutputStream.h"
#include "index/Utils.h"
#include "knowhere/binaryset.h"
#include "log/Log.h"
#include "milvus-storage/filesystem/fs.h"
#include "nlohmann/json.hpp"
#include "pb/schema.pb.h"
#include "storage/ChunkManager.h"
#include "storage/DataCodec.h"
#include "storage/FileManager.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"

namespace milvus::storage {

MemFileManagerImpl::MemFileManagerImpl(
    const FileManagerContext& fileManagerContext)
    : FileManagerImpl(fileManagerContext.fieldDataMeta,
                      fileManagerContext.indexMeta) {
    rcm_ = fileManagerContext.chunkManagerPtr;
    fs_ = fileManagerContext.fs;
    loon_ffi_properties_ = fileManagerContext.loon_ffi_properties;
    plugin_context_ = fileManagerContext.plugin_context;
}

bool
MemFileManagerImpl::AddFile(const std::string& filename /* unused */) noexcept {
    return false;
}

bool
MemFileManagerImpl::AddBinarySet(const BinarySet& binary_set,
                                 const std::string& prefix) {
    std::vector<const uint8_t*> data_slices;
    std::vector<int64_t> slice_sizes;
    std::vector<std::string> slice_names;

    auto AddBatchIndexFiles = [&]() {
        auto res = PutIndexData(rcm_.get(),
                                data_slices,
                                slice_sizes,
                                slice_names,
                                field_meta_,
                                index_meta_,
                                plugin_context_);
        for (auto& [file, size] : res) {
            remote_paths_to_size_[file] = size;
        }
    };

    int64_t batch_size = 0;
    for (const auto& iter : binary_set.binary_map_) {
        if (batch_size >= DEFAULT_FIELD_MAX_MEMORY_LIMIT) {
            AddBatchIndexFiles();
            data_slices.clear();
            slice_sizes.clear();
            slice_names.clear();
            batch_size = 0;
        }

        data_slices.emplace_back(iter.second->data.get());
        slice_sizes.emplace_back(iter.second->size);
        slice_names.emplace_back(prefix + "/" + iter.first);
        batch_size += iter.second->size;
        added_total_mem_size_ += iter.second->size;
    }

    if (data_slices.size() > 0) {
        AddBatchIndexFiles();
    }

    return true;
}

std::shared_ptr<InputStream>
MemFileManagerImpl::OpenInputStream(const std::string& filename) {
    auto remote_file_path = GetRemoteIndexObjectPrefix() + "/" + filename;

    auto fs = fs_;
    AssertInfo(fs, "fs is nullptr");

    auto remote_file = fs->OpenInputFile(remote_file_path);
    AssertInfo(remote_file.ok(),
               "failed to open remote file, reason: {}",
               remote_file.status().ToString());

    return std::make_shared<milvus::storage::RemoteInputStream>(
        std::move(remote_file.ValueOrDie()));
}

std::shared_ptr<InputStream>
MemFileManagerImpl::OpenInputStreamByPath(const std::string& remote_path) {
    auto fs = fs_;
    AssertInfo(fs, "fs is nullptr");

    auto remote_file = fs->OpenInputFile(remote_path);
    AssertInfo(remote_file.ok(),
               "failed to open remote file: {}, reason: {}",
               remote_path,
               remote_file.status().ToString());

    return std::make_shared<milvus::storage::RemoteInputStream>(
        std::move(remote_file.ValueOrDie()));
}

std::shared_ptr<OutputStream>
MemFileManagerImpl::OpenOutputStream(const std::string& filename) {
    auto remote_file_path = GetRemoteIndexObjectPrefix() + "/" + filename;

    auto fs = fs_;
    AssertInfo(fs, "fs is nullptr");

    auto remote_stream = fs->OpenOutputStream(remote_file_path);
    AssertInfo(remote_stream.ok(),
               "failed to open remote stream, reason: {}",
               remote_stream.status().ToString());

    return std::make_shared<milvus::storage::RemoteOutputStream>(
        std::move(remote_stream.ValueOrDie()));
}

std::shared_ptr<OutputStream>
MemFileManagerImpl::OpenTextLogOutputStream(const std::string& filename) {
    auto remote_file_path = GetRemoteTextLogPrefix() + "/" + filename;

    auto fs = fs_;
    AssertInfo(fs, "fs is nullptr");

    auto remote_stream = fs->OpenOutputStream(remote_file_path);
    AssertInfo(remote_stream.ok(),
               "failed to open remote stream, reason: {}",
               remote_stream.status().ToString());

    return std::make_shared<milvus::storage::RemoteOutputStream>(
        std::move(remote_stream.ValueOrDie()));
}

bool
MemFileManagerImpl::AddFileMeta(const FileMeta& file_meta) {
    return true;
}

bool
MemFileManagerImpl::AddFile(const BinarySet& binary_set) {
    return AddBinarySet(binary_set, GetRemoteIndexObjectPrefix());
}

bool
MemFileManagerImpl::AddTextLog(const BinarySet& binary_set) {
    return AddBinarySet(binary_set, GetRemoteTextLogPrefix());
}

bool
MemFileManagerImpl::LoadFile(const std::string& filename) noexcept {
    return true;
}

std::map<std::string, std::unique_ptr<DataCodec>>
MemFileManagerImpl::LoadIndexToMemory(
    const std::vector<std::string>& remote_files,
    milvus::proto::common::LoadPriority priority) {
    std::map<std::string, std::unique_ptr<DataCodec>> file_to_index_data;
    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    std::vector<std::string> batch_files;

    auto LoadBatchIndexFiles = [&]() {
        auto index_datas = GetObjectData(
            rcm_.get(), batch_files, milvus::PriorityForLoad(priority));
        // Wait for all futures to ensure all threads complete
        auto codecs = storage::WaitAllFutures(std::move(index_datas));
        for (size_t idx = 0; idx < batch_files.size(); ++idx) {
            auto file_name =
                batch_files[idx].substr(batch_files[idx].find_last_of('/') + 1);
            file_to_index_data[file_name] = std::move(codecs[idx]);
        }
    };

    for (auto& file : remote_files) {
        if (batch_files.size() >= parallel_degree) {
            LoadBatchIndexFiles();
            batch_files.clear();
        }
        batch_files.emplace_back(file);
    }

    if (batch_files.size() > 0) {
        LoadBatchIndexFiles();
    }

    AssertInfo(file_to_index_data.size() == remote_files.size(),
               "inconsistent file num and index data num!");
    return file_to_index_data;
}

std::vector<FieldDataPtr>
MemFileManagerImpl::CacheRawDataToMemory(const Config& config) {
    auto storage_version =
        index::GetValueFromConfig<int64_t>(config, STORAGE_VERSION_KEY)
            .value_or(0);
    if (storage_version == STORAGE_V2 || storage_version == STORAGE_V3) {
        return cache_raw_data_to_memory_storage_v2(config);
    }
    return cache_raw_data_to_memory_internal(config);
}

std::vector<FieldDataPtr>
MemFileManagerImpl::cache_raw_data_to_memory_internal(const Config& config) {
    auto insert_files = index::GetValueFromConfig<std::vector<std::string>>(
        config, INSERT_FILES_KEY);
    AssertInfo(insert_files.has_value(),
               "insert file paths is empty when build index");
    auto remote_files = insert_files.value();
    SortByPath(remote_files);

    auto parallel_degree =
        uint64_t(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    std::vector<std::string> batch_files;
    std::vector<FieldDataPtr> field_datas;

    auto FetchRawData = [&]() {
        auto raw_datas = GetObjectData(rcm_.get(), batch_files);
        // Wait for all futures to ensure all threads complete
        auto codecs = storage::WaitAllFutures(std::move(raw_datas));
        for (auto& codec : codecs) {
            field_datas.emplace_back(codec->GetFieldData());
        }
    };

    for (auto& file : remote_files) {
        if (batch_files.size() >= parallel_degree) {
            FetchRawData();
            batch_files.clear();
        }
        batch_files.emplace_back(file);
    }
    if (batch_files.size() > 0) {
        FetchRawData();
    }

    AssertInfo(field_datas.size() == remote_files.size(),
               "inconsistent file num and raw data num!");
    return field_datas;
}

std::vector<FieldDataPtr>
MemFileManagerImpl::cache_raw_data_to_memory_storage_v2(const Config& config) {
    auto data_type = index::GetValueFromConfig<DataType>(config, DATA_TYPE_KEY);
    AssertInfo(data_type.has_value(),
               "[StorageV2] data type is empty when build index");
    auto element_type =
        index::GetValueFromConfig<DataType>(config, ELEMENT_TYPE_KEY);
    AssertInfo(element_type.has_value(),
               "[StorageV2] element type is empty when build index");
    auto dim = index::GetValueFromConfig<int64_t>(config, DIM_KEY).value_or(0);
    auto segment_insert_files =
        index::GetValueFromConfig<std::vector<std::vector<std::string>>>(
            config, SEGMENT_INSERT_FILES_KEY);
    auto manifest =
        index::GetValueFromConfig<std::string>(config, SEGMENT_MANIFEST_KEY);
    AssertInfo(segment_insert_files.has_value() || manifest.has_value(),
               "[StorageV2] insert file paths and manifest for storage v2 is "
               "empty when build index");
    // use manifest file for storage v2
    auto manifest_path_str = manifest.value_or("");
    if (manifest_path_str != "") {
        AssertInfo(loon_ffi_properties_ != nullptr,
                   "[StorageV2] loon ffi properties is null when build index "
                   "with manifest");
        return GetFieldDatasFromManifest(manifest_path_str,
                                         loon_ffi_properties_,
                                         field_meta_,
                                         data_type,
                                         dim,
                                         element_type);
    }

    auto remote_files = segment_insert_files.value();
    for (auto& files : remote_files) {
        SortByPath(files);
    }
    auto field_datas = GetFieldDatasFromStorageV2(remote_files,
                                                  field_meta_.field_id,
                                                  data_type.value(),
                                                  element_type.value(),
                                                  dim,
                                                  fs_);
    // field data list could differ for storage v2 group list
    return field_datas;
}

template <DataType T>
std::vector<std::vector<uint32_t>>
GetOptFieldIvfDataImpl(const std::vector<FieldDataPtr>& field_datas) {
    using FieldDataT = DataTypeNativeOrVoid<T>;
    std::unordered_map<FieldDataT, std::vector<uint32_t>> mp;
    uint32_t offset = 0;
    for (const auto& field_data : field_datas) {
        for (int64_t i = 0; i < field_data->get_num_rows(); ++i) {
            auto val =
                *reinterpret_cast<const FieldDataT*>(field_data->RawValue(i));
            mp[val].push_back(offset++);
        }
    }

    // opt field data is not used if there is only one value
    if (mp.size() <= 1) {
        return {};
    }
    std::vector<std::vector<uint32_t>> scalar_info;
    scalar_info.reserve(mp.size());
    for (auto& [field_id, tup] : mp) {
        scalar_info.emplace_back(std::move(tup));
    }
    LOG_INFO("Get opt fields with {} categories", scalar_info.size());
    return scalar_info;
}

std::vector<std::vector<uint32_t>>
GetOptFieldIvfData(const DataType& dt,
                   const std::vector<FieldDataPtr>& field_datas) {
    switch (dt) {
        case DataType::BOOL:
            return GetOptFieldIvfDataImpl<DataType::BOOL>(field_datas);
        case DataType::INT8:
            return GetOptFieldIvfDataImpl<DataType::INT8>(field_datas);
        case DataType::INT16:
            return GetOptFieldIvfDataImpl<DataType::INT16>(field_datas);
        case DataType::INT32:
            return GetOptFieldIvfDataImpl<DataType::INT32>(field_datas);
        case DataType::TIMESTAMPTZ:
            return GetOptFieldIvfDataImpl<DataType::TIMESTAMPTZ>(field_datas);
        case DataType::INT64:
            return GetOptFieldIvfDataImpl<DataType::INT64>(field_datas);
        case DataType::FLOAT:
            return GetOptFieldIvfDataImpl<DataType::FLOAT>(field_datas);
        case DataType::DOUBLE:
            return GetOptFieldIvfDataImpl<DataType::DOUBLE>(field_datas);
        case DataType::STRING:
            return GetOptFieldIvfDataImpl<DataType::STRING>(field_datas);
        case DataType::VARCHAR:
            return GetOptFieldIvfDataImpl<DataType::VARCHAR>(field_datas);
        default:
            LOG_WARN("Unsupported data type in optional scalar field: ", dt);
            return {};
    }
    return {};
}

std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>>
MemFileManagerImpl::CacheOptFieldToMemory(const Config& config) {
    auto storage_version =
        index::GetValueFromConfig<int64_t>(config, STORAGE_VERSION_KEY)
            .value_or(0);
    if (storage_version == STORAGE_V2) {
        return cache_opt_field_memory_v2(config);
    }
    return cache_opt_field_memory(config);
}

std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>>
MemFileManagerImpl::cache_opt_field_memory(const Config& config) {
    std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>> res;
    auto opt_fields =
        index::GetValueFromConfig<OptFieldT>(config, VEC_OPT_FIELDS);
    if (!opt_fields.has_value()) {
        return res;
    }
    auto fields_map = opt_fields.value();
    auto num_of_fields = fields_map.size();
    if (0 == num_of_fields) {
        return {};
    } else if (num_of_fields > 1) {
        ThrowInfo(
            ErrorCode::NotImplemented,
            "vector index build with multiple fields is not supported yet");
    }

    for (auto& [field_id, tup] : fields_map) {
        const auto& field_type = std::get<1>(tup);
        auto& field_paths = std::get<3>(tup);
        if (0 == field_paths.size()) {
            LOG_WARN("optional field {} has no data", field_id);
            return {};
        }

        SortByPath(field_paths);
        std::vector<FieldDataPtr> field_datas =
            FetchFieldData(rcm_.get(), field_paths);
        res[field_id] = GetOptFieldIvfData(field_type, field_datas);
    }
    return res;
}

std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>>
MemFileManagerImpl::cache_opt_field_memory_v2(const Config& config) {
    auto opt_fields =
        index::GetValueFromConfig<OptFieldT>(config, VEC_OPT_FIELDS);
    if (!opt_fields.has_value()) {
        return {};
    }
    auto fields_map = opt_fields.value();
    auto num_of_fields = fields_map.size();
    if (0 == num_of_fields) {
        return {};
    } else if (num_of_fields > 1) {
        ThrowInfo(
            ErrorCode::NotImplemented,
            "vector index build with multiple fields is not supported yet");
    }

    auto manifest =
        index::GetValueFromConfig<std::string>(config, SEGMENT_MANIFEST_KEY);
    // use manifest file for storage v2
    auto manifest_path_str = manifest.value_or("");
    if (manifest_path_str != "") {
        AssertInfo(loon_ffi_properties_ != nullptr,
                   "[StorageV2] loon ffi properties is null when build index "
                   "with manifest");
        std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>> res;
        for (auto& [field_id, tup] : fields_map) {
            const auto& field_type = std::get<1>(tup);
            const auto& element_type = std::get<2>(tup);

            // compose field schema for optional field
            proto::schema::FieldSchema field_schema;
            field_schema.set_fieldid(field_id);
            field_schema.set_nullable(true);  // use always nullable
            milvus::storage::FieldDataMeta field_meta{field_meta_.collection_id,
                                                      field_meta_.partition_id,
                                                      field_meta_.segment_id,
                                                      field_id,
                                                      field_schema};
            auto field_datas = GetFieldDatasFromManifest(manifest_path_str,
                                                         loon_ffi_properties_,
                                                         field_meta_,
                                                         field_type,
                                                         1,  // scalar field
                                                         element_type);

            res[field_id] = GetOptFieldIvfData(field_type, field_datas);
        }
        return res;
    }
    auto segment_insert_files =
        index::GetValueFromConfig<std::vector<std::vector<std::string>>>(
            config, SEGMENT_INSERT_FILES_KEY);
    AssertInfo(segment_insert_files.has_value(),
               "insert file paths for storage v2 is empty when build index");
    auto remote_files = segment_insert_files.value();
    for (auto& files : remote_files) {
        SortByPath(files);
    }

    std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>> res;
    for (auto& [field_id, tup] : fields_map) {
        const auto& field_type = std::get<1>(tup);
        const auto& element_type = std::get<2>(tup);

        auto field_datas = GetFieldDatasFromStorageV2(
            remote_files, field_id, field_type, element_type, 1, fs_);

        res[field_id] = GetOptFieldIvfData(field_type, field_datas);
    }
    return res;
}

std::optional<bool>
MemFileManagerImpl::IsExisted(const std::string& filename) noexcept {
    // TODO: implement this interface
    return false;
}

bool
MemFileManagerImpl::RemoveFile(const std::string& filename) noexcept {
    // TODO: implement this interface
    return false;
}

size_t
MemFileManagerImpl::StreamWriteIndex(const std::string& filename,
                                     const std::vector<SerializeEntry>& entries,
                                     WriteEntryDataFn write_entry_data) {
    auto output = OpenOutputStream(filename);

    size_t total_written = 0;

    // Check if encryption is needed
    if (plugin_context_) {
        auto cipherPlugin = PluginLoader::GetInstance().getCipherPlugin();
        if (cipherPlugin) {
            auto [encryptor, edek] = cipherPlugin->GetEncryptor(
                plugin_context_->ez_id, plugin_context_->collection_id);

            if (encryptor) {
                total_written = StreamWriteIndexFileEncrypted(
                    output.get(),
                    field_meta_,
                    index_meta_,
                    entries,
                    write_entry_data,
                    encryptor,
                    edek,
                    std::to_string(plugin_context_->ez_id));

                output->Close();
                RegisterStreamingUpload(filename, total_written);
                return total_written;
            }
        }
    }

    // No encryption - use standard write
    total_written = StreamWriteIndexFile(
        output.get(), field_meta_, index_meta_, entries, write_entry_data);

    output->Close();
    RegisterStreamingUpload(filename, total_written);
    return total_written;
}

size_t
MemFileManagerImpl::StreamWriteTextLogIndex(
    const std::string& filename,
    const std::vector<SerializeEntry>& entries,
    WriteEntryDataFn write_entry_data) {
    auto output = OpenTextLogOutputStream(filename);

    size_t total_written = 0;

    // Check if encryption is needed
    if (plugin_context_) {
        auto cipherPlugin = PluginLoader::GetInstance().getCipherPlugin();
        if (cipherPlugin) {
            auto [encryptor, edek] = cipherPlugin->GetEncryptor(
                plugin_context_->ez_id, plugin_context_->collection_id);

            if (encryptor) {
                total_written = StreamWriteIndexFileEncrypted(
                    output.get(),
                    field_meta_,
                    index_meta_,
                    entries,
                    write_entry_data,
                    encryptor,
                    edek,
                    std::to_string(plugin_context_->ez_id));

                output->Close();
                RegisterTextLogStreamingUpload(filename, total_written);
                return total_written;
            }
        }
    }

    // No encryption - use standard write
    total_written = StreamWriteIndexFile(
        output.get(), field_meta_, index_meta_, entries, write_entry_data);

    output->Close();
    RegisterTextLogStreamingUpload(filename, total_written);
    return total_written;
}

}  // namespace milvus::storage
