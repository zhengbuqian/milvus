
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

#include "storage/Util.h"
#include <memory>
#include "arrow/array/builder_binary.h"
#include "arrow/type_fwd.h"
#include "common/EasyAssert.h"
#include "common/Consts.h"
#include "fmt/format.h"
#include "storage/ChunkManager.h"
#ifdef AZURE_BUILD_DIR
#include "storage/AzureChunkManager.h"
#endif
#include "storage/FieldData.h"
#include "storage/InsertData.h"
#include "storage/FieldDataInterface.h"
#include "storage/ThreadPools.h"
#include "storage/LocalChunkManager.h"
#include "storage/MinioChunkManager.h"
#include "storage/OpenDALChunkManager.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/DiskFileManagerImpl.h"
#include "log/Log.h"

namespace milvus::storage {

std::map<std::string, ChunkManagerType> ChunkManagerType_Map = {
    {"local", ChunkManagerType::Local},
    {"minio", ChunkManagerType::Minio},
    {"remote", ChunkManagerType::Remote},
    {"opendal", ChunkManagerType::OpenDAL}};

enum class CloudProviderType : int8_t {
    UNKNOWN = 0,
    AWS = 1,
    GCP = 2,
    ALIYUN = 3,
    AZURE = 4,
};

std::map<std::string, CloudProviderType> CloudProviderType_Map = {
    {"aws", CloudProviderType::AWS},
    {"gcp", CloudProviderType::GCP},
    {"aliyun", CloudProviderType::ALIYUN},
    {"azure", CloudProviderType::AZURE}};

std::map<std::string, int> ReadAheadPolicy_Map = {
    {"normal", MADV_NORMAL},
    {"random", MADV_RANDOM},
    {"sequential", MADV_SEQUENTIAL},
    {"willneed", MADV_WILLNEED},
    {"dontneed", MADV_DONTNEED}};

StorageType
ReadMediumType(BinlogReaderPtr reader) {
    AssertInfo(reader->Tell() == 0,
               "medium type must be parsed from stream header");
    int32_t magic_num;
    auto ret = reader->Read(sizeof(magic_num), &magic_num);
    AssertInfo(ret.ok(), "read binlog failed");
    if (magic_num == MAGIC_NUM) {
        return StorageType::Remote;
    }

    return StorageType::LocalDisk;
}

void
add_vector_payload(std::shared_ptr<arrow::ArrayBuilder> builder,
                   uint8_t* values,
                   int length) {
    AssertInfo(builder != nullptr, "empty arrow builder");
    auto binary_builder =
        std::dynamic_pointer_cast<arrow::FixedSizeBinaryBuilder>(builder);
    auto ast = binary_builder->AppendValues(values, length);
    AssertInfo(ast.ok(), "append value to arrow builder failed");
}

// append values for numeric data
template <typename DT, typename BT>
void
add_numeric_payload(std::shared_ptr<arrow::ArrayBuilder> builder,
                    DT* start,
                    int length) {
    AssertInfo(builder != nullptr, "empty arrow builder");
    auto numeric_builder = std::dynamic_pointer_cast<BT>(builder);
    auto ast = numeric_builder->AppendValues(start, start + length);
    AssertInfo(ast.ok(), "append value to arrow builder failed");
}

void
AddPayloadToArrowBuilder(std::shared_ptr<arrow::ArrayBuilder> builder,
                         const Payload& payload) {
    AssertInfo(builder != nullptr, "empty arrow builder");
    auto raw_data = const_cast<uint8_t*>(payload.raw_data);
    auto length = payload.rows;
    auto data_type = payload.data_type;

    switch (data_type) {
        case DataType::BOOL: {
            auto bool_data = reinterpret_cast<bool*>(raw_data);
            add_numeric_payload<bool, arrow::BooleanBuilder>(
                builder, bool_data, length);
            break;
        }
        case DataType::INT8: {
            auto int8_data = reinterpret_cast<int8_t*>(raw_data);
            add_numeric_payload<int8_t, arrow::Int8Builder>(
                builder, int8_data, length);
            break;
        }
        case DataType::INT16: {
            auto int16_data = reinterpret_cast<int16_t*>(raw_data);
            add_numeric_payload<int16_t, arrow::Int16Builder>(
                builder, int16_data, length);
            break;
        }
        case DataType::INT32: {
            auto int32_data = reinterpret_cast<int32_t*>(raw_data);
            add_numeric_payload<int32_t, arrow::Int32Builder>(
                builder, int32_data, length);
            break;
        }
        case DataType::INT64: {
            auto int64_data = reinterpret_cast<int64_t*>(raw_data);
            add_numeric_payload<int64_t, arrow::Int64Builder>(
                builder, int64_data, length);
            break;
        }
        case DataType::FLOAT: {
            auto float_data = reinterpret_cast<float*>(raw_data);
            add_numeric_payload<float, arrow::FloatBuilder>(
                builder, float_data, length);
            break;
        }
        case DataType::DOUBLE: {
            auto double_data = reinterpret_cast<double_t*>(raw_data);
            add_numeric_payload<double, arrow::DoubleBuilder>(
                builder, double_data, length);
            break;
        }
        case DataType::VECTOR_FLOAT16:
        case DataType::VECTOR_BINARY:
        case DataType::VECTOR_FLOAT: {
            add_vector_payload(builder, const_cast<uint8_t*>(raw_data), length);
            break;
        }
        default: {
            PanicInfo(DataTypeInvalid,
                      fmt::format("unsupported data type 7 {}", data_type));
        }
    }
}

void
AddOneStringToArrowBuilder(std::shared_ptr<arrow::ArrayBuilder> builder,
                           const char* str,
                           int str_size) {
    AssertInfo(builder != nullptr, "empty arrow builder");
    auto string_builder =
        std::dynamic_pointer_cast<arrow::StringBuilder>(builder);
    arrow::Status ast;
    if (str == nullptr || str_size < 0) {
        ast = string_builder->AppendNull();
    } else {
        ast = string_builder->Append(str, str_size);
    }
    AssertInfo(ast.ok(), "append value to arrow builder failed");
}

void
AddOneBinaryToArrowBuilder(std::shared_ptr<arrow::ArrayBuilder> builder,
                           const uint8_t* data,
                           int length) {
    AssertInfo(builder != nullptr, "empty arrow builder");
    auto binary_builder =
        std::dynamic_pointer_cast<arrow::BinaryBuilder>(builder);
    arrow::Status ast;
    if (data == nullptr || length < 0) {
        ast = binary_builder->AppendNull();
    } else {
        ast = binary_builder->Append(data, length);
    }
    AssertInfo(ast.ok(), "append value to arrow builder failed");
}

std::shared_ptr<arrow::ArrayBuilder>
CreateArrowBuilder(DataType data_type) {
    switch (static_cast<DataType>(data_type)) {
        case DataType::BOOL: {
            return std::make_shared<arrow::BooleanBuilder>();
        }
        case DataType::INT8: {
            return std::make_shared<arrow::Int8Builder>();
        }
        case DataType::INT16: {
            return std::make_shared<arrow::Int16Builder>();
        }
        case DataType::INT32: {
            return std::make_shared<arrow::Int32Builder>();
        }
        case DataType::INT64: {
            return std::make_shared<arrow::Int64Builder>();
        }
        case DataType::FLOAT: {
            return std::make_shared<arrow::FloatBuilder>();
        }
        case DataType::DOUBLE: {
            return std::make_shared<arrow::DoubleBuilder>();
        }
        case DataType::VARCHAR:
        case DataType::STRING: {
            return std::make_shared<arrow::StringBuilder>();
        }
        case DataType::ARRAY:
        case DataType::JSON: {
            return std::make_shared<arrow::BinaryBuilder>();
        }
        default: {
            PanicInfo(
                DataTypeInvalid,
                fmt::format("unsupported numeric data type {}", data_type));
        }
    }
}

std::shared_ptr<arrow::ArrayBuilder>
CreateArrowBuilder(DataType data_type, int dim) {
    switch (static_cast<DataType>(data_type)) {
        case DataType::VECTOR_FLOAT: {
            AssertInfo(dim > 0, "invalid dim value");
            return std::make_shared<arrow::FixedSizeBinaryBuilder>(
                arrow::fixed_size_binary(dim * sizeof(float)));
        }
        case DataType::VECTOR_BINARY: {
            AssertInfo(dim % 8 == 0 && dim > 0, "invalid dim value");
            return std::make_shared<arrow::FixedSizeBinaryBuilder>(
                arrow::fixed_size_binary(dim / 8));
        }
        case DataType::VECTOR_FLOAT16: {
            AssertInfo(dim > 0, "invalid dim value");
            return std::make_shared<arrow::FixedSizeBinaryBuilder>(
                arrow::fixed_size_binary(dim * sizeof(float16)));
        }
        default: {
            PanicInfo(
                DataTypeInvalid,
                fmt::format("unsupported vector data type {}", data_type));
        }
    }
}

std::shared_ptr<arrow::Schema>
CreateArrowSchema(DataType data_type) {
    switch (static_cast<DataType>(data_type)) {
        case DataType::BOOL: {
            return arrow::schema({arrow::field("val", arrow::boolean())});
        }
        case DataType::INT8: {
            return arrow::schema({arrow::field("val", arrow::int8())});
        }
        case DataType::INT16: {
            return arrow::schema({arrow::field("val", arrow::int16())});
        }
        case DataType::INT32: {
            return arrow::schema({arrow::field("val", arrow::int32())});
        }
        case DataType::INT64: {
            return arrow::schema({arrow::field("val", arrow::int64())});
        }
        case DataType::FLOAT: {
            return arrow::schema({arrow::field("val", arrow::float32())});
        }
        case DataType::DOUBLE: {
            return arrow::schema({arrow::field("val", arrow::float64())});
        }
        case DataType::VARCHAR:
        case DataType::STRING: {
            return arrow::schema({arrow::field("val", arrow::utf8())});
        }
        case DataType::ARRAY:
        case DataType::JSON: {
            return arrow::schema({arrow::field("val", arrow::binary())});
        }
        default: {
            PanicInfo(
                DataTypeInvalid,
                fmt::format("unsupported numeric data type {}", data_type));
        }
    }
}

std::shared_ptr<arrow::Schema>
CreateArrowSchema(DataType data_type, int dim) {
    switch (static_cast<DataType>(data_type)) {
        case DataType::VECTOR_FLOAT: {
            AssertInfo(dim > 0, "invalid dim value");
            return arrow::schema({arrow::field(
                "val", arrow::fixed_size_binary(dim * sizeof(float)))});
        }
        case DataType::VECTOR_BINARY: {
            AssertInfo(dim % 8 == 0 && dim > 0, "invalid dim value");
            return arrow::schema(
                {arrow::field("val", arrow::fixed_size_binary(dim / 8))});
        }
        case DataType::VECTOR_FLOAT16: {
            AssertInfo(dim > 0, "invalid dim value");
            return arrow::schema({arrow::field(
                "val", arrow::fixed_size_binary(dim * sizeof(float16)))});
        }
        // case DataType::VECTOR_SPARSE_FLOAT: {
        //     AssertInfo(dim > 0, "invalid dim value");
        //     return arrow::schema({arrow::field(
        //         "val", arrow::fixed_size_binary(dim * sizeof(float)))});
        // }
        default: {
            PanicInfo(
                DataTypeInvalid,
                fmt::format("unsupported vector data type {}", data_type));
        }
    }
}

int
GetDimensionFromFileMetaData(const parquet::ColumnDescriptor* schema,
                             DataType data_type) {
    switch (data_type) {
        case DataType::VECTOR_FLOAT: {
            return schema->type_length() / sizeof(float);
        }
        case DataType::VECTOR_BINARY: {
            return schema->type_length() * 8;
        }
        case DataType::VECTOR_FLOAT16: {
            return schema->type_length() / sizeof(float16);
        }
        case DataType::VECTOR_SPARSE_FLOAT: {
            // TODO(SPARSE): do not allow this method call on sparse
            return 1;
        }
        default:
            PanicInfo(DataTypeInvalid,
                      fmt::format("unsupported data type 6 {}", data_type));
    }
}

int
GetDimensionFromArrowArray(std::shared_ptr<arrow::Array> data,
                           DataType data_type) {
    switch (data_type) {
        case DataType::VECTOR_FLOAT: {
            AssertInfo(
                data->type()->id() == arrow::Type::type::FIXED_SIZE_BINARY,
                "inconsistent data type");
            auto array =
                std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(data);
            return array->byte_width() / sizeof(float);
        }
        case DataType::VECTOR_BINARY: {
            AssertInfo(
                data->type()->id() == arrow::Type::type::FIXED_SIZE_BINARY,
                "inconsistent data type");
            auto array =
                std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(data);
            return array->byte_width() * 8;
        }
        default:
            PanicInfo(DataTypeInvalid,
                      fmt::format("unsupported data type 5 {}", data_type));
    }
}

std::string
GenIndexPathPrefix(ChunkManagerPtr cm,
                   int64_t build_id,
                   int64_t index_version) {
    return cm->GetRootPath() + "/" + std::string(INDEX_ROOT_PATH) + "/" +
           std::to_string(build_id) + "/" + std::to_string(index_version) + "/";
}

std::string
GetIndexPathPrefixWithBuildID(ChunkManagerPtr cm, int64_t build_id) {
    return cm->GetRootPath() + "/" + std::string(INDEX_ROOT_PATH) + "/" +
           std::to_string(build_id);
}

std::string
GenFieldRawDataPathPrefix(ChunkManagerPtr cm,
                          int64_t segment_id,
                          int64_t field_id) {
    return cm->GetRootPath() + "/" + std::string(RAWDATA_ROOT_PATH) + "/" +
           std::to_string(segment_id) + "/" + std::to_string(field_id) + "/";
}

std::string
GetSegmentRawDataPathPrefix(ChunkManagerPtr cm, int64_t segment_id) {
    return cm->GetRootPath() + "/" + std::string(RAWDATA_ROOT_PATH) + "/" +
           std::to_string(segment_id);
}

std::unique_ptr<DataCodec>
DownloadAndDecodeRemoteFile(ChunkManager* chunk_manager,
                            const std::string& file) {
    auto fileSize = chunk_manager->Size(file);
    auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[fileSize]);
    chunk_manager->Read(file, buf.get(), fileSize);

    return DeserializeFileData(buf, fileSize);
}

std::pair<std::string, size_t>
EncodeAndUploadIndexSlice(ChunkManager* chunk_manager,
                          uint8_t* buf,
                          int64_t batch_size,
                          IndexMeta index_meta,
                          FieldDataMeta field_meta,
                          std::string object_key) {
    auto field_data = CreateFieldData(DataType::INT8);
    field_data->FillFieldData(buf, batch_size);
    auto indexData = std::make_shared<IndexData>(field_data);
    indexData->set_index_meta(index_meta);
    indexData->SetFieldDataMeta(field_meta);
    auto serialized_index_data = indexData->serialize_to_remote_file();
    auto serialized_index_size = serialized_index_data.size();
    chunk_manager->Write(
        object_key, serialized_index_data.data(), serialized_index_size);
    return std::make_pair(std::move(object_key), serialized_index_size);
}

std::pair<std::string, size_t>
EncodeAndUploadFieldSlice(ChunkManager* chunk_manager,
                          uint8_t* buf,
                          int64_t element_count,
                          FieldDataMeta field_data_meta,
                          const FieldMeta& field_meta,
                          std::string object_key) {
    auto field_data =
        CreateFieldData(field_meta.get_data_type(), field_meta.get_dim(), 0);
    field_data->FillFieldData(buf, element_count);
    auto insertData = std::make_shared<InsertData>(field_data);
    insertData->SetFieldDataMeta(field_data_meta);
    auto serialized_inserted_data = insertData->serialize_to_remote_file();
    auto serialized_inserted_data_size = serialized_inserted_data.size();
    chunk_manager->Write(
        object_key, serialized_inserted_data.data(), serialized_inserted_data_size);
    return std::make_pair(std::move(object_key), serialized_inserted_data_size);
}

std::vector<FieldDataPtr>
GetObjectData(ChunkManager* remote_chunk_manager,
              const std::vector<std::string>& remote_files) {
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);
    std::vector<std::future<std::unique_ptr<DataCodec>>> futures;
    for (auto& file : remote_files) {
        futures.emplace_back(pool.Submit(
            DownloadAndDecodeRemoteFile, remote_chunk_manager, file));
    }

    std::vector<FieldDataPtr> datas;
    for (int i = 0; i < futures.size(); ++i) {
        auto res = futures[i].get();
        datas.emplace_back(res->GetFieldData());
    }
    ReleaseArrowUnused();
    return datas;
}

std::map<std::string, int64_t>
PutIndexData(ChunkManager* remote_chunk_manager,
             const std::vector<const uint8_t*>& data_slices,
             const std::vector<int64_t>& slice_sizes,
             const std::vector<std::string>& slice_names,
             FieldDataMeta& field_meta,
             IndexMeta& index_meta) {
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    std::vector<std::future<std::pair<std::string, size_t>>> futures;
    AssertInfo(data_slices.size() == slice_sizes.size(),
               "inconsistent size of data slices with slice sizes!");
    AssertInfo(data_slices.size() == slice_names.size(),
               "inconsistent size of data slices with slice names!");

    for (int64_t i = 0; i < data_slices.size(); ++i) {
        futures.push_back(pool.Submit(EncodeAndUploadIndexSlice,
                                      remote_chunk_manager,
                                      const_cast<uint8_t*>(data_slices[i]),
                                      slice_sizes[i],
                                      index_meta,
                                      field_meta,
                                      slice_names[i]));
    }

    std::map<std::string, int64_t> remote_paths_to_size;
    for (auto& future : futures) {
        auto res = future.get();
        remote_paths_to_size[res.first] = res.second;
    }

    ReleaseArrowUnused();
    return remote_paths_to_size;
}

int64_t
GetTotalNumRowsForFieldDatas(const std::vector<FieldDataPtr>& field_datas) {
    int64_t count = 0;
    for (auto& field_data : field_datas) {
        count += field_data->get_num_rows();
    }

    return count;
}

size_t
GetNumRowsForLoadInfo(const LoadFieldDataInfo& load_info) {
    if (load_info.field_infos.empty()) {
        return 0;
    }

    auto& field = load_info.field_infos.begin()->second;
    return field.row_count;
}

void
ReleaseArrowUnused() {
    static std::mutex release_mutex;

    // While multiple threads are releasing memory,
    // we don't need everyone do releasing,
    // just let some of them do this also works well
    if (release_mutex.try_lock()) {
        arrow::default_memory_pool()->ReleaseUnused();
        release_mutex.unlock();
    }
}

ChunkManagerPtr
CreateChunkManager(const StorageConfig& storage_config) {
    auto storage_type = ChunkManagerType_Map[storage_config.storage_type];

    switch (storage_type) {
        case ChunkManagerType::Local: {
            return std::make_shared<LocalChunkManager>(
                storage_config.root_path);
        }
        case ChunkManagerType::Minio: {
            return std::make_shared<MinioChunkManager>(storage_config);
        }
        case ChunkManagerType::Remote: {
            auto cloud_provider_type =
                CloudProviderType_Map[storage_config.cloud_provider];
            switch (cloud_provider_type) {
                case CloudProviderType::AWS: {
                    return std::make_shared<AwsChunkManager>(storage_config);
                }
                case CloudProviderType::GCP: {
                    return std::make_shared<GcpChunkManager>(storage_config);
                }
                case CloudProviderType::ALIYUN: {
                    return std::make_shared<AliyunChunkManager>(storage_config);
                }
#ifdef AZURE_BUILD_DIR
                case CloudProviderType::AZURE: {
                    return std::make_shared<AzureChunkManager>(storage_config);
                }
#endif
                default: {
                    return std::make_shared<MinioChunkManager>(storage_config);
                }
            }
        }
        case ChunkManagerType::OpenDAL: {
            return std::make_shared<OpenDALChunkManager>(storage_config);
        }

        default: {
            PanicInfo(ConfigInvalid,
                      fmt::format("unsupported storage_config.storage_type {}",
                                  fmt::underlying(storage_type)));
        }
    }
}

FieldDataPtr
CreateFieldData(const DataType& type, int64_t dim, int64_t total_num_rows) {
    switch (type) {
        case DataType::BOOL:
            return std::make_shared<FieldData<bool>>(type, total_num_rows);
        case DataType::INT8:
            return std::make_shared<FieldData<int8_t>>(type, total_num_rows);
        case DataType::INT16:
            return std::make_shared<FieldData<int16_t>>(type, total_num_rows);
        case DataType::INT32:
            return std::make_shared<FieldData<int32_t>>(type, total_num_rows);
        case DataType::INT64:
            return std::make_shared<FieldData<int64_t>>(type, total_num_rows);
        case DataType::FLOAT:
            return std::make_shared<FieldData<float>>(type, total_num_rows);
        case DataType::DOUBLE:
            return std::make_shared<FieldData<double>>(type, total_num_rows);
        case DataType::STRING:
        case DataType::VARCHAR:
            return std::make_shared<FieldData<std::string>>(type,
                                                            total_num_rows);
        case DataType::JSON:
            return std::make_shared<FieldData<Json>>(type, total_num_rows);
        case DataType::ARRAY:
            return std::make_shared<FieldData<Array>>(type, total_num_rows);
        case DataType::VECTOR_FLOAT:
            return std::make_shared<FieldData<FloatVector>>(
                dim, type, total_num_rows);
        case DataType::VECTOR_BINARY:
            return std::make_shared<FieldData<BinaryVector>>(
                dim, type, total_num_rows);
        case DataType::VECTOR_FLOAT16:
            return std::make_shared<FieldData<Float16Vector>>(
                dim, type, total_num_rows);
        case DataType::VECTOR_SPARSE_FLOAT:
            return std::make_shared<FieldData<SparseFloatVector>>(
                type, total_num_rows);
        default:
            throw SegcoreError(
                DataTypeInvalid,
                "CreateFieldData not support data type " + datatype_name(type));
    }
}

int64_t
GetByteSizeOfFieldDatas(const std::vector<FieldDataPtr>& field_datas) {
    int64_t result = 0;
    for (auto& data : field_datas) {
        result += data->Size();
    }

    return result;
}

std::vector<storage::FieldDataPtr>
CollectFieldDataChannel(storage::FieldDataChannelPtr& channel) {
    std::vector<storage::FieldDataPtr> result;
    storage::FieldDataPtr field_data;
    while (channel->pop(field_data)) {
        result.push_back(field_data);
    }
    return result;
}

storage::FieldDataPtr
MergeFieldData(std::vector<storage::FieldDataPtr>& data_array) {
    if (data_array.size() == 0) {
        return nullptr;
    }

    if (data_array.size() == 1) {
        return data_array[0];
    }

    size_t total_length = 0;
    for (const auto& data : data_array) {
        total_length += data->Length();
    }

    auto merged_data = storage::CreateFieldData(data_array[0]->get_data_type());
    merged_data->Reserve(total_length);
    for (const auto& data : data_array) {
        merged_data->FillFieldData(data->Data(), data->Length());
    }
    return merged_data;
}

}  // namespace milvus::storage
