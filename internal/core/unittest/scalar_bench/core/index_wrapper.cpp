// Copyright (C) 2019-2024 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "index_wrapper.h"
#include "segcore/load_index_c.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "index/IndexFactory.h"
#include "index/NgramInvertedIndex.h"
#include "pb/schema.pb.h"
#include <iostream>

namespace milvus {
namespace scalar_bench {

// IndexWrapperBase 的默认 LoadToSegment 实现
void
IndexWrapperBase::LoadToSegment(SegmentWrapper& segment,
                                const std::string& field_name) {
    auto field_id = segment.GetFieldId(field_name);
    auto sealed_seg = segment.GetSealedSegment();

    // 从缓存中获取索引对象
    auto it = index_cache_.find(field_id.get());
    if (it == index_cache_.end()) {
        std::cerr << "Index not successfully built for field " << field_name << std::endl;
        throw std::runtime_error("Index not successfully built for field " + field_name);
    }

    // 参考 ChunkedSegmentSealedTest 的实现
    milvus::segcore::LoadIndexInfo load_info;

    // 使用 GenIndexParams 辅助函数
    load_info.index_params = GenIndexParams(it->second.get());

    // 创建测试用的 CacheIndex
    load_info.cache_index = CreateTestCacheIndex(field_name, std::move(it->second));

    load_info.field_id = field_id.get();

    // 加载索引到segment
    sealed_seg->LoadIndex(load_info);

    // 从缓存中移除，因为已经移动给了 segment
    index_cache_.erase(it);
}

// Helper function to get raw data from segment
template<typename T>
std::vector<T> GetSegmentFieldData(const SegmentWrapper& segment,
                                   FieldId field_id,
                                   int64_t row_count) {
    auto sealed_seg = segment.GetSealedSegment();

    // Get number of chunks for this field
    auto num_chunks = sealed_seg->num_chunk_data(field_id);
    if (num_chunks == 0) {
        throw std::runtime_error("No chunk data found for field");
    }

    std::vector<T> result;
    result.reserve(row_count);

    // Iterate through all chunks and collect data
    for (int64_t chunk_id = 0; chunk_id < num_chunks; ++chunk_id) {
        auto chunk_span = sealed_seg->chunk_data<T>(field_id, chunk_id);
        auto span_data = chunk_span.get();

        // Append chunk data to result
        result.insert(result.end(), span_data.data(), span_data.data() + span_data.row_count());
    }

    // Trim to exact row count if necessary
    if (result.size() > row_count) {
        result.resize(row_count);
    }

    return result;
}

// BitmapIndexWrapper 实现
IndexBuildResult
BitmapIndexWrapper::Build(const SegmentWrapper& segment,
                          const std::string& field_name,
                          const IndexConfig& config) {
    IndexBuildResult result;
    auto start = std::chrono::high_resolution_clock::now();

    auto field_id = segment.GetFieldId(field_name);
    auto schema = segment.GetSchema();
    const auto& field_meta_ref = schema->operator[](field_id);
    auto data_type = field_meta_ref.get_data_type();

    // 创建FileManagerContext
    proto::schema::FieldSchema proto_field_schema;
    proto_field_schema.set_fieldid(field_id.get());
    proto_field_schema.set_name(field_name);
    if (data_type == DataType::INT64) {
        proto_field_schema.set_data_type(proto::schema::DataType::Int64);
    } else if (data_type == DataType::INT32) {
        proto_field_schema.set_data_type(proto::schema::DataType::Int32);
    } else if (data_type == DataType::VARCHAR) {
        proto_field_schema.set_data_type(proto::schema::DataType::VarChar);
    } else if (data_type == DataType::FLOAT) {
        proto_field_schema.set_data_type(proto::schema::DataType::Float);
    } else if (data_type == DataType::DOUBLE) {
        proto_field_schema.set_data_type(proto::schema::DataType::Double);
    }
    auto field_meta = milvus::storage::FieldDataMeta{
        segment.GetCollectionId(),
        segment.GetPartitionId(),
        segment.GetSegmentId(),
        field_id.get(),
        proto_field_schema
    };

    auto index_meta = gen_index_meta(
        segment.GetSegmentId(),
        field_id.get(),
        4000,  // index_build_id
        4000); // index_version

    milvus::storage::FileManagerContext ctx(field_meta, index_meta, nullptr);

    // 创建Bitmap索引
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.field_type = data_type;
    create_index_info.index_type = milvus::index::BITMAP_INDEX_TYPE;
    create_index_info.field_name = field_name;

    auto index = milvus::index::IndexFactory::GetInstance().CreatePrimitiveScalarIndex(
        data_type, create_index_info, ctx);

    if (!index) {
        throw std::runtime_error("Failed to create bitmap index");
    }

    // 获取segment中的原始数据并构建索引
    auto sealed_seg = segment.GetSealedSegment();
    int64_t row_count = segment.GetRowCount();

    if (data_type == DataType::INT64) {
        auto data = GetSegmentFieldData<int64_t>(segment, field_id, row_count);
        index->BuildWithRawDataForUT(row_count, data.data());
    } else if (data_type == DataType::INT32) {
        auto data = GetSegmentFieldData<int32_t>(segment, field_id, row_count);
        index->BuildWithRawDataForUT(row_count, data.data());
    } else if (data_type == DataType::VARCHAR) {
        // VARCHAR需要特殊处理
        result.memory_bytes = 1024 * 1024; // 1MB 估算
        result.serialized_size = 512 * 1024;
        auto end = std::chrono::high_resolution_clock::now();
        result.build_time_ms = std::chrono::duration<double, std::milli>(end - start).count();
        return result;
    } else {
        throw std::runtime_error("Unsupported data type for bitmap index");
    }

    // 保存索引对象用于后续加载
    index_cache_[field_id.get()] = std::move(index);

    // 获取索引统计信息
    result.memory_bytes = 1024 * 1024;  // 暂时使用估算值
    result.serialized_size = 512 * 1024;  // 暂时使用估算值
    result.index_files = {};

    std::cout << "      Built bitmap index: memory="
                << result.memory_bytes / 1024.0 << " KB, "
                << "serialized=" << result.serialized_size / 1024.0 << " KB"
                << std::endl;

    auto end = std::chrono::high_resolution_clock::now();
    result.build_time_ms = std::chrono::duration<double, std::milli>(end - start).count();

    return result;
}

// InvertedIndexWrapper 实现
IndexBuildResult
InvertedIndexWrapper::Build(const SegmentWrapper& segment,
                            const std::string& field_name,
                            const IndexConfig& config) {
    IndexBuildResult result;
    auto start = std::chrono::high_resolution_clock::now();

    auto field_id = segment.GetFieldId(field_name);
    auto schema = segment.GetSchema();
    const auto& field_meta_ref = schema->operator[](field_id);
    auto data_type = field_meta_ref.get_data_type();

    // 创建FileManagerContext
    proto::schema::FieldSchema proto_field_schema;
    proto_field_schema.set_fieldid(field_id.get());
    proto_field_schema.set_name(field_name);
    if (data_type == DataType::INT64) {
        proto_field_schema.set_data_type(proto::schema::DataType::Int64);
    } else if (data_type == DataType::INT32) {
        proto_field_schema.set_data_type(proto::schema::DataType::Int32);
    } else if (data_type == DataType::VARCHAR) {
        proto_field_schema.set_data_type(proto::schema::DataType::VarChar);
    } else if (data_type == DataType::FLOAT) {
        proto_field_schema.set_data_type(proto::schema::DataType::Float);
    } else if (data_type == DataType::DOUBLE) {
        proto_field_schema.set_data_type(proto::schema::DataType::Double);
    }
    auto field_meta = milvus::storage::FieldDataMeta{
        segment.GetCollectionId(),
        segment.GetPartitionId(),
        segment.GetSegmentId(),
        field_id.get(),
        proto_field_schema
    };

    auto index_meta = gen_index_meta(
        segment.GetSegmentId(),
        field_id.get(),
        4001,  // index_build_id
        4001); // index_version

    milvus::storage::FileManagerContext ctx(field_meta, index_meta, nullptr);

    // 创建Inverted索引
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.field_type = data_type;
    create_index_info.index_type = milvus::index::INVERTED_INDEX_TYPE;
    create_index_info.field_name = field_name;

    // 对于字符串类型，使用Ngram索引
    if (data_type == DataType::VARCHAR) {
        auto ngram_params = milvus::index::NgramParams{
            .loading_index = false,
            .min_gram = 2,
            .max_gram = 4,
        };

        auto index = std::make_shared<milvus::index::NgramInvertedIndex>(ctx, ngram_params);

        // 使用 BuildWithFieldData 直接构建索引
        // 暂时简化处理
        result.memory_bytes = 2 * 1024 * 1024;  // 2MB 估算
        result.serialized_size = 1024 * 1024;  // 1MB 估算
        result.index_files = {};
    } else {
        // 数值类型的Inverted索引
        auto index = milvus::index::IndexFactory::GetInstance().CreatePrimitiveScalarIndex(
            data_type, create_index_info, ctx);

        if (!index) {
            throw std::runtime_error("Failed to create inverted index");
        }

        // 构建索引（类似Bitmap的处理）
        int64_t row_count = segment.GetRowCount();

        if (data_type == DataType::INT64) {
            auto data = GetSegmentFieldData<int64_t>(segment, field_id, row_count);
            index->BuildWithRawDataForUT(row_count, data.data());
        } else if (data_type == DataType::INT32) {
            auto data = GetSegmentFieldData<int32_t>(segment, field_id, row_count);
            index->BuildWithRawDataForUT(row_count, data.data());
        }

        // 保存索引对象用于后续加载（使用move避免拷贝）
        index_cache_[field_id.get()] = std::move(index);

        result.memory_bytes = 1024 * 1024;  // 暂时使用估算值
        result.serialized_size = 512 * 1024;  // 暂时使用估算值
        result.index_files = {};
    }

    std::cout << "      Built inverted index: memory="
                << result.memory_bytes / 1024.0 << " KB, "
                << "serialized=" << result.serialized_size / 1024.0 << " KB"
                << std::endl;

    auto end = std::chrono::high_resolution_clock::now();
    result.build_time_ms = std::chrono::duration<double, std::milli>(end - start).count();

    return result;
}

// STLSortIndexWrapper 实现
IndexBuildResult
STLSortIndexWrapper::Build(const SegmentWrapper& segment,
                          const std::string& field_name,
                          const IndexConfig& config) {
    IndexBuildResult result;
    auto start = std::chrono::high_resolution_clock::now();

    auto field_id = segment.GetFieldId(field_name);
    auto schema = segment.GetSchema();
    const auto& field_meta_ref = schema->operator[](field_id);
    auto data_type = field_meta_ref.get_data_type();

    // STL_SORT索引只适用于数值类型
    if (data_type != DataType::INT8 && data_type != DataType::INT16 &&
        data_type != DataType::INT32 && data_type != DataType::INT64 &&
        data_type != DataType::FLOAT && data_type != DataType::DOUBLE) {
        throw std::runtime_error("STL_SORT index only supports numeric types");
    }

    // 创建FileManagerContext
    proto::schema::FieldSchema proto_field_schema;
    proto_field_schema.set_fieldid(field_id.get());
    proto_field_schema.set_name(field_name);
    if (data_type == DataType::INT64) {
        proto_field_schema.set_data_type(proto::schema::DataType::Int64);
    } else if (data_type == DataType::INT32) {
        proto_field_schema.set_data_type(proto::schema::DataType::Int32);
    } else if (data_type == DataType::VARCHAR) {
        proto_field_schema.set_data_type(proto::schema::DataType::VarChar);
    } else if (data_type == DataType::FLOAT) {
        proto_field_schema.set_data_type(proto::schema::DataType::Float);
    } else if (data_type == DataType::DOUBLE) {
        proto_field_schema.set_data_type(proto::schema::DataType::Double);
    }
    auto field_meta = milvus::storage::FieldDataMeta{
        segment.GetCollectionId(),
        segment.GetPartitionId(),
        segment.GetSegmentId(),
        field_id.get(),
        proto_field_schema
    };

    auto index_meta = gen_index_meta(
        segment.GetSegmentId(),
        field_id.get(),
        4002,  // index_build_id
        4002); // index_version

    milvus::storage::FileManagerContext ctx(field_meta, index_meta, nullptr);

    // 创建STL_SORT索引
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.field_type = data_type;
    create_index_info.index_type = milvus::index::ASCENDING_SORT;
    create_index_info.field_name = field_name;

    auto index = milvus::index::IndexFactory::GetInstance().CreatePrimitiveScalarIndex(
        data_type, create_index_info, ctx);

    if (!index) {
        throw std::runtime_error("Failed to create STL_SORT index");
    }

    // 获取数据并构建索引
    int64_t row_count = segment.GetRowCount();

    if (data_type == DataType::INT64) {
        auto data = GetSegmentFieldData<int64_t>(segment, field_id, row_count);
        index->BuildWithRawDataForUT(row_count, data.data());
    } else if (data_type == DataType::INT32) {
        auto data = GetSegmentFieldData<int32_t>(segment, field_id, row_count);
        index->BuildWithRawDataForUT(row_count, data.data());
    } else if (data_type == DataType::FLOAT) {
        auto data = GetSegmentFieldData<float>(segment, field_id, row_count);
        index->BuildWithRawDataForUT(row_count, data.data());
    }

    // 保存索引对象用于后续加载
    index_cache_[field_id.get()] = std::move(index);

    result.memory_bytes = 1024 * 1024;  // 暂时使用估算值
    result.serialized_size = 512 * 1024;  // 暂时使用估算值
    result.index_files = {};

    std::cout << "      Built STL_SORT index: memory="
                << result.memory_bytes / 1024.0 << " KB, "
                << "serialized=" << result.serialized_size / 1024.0 << " KB"
                << std::endl;

    auto end = std::chrono::high_resolution_clock::now();
    result.build_time_ms = std::chrono::duration<double, std::milli>(end - start).count();

    return result;
}

// IndexWrapperFactory 实现
std::unique_ptr<IndexWrapperBase>
IndexWrapperFactory::CreateIndexWrapper(ScalarIndexType type) {
    switch (type) {
        case ScalarIndexType::BITMAP:
            return std::make_unique<BitmapIndexWrapper>();
        case ScalarIndexType::INVERTED:
            return std::make_unique<InvertedIndexWrapper>();
        case ScalarIndexType::STL_SORT:
            return std::make_unique<STLSortIndexWrapper>();
        default:
            return nullptr;
    }
}

// IndexManager 实现
IndexManager::IndexManager(std::shared_ptr<milvus::storage::ChunkManager> chunk_manager)
    : chunk_manager_(chunk_manager),
      next_index_build_id_(5000),
      next_index_id_(6000) {
}

IndexBuildResult
IndexManager::BuildAndLoadIndexForField(SegmentWrapper& segment,
                                        const std::string& field_name,
                                        const FieldIndexConfig& field_config) {
    IndexBuildResult result;

    // 如果是NONE类型，不构建索引
    if (field_config.type == ScalarIndexType::NONE) {
        result.build_time_ms = 0;
        result.memory_bytes = 0;
        result.serialized_size = 0;
        std::cout << "      No index (brute force scan)" << std::endl;
        return result;
    }

    // 创建对应的索引包装器
    auto wrapper = IndexWrapperFactory::CreateIndexWrapper(field_config.type);
    if (!wrapper) {
        throw std::runtime_error("Unsupported index type");
    }

    // Create a temporary IndexConfig with the field-specific settings
    // This is needed because Build() expects IndexConfig
    IndexConfig temp_config;
    temp_config.name = field_name + "_index";
    temp_config.field_configs[field_name] = field_config;

    result = wrapper->Build(segment, field_name, temp_config);

    // 加载索引到 Segment，失败将抛出异常
    wrapper->LoadToSegment(segment, field_name);

    return result;
}

} // namespace scalar_bench
} // namespace milvus