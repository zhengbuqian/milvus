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
#include "test_utils/storage_test_utils.h"
#include "index/IndexFactory.h"
#include "indexbuilder/IndexFactory.h"
#include "index/Meta.h"
#include "pb/schema.pb.h"
#include "../utils/bench_paths.h"
#include <iostream>
#include <atomic>

namespace milvus {
namespace scalar_bench {
namespace {

inline int64_t
GenerateUniqueIdMsSeq() {
    static std::atomic<uint32_t> seq{0};
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();
    uint64_t s = seq.fetch_add(1, std::memory_order_relaxed) & 0xFFFFULL;
    return static_cast<int64_t>((static_cast<uint64_t>(now_ms) << 16) | s);
}
}  // namespace

void
IndexWrapper::LoadToSegment(SegmentWrapper& segment,
                            const std::string& field_name) {
    auto field_id = segment.GetFieldId(field_name);
    auto sealed_seg = segment.GetSealedSegment();

    auto it = index_cache_.find(field_id.get());
    if (it == index_cache_.end()) {
        std::cerr << "Index not successfully built for field " << field_name
                  << std::endl;
        throw std::runtime_error("Index not successfully built for field " +
                                 field_name);
    }

    milvus::segcore::LoadIndexInfo load_info;
    load_info.collection_id = segment.GetCollectionId();
    load_info.partition_id = segment.GetPartitionId();
    load_info.segment_id = segment.GetSegmentId();
    load_info.field_id = field_id.get();

    auto schema_ptr = segment.GetSchema();
    const auto& fm = schema_ptr->operator[](field_id);
    auto field_type = fm.get_data_type();
    load_info.field_type = field_type;
    load_info.element_type = DataType::NONE;

    auto& artifacts = it->second;
    auto field_meta = milvus::storage::FieldDataMeta{
        segment.GetCollectionId(),
        segment.GetPartitionId(),
        segment.GetSegmentId(),
        field_id.get(),
        artifacts.field_schema,
    };
    auto index_meta = milvus::storage::IndexMeta{
        segment.GetSegmentId(),
        field_id.get(),
        artifacts.index_build_id,
        artifacts.index_version,
    };
    auto root_path = GetSegmentsDir();
    auto storage_config = gen_local_storage_config(root_path);
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config);
    milvus::storage::FileManagerContext ctx(
        field_meta, index_meta, chunk_manager);
    ctx.set_for_loading_index(true);

    milvus::index::CreateIndexInfo index_info{};
    index_info.index_type = spec_.index_type;
    index_info.field_type = field_type;

    milvus::Config cfg;
    cfg["index_files"] = artifacts.index_files;
    cfg[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;

    auto index =
        milvus::index::IndexFactory::GetInstance().CreateIndex(index_info, ctx);
    index->Load(milvus::tracer::TraceContext{}, cfg);

    load_info.index_params = GenIndexParams(index.get());
    load_info.cache_index = CreateTestCacheIndex(field_name, std::move(index));
    load_info.field_id = field_id.get();
    sealed_seg->LoadIndex(load_info);
    index_cache_.erase(it);
}

IndexBuildResult
IndexWrapper::Build(const SegmentWrapper& segment,
                    const std::string& field_name,
                    const IndexConfig& config) {
    IndexBuildResult result;
    auto start = std::chrono::high_resolution_clock::now();

    auto field_id = segment.GetFieldId(field_name);
    auto schema = segment.GetSchema();
    const auto& field_meta_ref = schema->operator[](field_id);
    auto data_type = field_meta_ref.get_data_type();
    // 如 spec 要求仅支持数值，进行校验
    if (spec_.numeric_only) {
        bool is_numeric =
            (data_type == DataType::INT8 || data_type == DataType::INT16 ||
             data_type == DataType::INT32 || data_type == DataType::INT64 ||
             data_type == DataType::FLOAT || data_type == DataType::DOUBLE);
        if (!is_numeric) {
            throw std::runtime_error(spec_.name +
                                     " index only supports numeric types");
        }
    }

    // 创建FileManagerContext（使用与 Segment 相同的存储根路径）
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
    auto field_meta = milvus::storage::FieldDataMeta{segment.GetCollectionId(),
                                                     segment.GetPartitionId(),
                                                     segment.GetSegmentId(),
                                                     field_id.get(),
                                                     proto_field_schema};

    auto unique_id = GenerateUniqueIdMsSeq();
    auto index_meta = gen_index_meta(segment.GetSegmentId(),
                                     field_id.get(),
                                     unique_id,   // index_build_id
                                     unique_id);  // index_version

    auto root_path = GetSegmentsDir();
    auto storage_config = gen_local_storage_config(root_path);
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config);
    milvus::storage::FileManagerContext ctx(
        field_meta, index_meta, chunk_manager);

    // 通过 indexbuilder 离线构建并上传
    milvus::Config cfg;
    cfg[milvus::index::INDEX_TYPE] = spec_.index_type;
    // binlog 文件（SegmentWrapper 写入的路径模式）
    // 从 SegmentWrapper 获取真实写入的 binlog 路径
    auto insert_files = segment.GetFieldInsertFiles(field_id);
    cfg[INSERT_FILES_KEY] = insert_files;
    cfg[INDEX_NUM_ROWS_KEY] = segment.GetRowCount();

    auto builder =
        milvus::indexbuilder::IndexFactory::GetInstance().CreateIndex(
            data_type, cfg, ctx);
    builder->Build();
    auto stats = builder->Upload();

    result.memory_bytes = stats->GetMemSize();
    result.serialized_size = stats->GetSerializedSize();
    result.index_files = stats->GetIndexFiles();

    // 保存用于加载的构建产物
    BuiltIndexArtifacts artifacts;
    artifacts.index_files = result.index_files;
    artifacts.field_schema = proto_field_schema;
    artifacts.index_build_id = index_meta.build_id;
    artifacts.index_version = index_meta.index_version;
    artifacts.index_params = {
        {"index_type", spec_.index_type},
        {milvus::LOAD_PRIORITY, "HIGH"},
    };
    index_cache_[field_id.get()] = std::move(artifacts);

    std::cout << "      Built " << spec_.name
              << " index: memory=" << result.memory_bytes / 1024.0 << " KB, "
              << "serialized=" << result.serialized_size / 1024.0 << " KB"
              << std::endl;

    auto end = std::chrono::high_resolution_clock::now();
    result.build_time_ms =
        std::chrono::duration<double, std::milli>(end - start).count();

    return result;
}

IndexManager::IndexManager(
    std::shared_ptr<milvus::storage::ChunkManager> chunk_manager)
    : chunk_manager_(chunk_manager) {
}

IndexBuildResult
IndexManager::BuildAndLoadIndexForField(SegmentWrapper& segment,
                                        const std::string& field_name,
                                        const FieldIndexConfig& field_config) {
    IndexBuildResult result;

    if (field_config.type == ScalarIndexType::NONE) {
        result.build_time_ms = 0;
        result.memory_bytes = 0;
        result.serialized_size = 0;
        std::cout << "      No index (brute force scan)" << std::endl;
        return result;
    }

    std::unique_ptr<IndexWrapper> wrapper;
    switch (field_config.type) {
        case ScalarIndexType::BITMAP:
            wrapper =
                std::make_unique<IndexWrapper>(IndexWrapper::IndexBuildSpec{
                    .name = "BITMAP",
                    .index_type = milvus::index::BITMAP_INDEX_TYPE,
                    .build_id_seed = 4000,
                    .version_seed = 4000,
                    .numeric_only = false,
                });
            break;
        case ScalarIndexType::INVERTED:
            wrapper =
                std::make_unique<IndexWrapper>(IndexWrapper::IndexBuildSpec{
                    .name = "INVERTED",
                    .index_type = milvus::index::INVERTED_INDEX_TYPE,
                    .build_id_seed = 4001,
                    .version_seed = 4001,
                    .numeric_only = false,
                });
            break;
        case ScalarIndexType::STL_SORT:
            wrapper =
                std::make_unique<IndexWrapper>(IndexWrapper::IndexBuildSpec{
                    .name = "STL_SORT",
                    .index_type = milvus::index::ASCENDING_SORT,
                    .build_id_seed = 4002,
                    .version_seed = 4002,
                    .numeric_only = true,
                });
            break;
        default:
            throw std::runtime_error("Unsupported index type");
    }

    IndexConfig config;
    config.name = field_name + "_index";
    config.field_configs[field_name] = field_config;

    result = wrapper->Build(segment, field_name, config);
    wrapper->LoadToSegment(segment, field_name);

    return result;
}

}  // namespace scalar_bench
}  // namespace milvus