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

#include "segment_wrapper.h"
#include "segment_data.h"
#include "storage/Util.h"
#include "../utils/bench_paths.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "common/Consts.h"
// #include "storage/RemoteChunkManagerSingleton.h" // not used directly here
// #include "storage/LocalChunkManagerSingleton.h" // not used directly here
#include "test_utils/DataGen.h"
#include <iostream>
#include <numeric>

namespace milvus {
namespace scalar_bench {

int64_t SegmentWrapper::next_collection_id_ = 1000;
int64_t SegmentWrapper::next_segment_id_ = 2000;

// SchemaBuilder 实现
SchemaBuilder::SchemaBuilder() {
    schema_ = std::make_shared<milvus::Schema>();
}

void
SchemaBuilder::AddPrimaryKeyField(const std::string& name) {
    auto field_id = schema_->AddDebugField(name, DataType::INT64);
    schema_->set_primary_field_id(field_id);
}

void
SchemaBuilder::AddInt32Field(const std::string& name) {
    schema_->AddDebugField(name, DataType::INT32);
}

void
SchemaBuilder::AddInt64Field(const std::string& name) {
    schema_->AddDebugField(name, DataType::INT64);
}

void
SchemaBuilder::AddFloatField(const std::string& name) {
    schema_->AddDebugField(name, DataType::FLOAT);
}

void
SchemaBuilder::AddDoubleField(const std::string& name) {
    schema_->AddDebugField(name, DataType::DOUBLE);
}

void
SchemaBuilder::AddVarCharField(const std::string& name, size_t max_length) {
    // Note: DataType::VARCHAR doesn't have explicit max_length in AddDebugField
    // The max_length is handled internally by Milvus
    schema_->AddDebugField(name, DataType::VARCHAR);
}

void
SchemaBuilder::AddBoolField(const std::string& name) {
    schema_->AddDebugField(name, DataType::BOOL);
}

std::shared_ptr<milvus::Schema>
SchemaBuilder::Build() {
    return schema_;
}

// SegmentWrapper 实现
SegmentWrapper::SegmentWrapper()
    : collection_id_(next_collection_id_++),
      partition_id_(1),
      segment_id_(next_segment_id_++),
      row_count_(0) {
}

void
SegmentWrapper::Initialize(const DataConfig& config) {
    if (config.fields.empty()) {
        // No fields defined - this is an error
        throw std::runtime_error("No fields defined in data config");
    }
    
    // 构建Schema
    SchemaBuilder builder;

    // Check if pk field is already defined in the field configs
    bool has_pk = false;
    for (const auto& field_config : config.fields) {
        if (field_config.field_name == "pk") {
            has_pk = true;
            // TODO: 支持生成 字符串类型的 pk
            // Add pk as primary key field
            builder.AddPrimaryKeyField("pk");
            break;
        }
    }

    // If no pk field defined, add a default one
    if (!has_pk) {
        builder.AddPrimaryKeyField("pk");
    }

    // Build schema from field configurations (skip pk if already added)
    for (const auto& field_config : config.fields) {
        if (field_config.field_name == "pk") {
            continue;  // Already added as primary key
        }
        // Map field_type to schema field type
        switch (field_config.field_type) {
            case DataType::INT64:
                builder.AddInt64Field(field_config.field_name);
                break;
            case DataType::DOUBLE:
                builder.AddDoubleField(field_config.field_name);
                break;
            case DataType::VARCHAR: {
                // Get max length from string config if using string generator
                size_t max_len = 256;
                if (field_config.generator == FieldGeneratorType::CATEGORICAL) {
                    max_len = field_config.categorical_config.max_length > 0 ? field_config.categorical_config.max_length : 256;
                } else if (field_config.generator == FieldGeneratorType::VARCHAR) {
                    max_len = field_config.varchar_config.max_length > 0 ? field_config.varchar_config.max_length : 512;
                }
                builder.AddVarCharField(field_config.field_name, max_len);
                break;
            }
            case DataType::BOOL:
                builder.AddBoolField(field_config.field_name);
                break;
            case DataType::ARRAY: {
                builder.AddInt64Field(field_config.field_name);
                break;
            }
            default:
                builder.AddInt64Field(field_config.field_name); // Default fallback
                break;
        }
    }

    schema_ = builder.Build();

    // 建立字段名到ID的映射
    for (const auto& [field_id, field_meta] : schema_->get_fields()) {
        field_name_to_id_[field_meta.get_name().get()] = field_id;
        field_id_to_name_[field_id] = field_meta.get_name().get();
    }

    // 创建 Sealed Segment
    sealed_segment_ = milvus::segcore::CreateSealedSegment(schema_);

    // 初始化 ChunkManager
    std::string root_path = GetSegmentsDir();
    auto storage_config = gen_local_storage_config(root_path);
    chunk_manager_ = milvus::storage::CreateChunkManager(storage_config);

    std::cout << "    Initialized segment with schema:"
              << " collection_id=" << collection_id_
              << ", segment_id=" << segment_id_
              << ", fields=" << schema_->get_fields().size() << std::endl;
}

void
SegmentWrapper::LoadFromSegmentData(const SegmentData& segment_data) {
    row_count_ = segment_data.GetRowCount();

    // 首先加载系统字段 (row id 和 timestamp)
    LoadSystemFields(segment_data);

    // 为每个字段准备并加载数据
    for (const auto& field_name : segment_data.GetFieldNames()) {
        auto it = field_name_to_id_.find(field_name);
        if (it == field_name_to_id_.end()) {
            std::cerr << "Warning: Field " << field_name << " not found in schema" << std::endl;
            continue;
        }

        FieldId field_id = it->second;
        const auto& field_schema = schema_->operator[](field_id);
        DataType data_type = field_schema.get_data_type();

        // 获取字段数据（DataArray）并写入
        try {
            const auto& data_array = segment_data.GetFieldDataArray(field_name);
            WriteBinlogThenLoad(field_name, field_id, data_array);
        } catch (const std::exception& e) {
            std::cerr << "Error loading field " << field_name << ": " << e.what() << std::endl;
        }
    }

    std::cout << "    Loaded " << row_count_ << " rows into sealed segment" << std::endl;
}

void
SegmentWrapper::WriteBinlogThenLoad(const std::string& field_name,
                                   FieldId field_id,
                                   const milvus::DataArray& field_data) {
    const auto& field_schema = schema_->operator[](field_id);
    DataType data_type = field_schema.get_data_type();

    auto storage_field_data = milvus::segcore::CreateFieldDataFromDataArray(row_count_, &field_data, field_schema);

    // 准备binlog
    auto field_data_info = PrepareSingleFieldInsertBinlog(
        collection_id_,
        partition_id_,
        segment_id_,
        field_id.get(),
        {storage_field_data},
        chunk_manager_);

    // 加载数据到segment
    sealed_segment_->LoadFieldData(field_data_info);
}

FieldId
SegmentWrapper::GetFieldId(const std::string& field_name) const {
    auto it = field_name_to_id_.find(field_name);
    if (it == field_name_to_id_.end()) {
        throw std::runtime_error("Field not found: " + field_name);
    }
    return it->second;
}

void
SegmentWrapper::DropIndex(FieldId field_id) {
    if (sealed_segment_ != nullptr) {
        // Cast to ChunkedSegmentSealedImpl to access DropIndex method
        auto chunked_segment = std::dynamic_pointer_cast<milvus::segcore::ChunkedSegmentSealedImpl>(sealed_segment_);
        if (chunked_segment) {
            chunked_segment->DropIndex(field_id);
        }
    }
}

void
SegmentWrapper::LoadSystemFields(const SegmentData& segment_data) {
    int64_t row_count = segment_data.GetRowCount();

    // TODO: 允许生成 row id 和 timestamp 
    // 生成 row id 数据 (从 0 开始递增)
    std::vector<int64_t> row_ids(row_count);
    std::iota(row_ids.begin(), row_ids.end(), 0);

    // 生成 timestamp 数据 (使用相同的值，表示批量插入)
    std::vector<int64_t> timestamps(row_count, 1000000);  // 使用固定时间戳

    // 加载 row id
    {
        auto field_data = milvus::storage::CreateFieldData(
            DataType::INT64, DataType::NONE, false, 1, 0);
        field_data->FillFieldData(row_ids.data(), row_count);

        auto field_data_info = PrepareSingleFieldInsertBinlog(
            collection_id_,
            partition_id_,
            segment_id_,
            RowFieldID.get(),
            {field_data},
            chunk_manager_);

        sealed_segment_->LoadFieldData(field_data_info);
    }

    // 加载 timestamp
    {
        auto field_data = milvus::storage::CreateFieldData(
            DataType::INT64, DataType::NONE, false, 1, 0);
        field_data->FillFieldData(timestamps.data(), row_count);

        auto field_data_info = PrepareSingleFieldInsertBinlog(
            collection_id_,
            partition_id_,
            segment_id_,
            TimestampFieldID.get(),
            {field_data},
            chunk_manager_);

        sealed_segment_->LoadFieldData(field_data_info);
    }
}

} // namespace scalar_bench
} // namespace milvus