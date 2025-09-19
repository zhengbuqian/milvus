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
#include "storage/Util.h"
#include "bench_paths.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "common/Consts.h"
// #include "storage/RemoteChunkManagerSingleton.h" // not used directly here
// #include "storage/LocalChunkManagerSingleton.h" // not used directly here
// #include "test_utils/DataGen.h" // not used directly here
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
SchemaBuilder::AddInt64Field(const std::string& name) {
    schema_->AddDebugField(name, DataType::INT64);
}

void
SchemaBuilder::AddFloatField(const std::string& name) {
    schema_->AddDebugField(name, DataType::FLOAT);
}

void
SchemaBuilder::AddVarCharField(const std::string& name) {
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
    // 构建Schema
    SchemaBuilder builder;
    builder.AddPrimaryKeyField("pk");

    // 根据数据类型添加主要字段
    if (config.data_type == "INT64") {
        builder.AddInt64Field("field");
    } else if (config.data_type == "FLOAT" || config.data_type == "DOUBLE") {
        builder.AddFloatField("field");
    } else if (config.data_type == "VARCHAR") {
        builder.AddVarCharField("field");
    } else if (config.data_type == "BOOL") {
        builder.AddBoolField("field");
    } else {
        // 默认INT64
        builder.AddInt64Field("field");
    }

    // 添加辅助字段（用于复合查询）
    builder.AddInt64Field("field2");

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

        // 根据数据类型获取字段数据
        try {
            if (data_type == DataType::INT64) {
                const auto& data = segment_data.GetFieldData<int64_t>(field_name);
                PrepareInsertData(field_name, field_id, data);
            } else if (data_type == DataType::FLOAT) {
                const auto& data = segment_data.GetFieldData<double>(field_name);
                // 转换为float
                std::vector<float> float_data(data.begin(), data.end());
                PrepareInsertData(field_name, field_id, float_data);
            } else if (data_type == DataType::VARCHAR) {
                const auto& data = segment_data.GetFieldData<std::string>(field_name);
                PrepareInsertData(field_name, field_id, data);
            } else if (data_type == DataType::BOOL) {
                const auto& data = segment_data.GetFieldData<bool>(field_name);
                PrepareInsertData(field_name, field_id, data);
            }
        } catch (const std::exception& e) {
            std::cerr << "Error loading field " << field_name << ": " << e.what() << std::endl;
        }
    }

    std::cout << "    Loaded " << row_count_ << " rows into sealed segment" << std::endl;
}

void
SegmentWrapper::PrepareInsertData(const std::string& field_name,
                                   FieldId field_id,
                                   const FieldData& field_data) {
    const auto& field_schema = schema_->operator[](field_id);
    DataType data_type = field_schema.get_data_type();

    // 创建FieldData
    auto storage_field_data = CreateFieldDataFromVector(data_type, field_data);

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

std::shared_ptr<milvus::FieldDataBase>
SegmentWrapper::CreateFieldDataFromVector(DataType data_type,
                                           const FieldData& field_data) {
    auto storage_field_data = milvus::storage::CreateFieldData(
        data_type, DataType::NONE, false, 1, 0);

    // 根据类型填充数据
    std::visit([&storage_field_data](const auto& vec) {
        using T = typename std::decay_t<decltype(vec)>::value_type;

        if constexpr (std::is_same_v<T, int64_t>) {
            storage_field_data->FillFieldData(vec.data(), vec.size());
        } else if constexpr (std::is_same_v<T, float>) {
            storage_field_data->FillFieldData(vec.data(), vec.size());
        } else if constexpr (std::is_same_v<T, double>) {
            // 转换为float
            std::vector<float> float_vec(vec.begin(), vec.end());
            storage_field_data->FillFieldData(float_vec.data(), float_vec.size());
        } else if constexpr (std::is_same_v<T, bool>) {
            // vector<bool> is specialized and doesn't have a data() method,
            // so we need to convert to a regular vector of uint8_t
            std::vector<uint8_t> bool_vec(vec.size());
            for (size_t i = 0; i < vec.size(); ++i) {
                bool_vec[i] = vec[i] ? 1 : 0;
            }
            storage_field_data->FillFieldData(bool_vec.data(), bool_vec.size());
        } else if constexpr (std::is_same_v<T, std::string>) {
            storage_field_data->FillFieldData(vec.data(), vec.size());
        }
    }, field_data);

    return storage_field_data;
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