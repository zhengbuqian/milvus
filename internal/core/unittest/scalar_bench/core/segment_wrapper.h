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

#pragma once

#include <memory>
#include <vector>
#include <string>
#include <map>
#include <variant>

#include "common/Schema.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentSealed.h"
#include "segcore/segment_c.h"
#include "common/FieldData.h"
#include "storage/InsertData.h"
#include "storage/ChunkManager.h"
#include "test_utils/storage_test_utils.h"

// Forward declarations from benchmark config
#include "../config/benchmark_config.h"
#include "common/Types.h"

namespace milvus {
namespace scalar_bench {

// Forward declarations
class SegmentData;

// Schema构建器
class SchemaBuilder {
public:
    SchemaBuilder();

    // 添加字段
    void AddPrimaryKeyField(const std::string& name = "pk");
    void AddInt32Field(const std::string& name);
    void AddInt64Field(const std::string& name);
    void AddFloatField(const std::string& name);
    void AddDoubleField(const std::string& name);
    void AddVarCharField(const std::string& name, size_t max_length = 256);
    void AddBoolField(const std::string& name);

    // 构建Schema
    std::shared_ptr<milvus::Schema> Build();

private:
    std::shared_ptr<milvus::Schema> schema_;
};

// Segment包装器
class SegmentWrapper {
public:
    SegmentWrapper();
    ~SegmentWrapper() = default;

    // 初始化
    void Initialize(const DataConfig& config);

    // 从SegmentData加载数据到真实的Segment
    void LoadFromSegmentData(const SegmentData& segment_data);

    // 获取Schema
    std::shared_ptr<milvus::Schema> GetSchema() const { return schema_; }

    // 获取Sealed Segment
    std::shared_ptr<milvus::segcore::SegmentSealed> GetSealedSegment() const {
        return sealed_segment_;
    }

    // 获取字段ID映射
    FieldId GetFieldId(const std::string& field_name) const;

    // 获取行数
    int64_t GetRowCount() const { return row_count_; }

    // 获取Collection相关ID
    int64_t GetCollectionId() const { return collection_id_; }
    int64_t GetPartitionId() const { return partition_id_; }
    int64_t GetSegmentId() const { return segment_id_; }

    // 删除字段索引
    void DropIndex(FieldId field_id);

private:

    // 准备插入数据
    void WriteBinlogThenLoad(
        const std::string& field_name,
        FieldId field_id,
        const milvus::DataArray& field_data);

    // 加载系统字段
    void LoadSystemFields(const SegmentData& segment_data);

private:
    std::shared_ptr<milvus::Schema> schema_;
    std::shared_ptr<milvus::segcore::SegmentSealed> sealed_segment_;
    std::shared_ptr<milvus::storage::ChunkManager> chunk_manager_;

    std::map<std::string, FieldId> field_name_to_id_;
    std::map<FieldId, std::string> field_id_to_name_;

    int64_t collection_id_;
    int64_t partition_id_;
    int64_t segment_id_;
    int64_t row_count_;

    // 用于测试的ID分配
    static int64_t next_collection_id_;
    static int64_t next_segment_id_;
};

} // namespace scalar_bench
} // namespace milvus