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
#include <unordered_map>
#include "../config/benchmark_config.h"
#include "common/Types.h"

namespace milvus {
namespace scalar_bench {

class SegmentData {
public:
    SegmentData(const DataConfig& config);
    ~SegmentData() = default;

    // 添加字段数据
    void AddFieldData(const std::string& field_name, const milvus::DataArray&& data);

    // Add field configuration
    void AddFieldConfig(const std::string& field_name, const FieldConfig& config);

    // 获取字段数据（DataArray）
    const milvus::DataArray& GetFieldDataArray(const std::string& field_name) const;

    // 获取行数
    int64_t GetRowCount() const { return row_count_; }

    // 获取字段名列表
    std::vector<std::string> GetFieldNames() const;

    // 获取数据配置
    const DataConfig& GetConfig() const { return config_; }

    // Get field configuration
    const FieldConfig* GetFieldConfig(const std::string& field_name) const;

    // 计算内存使用
    size_t GetMemoryBytes() const;

    // 数据统计信息
    struct Statistics {
        int64_t null_count = 0;
        int64_t unique_count = 0;
        double min_value = 0;
        double max_value = 0;
        double avg_value = 0;
        std::string min_string;
        std::string max_string;
    };

    // 获取字段统计信息
    Statistics GetFieldStatistics(const std::string& field_name) const;

    // 验证数据完整性
    bool ValidateData() const;

    // 打印数据摘要
    void PrintSummary() const;

private:
    DataConfig config_;
    int64_t row_count_;
    std::unordered_map<std::string, milvus::DataArray> field_data_;
    std::unordered_map<std::string, std::vector<bool>> null_masks_;
    std::unordered_map<std::string, FieldConfig> field_configs_;  // Store field configurations

    // 计算字段内存大小
    size_t GetFieldMemoryBytes(const milvus::DataArray& data) const;
};

// Segment数据生成器
class SegmentDataGenerator {
public:
    // 生成完整的Segment数据 - Only supports multi-field generation
    static std::shared_ptr<SegmentData> GenerateSegmentData(const DataConfig& config);

private:
    // Multi-field generator using field generators
    static std::shared_ptr<SegmentData> GenerateMultiFieldData(const DataConfig& config);
};

} // namespace scalar_bench
} // namespace milvus