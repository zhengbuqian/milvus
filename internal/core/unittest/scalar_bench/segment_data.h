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
#include <variant>
#include <unordered_map>
#include <stdexcept>
#include "scalar_filter_benchmark.h"

namespace milvus {
namespace scalar_bench {

// 字段数据的变体类型
using FieldData = std::variant<
    std::vector<int8_t>,
    std::vector<int16_t>,
    std::vector<int32_t>,
    std::vector<int64_t>,
    std::vector<float>,
    std::vector<double>,
    std::vector<bool>,
    std::vector<std::string>
>;

// Segment数据包装类
class SegmentData {
public:
    SegmentData(const DataConfig& config);
    ~SegmentData() = default;

    // 添加字段数据
    void AddFieldData(const std::string& field_name, FieldData data);

    // 获取字段数据
    template<typename T>
    const std::vector<T>& GetFieldData(const std::string& field_name) const {
        auto it = field_data_.find(field_name);
        if (it == field_data_.end()) {
            throw std::runtime_error("Field not found: " + field_name);
        }
        return std::get<std::vector<T>>(it->second);
    }

    // 获取行数
    int64_t GetRowCount() const { return row_count_; }

    // 获取字段名列表
    std::vector<std::string> GetFieldNames() const;

    // 获取数据配置
    const DataConfig& GetConfig() const { return config_; }

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
    std::unordered_map<std::string, FieldData> field_data_;
    std::unordered_map<std::string, std::vector<bool>> null_masks_;

    // 计算字段内存大小
    size_t GetFieldMemoryBytes(const FieldData& data) const;
};

// Segment数据生成器
class SegmentDataGenerator {
public:
    // 生成完整的Segment数据
    static std::shared_ptr<SegmentData> GenerateSegmentData(const DataConfig& config);

private:
    // 生成不同类型的字段数据
    static FieldData GenerateIntFieldData(const DataConfig& config);
    static FieldData GenerateFloatFieldData(const DataConfig& config);
    static FieldData GenerateStringFieldData(const DataConfig& config);
    static FieldData GenerateBoolFieldData(const DataConfig& config);
};

} // namespace scalar_bench
} // namespace milvus