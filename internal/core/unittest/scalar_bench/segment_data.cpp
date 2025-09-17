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

#include "segment_data.h"
#include "data_generator.h"
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include <set>

namespace milvus {
namespace scalar_bench {

SegmentData::SegmentData(const DataConfig& config)
    : config_(config), row_count_(config.segment_size) {
}

void
SegmentData::AddFieldData(const std::string& field_name, FieldData data) {
    field_data_[field_name] = std::move(data);
}

std::vector<std::string>
SegmentData::GetFieldNames() const {
    std::vector<std::string> names;
    for (const auto& [name, _] : field_data_) {
        names.push_back(name);
    }
    return names;
}

size_t
SegmentData::GetMemoryBytes() const {
    size_t total_bytes = sizeof(*this);

    for (const auto& [_, data] : field_data_) {
        total_bytes += GetFieldMemoryBytes(data);
    }

    for (const auto& [_, mask] : null_masks_) {
        total_bytes += mask.size() / 8;  // bool vector
    }

    return total_bytes;
}

size_t
SegmentData::GetFieldMemoryBytes(const FieldData& data) const {
    return std::visit([](const auto& vec) -> size_t {
        using T = typename std::decay_t<decltype(vec)>::value_type;
        if constexpr (std::is_same_v<T, std::string>) {
            size_t total = 0;
            for (const auto& s : vec) {
                total += s.size() + sizeof(std::string);
            }
            return total;
        } else {
            return vec.size() * sizeof(T);
        }
    }, data);
}

SegmentData::Statistics
SegmentData::GetFieldStatistics(const std::string& field_name) const {
    Statistics stats;

    auto it = field_data_.find(field_name);
    if (it == field_data_.end()) {
        return stats;
    }

    std::visit([&stats](const auto& vec) {
        using T = typename std::decay_t<decltype(vec)>::value_type;

        if (vec.empty()) {
            return;
        }

        if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>) {
            // 数值类型统计
            auto [min_it, max_it] = std::minmax_element(vec.begin(), vec.end());
            stats.min_value = static_cast<double>(*min_it);
            stats.max_value = static_cast<double>(*max_it);

            double sum = std::accumulate(vec.begin(), vec.end(), 0.0);
            stats.avg_value = sum / vec.size();

            std::set<T> unique_values(vec.begin(), vec.end());
            stats.unique_count = unique_values.size();
        } else if constexpr (std::is_same_v<T, std::string>) {
            // 字符串类型统计
            auto [min_it, max_it] = std::minmax_element(vec.begin(), vec.end());
            stats.min_string = *min_it;
            stats.max_string = *max_it;

            std::set<std::string> unique_values(vec.begin(), vec.end());
            stats.unique_count = unique_values.size();
        } else if constexpr (std::is_same_v<T, bool>) {
            // 布尔类型统计
            int true_count = std::count(vec.begin(), vec.end(), true);
            stats.min_value = 0;
            stats.max_value = 1;
            stats.avg_value = static_cast<double>(true_count) / vec.size();
            stats.unique_count = (true_count > 0 && true_count < vec.size()) ? 2 : 1;
        }
    }, it->second);

    return stats;
}

bool
SegmentData::ValidateData() const {
    // 检查所有字段的行数是否一致
    for (const auto& [name, data] : field_data_) {
        int64_t field_size = std::visit([](const auto& vec) -> int64_t {
            return vec.size();
        }, data);

        if (field_size != row_count_) {
            std::cerr << "Field " << name << " size mismatch: "
                      << field_size << " vs " << row_count_ << std::endl;
            return false;
        }
    }

    return true;
}

void
SegmentData::PrintSummary() const {
    std::cout << "\n=== Segment Data Summary ===" << std::endl;
    std::cout << "Configuration: " << config_.name << std::endl;
    std::cout << "Row Count: " << row_count_ << std::endl;
    std::cout << "Memory Usage: " << GetMemoryBytes() / (1024.0 * 1024.0) << " MB" << std::endl;

    std::cout << "\nField Statistics:" << std::endl;
    std::cout << std::setw(20) << "Field Name"
              << std::setw(15) << "Type"
              << std::setw(15) << "Unique Values"
              << std::setw(20) << "Min"
              << std::setw(20) << "Max" << std::endl;
    std::cout << std::string(90, '-') << std::endl;

    for (const auto& [field_name, field_data] : field_data_) {
        auto stats = GetFieldStatistics(field_name);

        std::cout << std::setw(20) << field_name;

        // 打印类型
        std::visit([](const auto& vec) {
            using T = typename std::decay_t<decltype(vec)>::value_type;
            if constexpr (std::is_same_v<T, int64_t>) {
                std::cout << std::setw(15) << "INT64";
            } else if constexpr (std::is_same_v<T, double>) {
                std::cout << std::setw(15) << "DOUBLE";
            } else if constexpr (std::is_same_v<T, std::string>) {
                std::cout << std::setw(15) << "VARCHAR";
            } else if constexpr (std::is_same_v<T, bool>) {
                std::cout << std::setw(15) << "BOOL";
            } else {
                std::cout << std::setw(15) << "UNKNOWN";
            }
        }, field_data);

        std::cout << std::setw(15) << stats.unique_count;

        // 打印最小/最大值
        if (!stats.min_string.empty()) {
            std::cout << std::setw(20) << stats.min_string.substr(0, 18)
                      << std::setw(20) << stats.max_string.substr(0, 18);
        } else {
            std::cout << std::setw(20) << stats.min_value
                      << std::setw(20) << stats.max_value;
        }

        std::cout << std::endl;
    }
}

// SegmentDataGenerator 实现

std::shared_ptr<SegmentData>
SegmentDataGenerator::GenerateSegmentData(const DataConfig& config) {
    auto segment_data = std::make_shared<SegmentData>(config);

    // 生成主键字段（始终为INT64）
    {
        std::vector<int64_t> pk_data;
        pk_data.reserve(config.segment_size);
        for (int64_t i = 0; i < config.segment_size; ++i) {
            pk_data.push_back(i);
        }
        segment_data->AddFieldData("pk", std::move(pk_data));
    }

    // 根据数据类型生成主要测试字段
    if (config.data_type == "INT64") {
        segment_data->AddFieldData("field", GenerateIntFieldData(config));
    } else if (config.data_type == "FLOAT" || config.data_type == "DOUBLE") {
        segment_data->AddFieldData("field", GenerateFloatFieldData(config));
    } else if (config.data_type == "VARCHAR") {
        segment_data->AddFieldData("field", GenerateStringFieldData(config));
    } else if (config.data_type == "BOOL") {
        segment_data->AddFieldData("field", GenerateBoolFieldData(config));
    } else {
        // 默认生成INT64
        segment_data->AddFieldData("field", GenerateIntFieldData(config));
    }

    // 添加额外的测试字段（用于复合条件测试）
    {
        DataConfig int_config = config;
        int_config.cardinality = 100;
        segment_data->AddFieldData("field2", GenerateIntFieldData(int_config));
    }

    return segment_data;
}

FieldData
SegmentDataGenerator::GenerateIntFieldData(const DataConfig& config) {
    DataGenerator gen;
    auto data = gen.GenerateIntData(
        config.segment_size,
        config.distribution,
        0,                      // min_val
        config.segment_size,    // max_val
        config.cardinality
    );

    return data;
}

FieldData
SegmentDataGenerator::GenerateFloatFieldData(const DataConfig& config) {
    DataGenerator gen;
    auto data = gen.GenerateFloatData(
        config.segment_size,
        config.distribution,
        0.0,                         // min_val
        static_cast<double>(config.segment_size)  // max_val
    );

    // 应用基数限制（如果需要）
    if (config.cardinality > 0 && config.cardinality < data.size()) {
        // 简单的基数限制：将数据量化到指定数量的桶
        double range = config.segment_size;
        double bucket_size = range / config.cardinality;

        for (auto& val : data) {
            int bucket = static_cast<int>(val / bucket_size);
            val = bucket * bucket_size + bucket_size / 2;
        }
    }

    return data;
}

FieldData
SegmentDataGenerator::GenerateStringFieldData(const DataConfig& config) {
    DataGenerator gen;

    StringGenConfig string_config;
    // 根据基数选择合适的字符串生成模式
    if (config.cardinality < 100) {
        // 低基数：使用模板模式
        string_config.pattern = StringGenConfig::Pattern::TEMPLATE;
        string_config.template_config.prefix = "status_";
        string_config.template_config.numeric_digits = 3;
        string_config.template_config.zero_padding = false;
    } else if (config.cardinality < 10000) {
        // 中基数：使用模板模式
        string_config.pattern = StringGenConfig::Pattern::TEMPLATE;
        string_config.template_config.prefix = "user_";
        string_config.template_config.suffix = "_data";
        string_config.template_config.numeric_digits = 7;
    } else {
        // 高基数：使用UUID模式
        string_config.pattern = StringGenConfig::Pattern::UUID_LIKE;
    }

    string_config.distribution = config.distribution;
    string_config.cardinality = config.cardinality;

    auto data = gen.GenerateStringData(config.segment_size, string_config);

    return data;
}

FieldData
SegmentDataGenerator::GenerateBoolFieldData(const DataConfig& config) {
    DataGenerator gen;

    // 对于布尔类型，基数最多为2
    double true_ratio = 0.5;
    if (config.cardinality == 1) {
        true_ratio = 1.0;  // 全部为true或全部为false
    }

    auto data = gen.GenerateBoolData(config.segment_size, true_ratio);

    return data;
}

} // namespace scalar_bench
} // namespace milvus