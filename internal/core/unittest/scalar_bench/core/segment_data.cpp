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
#include "../generators/field_generator.h"
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
SegmentData::AddFieldData(const std::string& field_name, const milvus::DataArray&& data) {
    field_data_[field_name] = std::move(data);
}

const milvus::DataArray&
SegmentData::GetFieldDataArray(const std::string& field_name) const {
    auto it = field_data_.find(field_name);
    if (it == field_data_.end()) {
        throw std::runtime_error("Field not found: " + field_name);
    }
    return it->second;
}

void
SegmentData::AddFieldConfig(const std::string& field_name, const FieldConfig& config) {
    field_configs_[field_name] = config;
}

const FieldConfig*
SegmentData::GetFieldConfig(const std::string& field_name) const {
    auto it = field_configs_.find(field_name);
    if (it == field_configs_.end()) {
        return nullptr;
    }
    return &it->second;
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
SegmentData::GetFieldMemoryBytes(const milvus::DataArray& data) const {
    // Rough estimate based on type content sizes
    switch (data.type()) {
        case milvus::proto::schema::DataType::Bool:
            return data.scalars().bool_data().data().size() * sizeof(bool);
        case milvus::proto::schema::DataType::Int8:
        case milvus::proto::schema::DataType::Int16:
        case milvus::proto::schema::DataType::Int32:
            return data.scalars().int_data().data().size() * sizeof(int32_t);
        case milvus::proto::schema::DataType::Int64:
            return data.scalars().long_data().data().size() * sizeof(int64_t);
        case milvus::proto::schema::DataType::Float:
            return data.scalars().float_data().data().size() * sizeof(float);
        case milvus::proto::schema::DataType::Double:
            return data.scalars().double_data().data().size() * sizeof(double);
        case milvus::proto::schema::DataType::VarChar:
        case milvus::proto::schema::DataType::String: {
            size_t total = 0;
            for (const auto& s : data.scalars().string_data().data()) {
                total += s.size() + sizeof(std::string);
            }
            return total;
        }
        case milvus::proto::schema::DataType::Array: {
            size_t total = 0;
            for (const auto& sf : data.scalars().array_data().data()) {
                // approximate scalar field sizes recursively for common cases
                if (sf.has_int_data()) total += sf.int_data().data().size() * sizeof(int32_t);
                if (sf.has_long_data()) total += sf.long_data().data().size() * sizeof(int64_t);
                if (sf.has_float_data()) total += sf.float_data().data().size() * sizeof(float);
                if (sf.has_double_data()) total += sf.double_data().data().size() * sizeof(double);
                if (sf.has_bool_data()) total += sf.bool_data().data().size() * sizeof(bool);
                if (sf.has_string_data()) {
                    for (const auto& s : sf.string_data().data()) total += s.size() + sizeof(std::string);
                }
            }
            return total;
        }
        default:
            return 0;
    }
}

SegmentData::Statistics
SegmentData::GetFieldStatistics(const std::string& field_name) const {
    Statistics stats;

    auto it = field_data_.find(field_name);
    if (it == field_data_.end()) {
        return stats;
    }

    const auto& data = it->second;
    switch (data.type()) {
        case milvus::proto::schema::DataType::Int8:
        case milvus::proto::schema::DataType::Int16:
        case milvus::proto::schema::DataType::Int32: {
            const auto& vec = data.scalars().int_data().data();
            if (vec.empty()) break;
            auto [min_it, max_it] = std::minmax_element(vec.begin(), vec.end());
            stats.min_value = static_cast<double>(*min_it);
            stats.max_value = static_cast<double>(*max_it);
            double sum = std::accumulate(vec.begin(), vec.end(), 0.0);
            stats.avg_value = sum / vec.size();
            std::set<int32_t> uniq(vec.begin(), vec.end());
            stats.unique_count = uniq.size();
            break;
        }
        case milvus::proto::schema::DataType::Int64: {
            const auto& vec = data.scalars().long_data().data();
            if (vec.empty()) break;
            auto [min_it, max_it] = std::minmax_element(vec.begin(), vec.end());
            stats.min_value = static_cast<double>(*min_it);
            stats.max_value = static_cast<double>(*max_it);
            double sum = std::accumulate(vec.begin(), vec.end(), 0.0);
            stats.avg_value = sum / vec.size();
            std::set<int64_t> uniq(vec.begin(), vec.end());
            stats.unique_count = uniq.size();
            break;
        }
        case milvus::proto::schema::DataType::Float: {
            const auto& vec = data.scalars().float_data().data();
            if (vec.empty()) break;
            auto [min_it, max_it] = std::minmax_element(vec.begin(), vec.end());
            stats.min_value = static_cast<double>(*min_it);
            stats.max_value = static_cast<double>(*max_it);
            double sum = std::accumulate(vec.begin(), vec.end(), 0.0);
            stats.avg_value = sum / vec.size();
            std::set<float> uniq(vec.begin(), vec.end());
            stats.unique_count = uniq.size();
            break;
        }
        case milvus::proto::schema::DataType::Double: {
            const auto& vec = data.scalars().double_data().data();
            if (vec.empty()) break;
            auto [min_it, max_it] = std::minmax_element(vec.begin(), vec.end());
            stats.min_value = static_cast<double>(*min_it);
            stats.max_value = static_cast<double>(*max_it);
            double sum = std::accumulate(vec.begin(), vec.end(), 0.0);
            stats.avg_value = sum / vec.size();
            std::set<double> uniq(vec.begin(), vec.end());
            stats.unique_count = uniq.size();
            break;
        }
        case milvus::proto::schema::DataType::Bool: {
            const auto& vec = data.scalars().bool_data().data();
            if (vec.empty()) break;
            int true_count = std::count(vec.begin(), vec.end(), true);
            stats.min_value = 0;
            stats.max_value = 1;
            stats.avg_value = static_cast<double>(true_count) / vec.size();
            stats.unique_count = (true_count > 0 && true_count < vec.size()) ? 2 : 1;
            break;
        }
        case milvus::proto::schema::DataType::VarChar:
        case milvus::proto::schema::DataType::String: {
            const auto& vec = data.scalars().string_data().data();
            if (vec.empty()) break;
            auto [min_it, max_it] = std::minmax_element(vec.begin(), vec.end());
            stats.min_string = *min_it;
            stats.max_string = *max_it;
            std::set<std::string> uniq(vec.begin(), vec.end());
            stats.unique_count = uniq.size();
            break;
        }
        default:
            break;
    }

    return stats;
}

bool
SegmentData::ValidateData() const {
    // 检查所有字段的行数是否一致
    for (const auto& [name, data] : field_data_) {
        int64_t field_size = 0;
        switch (data.type()) {
            case milvus::proto::schema::DataType::Bool:
                field_size = data.scalars().bool_data().data_size();
                break;
            case milvus::proto::schema::DataType::Int8:
            case milvus::proto::schema::DataType::Int16:
            case milvus::proto::schema::DataType::Int32:
                field_size = data.scalars().int_data().data_size();
                break;
            case milvus::proto::schema::DataType::Int64:
                field_size = data.scalars().long_data().data_size();
                break;
            case milvus::proto::schema::DataType::Float:
                field_size = data.scalars().float_data().data_size();
                break;
            case milvus::proto::schema::DataType::Double:
                field_size = data.scalars().double_data().data_size();
                break;
            case milvus::proto::schema::DataType::VarChar:
            case milvus::proto::schema::DataType::String:
                field_size = data.scalars().string_data().data_size();
                break;
            case milvus::proto::schema::DataType::Array:
                field_size = data.scalars().array_data().data_size();
                break;
            default:
                field_size = 0;
        }

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

        // 打印类型（简化）
        std::cout << std::setw(15) << static_cast<int>(field_data.type());

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
    // Check if this is a multi-field configuration
    if (!config.fields.empty()) {
        return GenerateMultiFieldData(config);
    }

    // Legacy single-field generation is no longer supported
    // All data generation must use the multi-field configuration
    throw std::runtime_error(
        "Single-field data generation is no longer supported. "
        "Please use multi-field configuration with 'fields' array.");
}

std::shared_ptr<SegmentData>
SegmentDataGenerator::GenerateMultiFieldData(const DataConfig& config) {
    auto segment_data = std::make_shared<SegmentData>(config);

    // Create random context with seed
    RandomContext ctx(config.segment_seed > 0 ? config.segment_seed : 42);

    // Always generate primary key field first
    {
        milvus::DataArray pk_array;
        pk_array.set_type(milvus::proto::schema::DataType::Int64);
        pk_array.set_field_name("pk");
        pk_array.set_is_dynamic(false);
        auto* long_array = pk_array.mutable_scalars()->mutable_long_data();
        long_array->mutable_data()->Reserve(config.segment_size);
        for (int64_t i = 0; i < config.segment_size; ++i) {
            long_array->add_data(i);
        }
        segment_data->AddFieldData("pk", std::move(pk_array));
    }

    // Generate data for each configured field
    for (const auto& field_config : config.fields) {
        try {
            // Create generator for this field
            auto generator = FieldGeneratorFactory::CreateGenerator(field_config);

            // Generate data
            DataArray field_data = generator->Generate(config.segment_size, ctx);

            // Add to segment
            segment_data->AddFieldData(field_config.field_name, std::move(field_data));
            segment_data->AddFieldConfig(field_config.field_name, field_config);

            std::cout << "Generated field: " << field_config.field_name
                      << " (rows=" << config.segment_size << ")" << std::endl;

        } catch (const std::exception& e) {
            throw std::runtime_error(
                "Failed to generate field '" + field_config.field_name + "': " + e.what());
        }
    }

    return segment_data;
}

} // namespace scalar_bench
} // namespace milvus