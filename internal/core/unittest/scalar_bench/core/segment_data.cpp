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

int64_t
GetFieldDataRowCount(const proto::schema::FieldData& field_data) {
    if (!field_data.has_scalars()) {
        return 0;
    }

    const auto& scalars = field_data.scalars();
    switch (scalars.data_case()) {
        case proto::schema::ScalarField::kBoolData:
            return scalars.bool_data().data_size();
        case proto::schema::ScalarField::kIntData:
            return scalars.int_data().data_size();
        case proto::schema::ScalarField::kLongData:
            return scalars.long_data().data_size();
        case proto::schema::ScalarField::kFloatData:
            return scalars.float_data().data().size();
        case proto::schema::ScalarField::kDoubleData:
            return scalars.double_data().data().size();
        case proto::schema::ScalarField::kStringData:
            return scalars.string_data().data_size();
        case proto::schema::ScalarField::kArrayData:
            return scalars.array_data().data_size();
        case proto::schema::ScalarField::kJsonData:
            return scalars.json_data().data_size();
        case proto::schema::ScalarField::kBytesData:
            return scalars.bytes_data().data_size();
        case proto::schema::ScalarField::DATA_NOT_SET:
        default:
            return 0;
    }
}

SegmentData::SegmentData(const DataConfig& config)
    : config_(config), row_count_(config.segment_size) {
}

void
SegmentData::AddFieldData(const std::string& field_name,
                          proto::schema::FieldData data) {
    data.set_field_name(field_name);
    field_data_[field_name] = std::move(data);
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

const proto::schema::FieldData&
SegmentData::GetFieldData(const std::string& field_name) const {
    auto it = field_data_.find(field_name);
    if (it == field_data_.end()) {
        throw std::runtime_error("Field not found: " + field_name);
    }
    return it->second;
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
SegmentData::GetFieldMemoryBytes(const proto::schema::FieldData& data) const {
    return static_cast<size_t>(data.ByteSizeLong());
}

SegmentData::Statistics
SegmentData::GetFieldStatistics(const std::string& field_name) const {
    Statistics stats;

    auto it = field_data_.find(field_name);
    if (it == field_data_.end()) {
        return stats;
    }

    const auto& field_data = it->second;
    if (!field_data.has_scalars()) {
        return stats;
    }

    const auto& scalars = field_data.scalars();
    switch (scalars.data_case()) {
        case proto::schema::ScalarField::kBoolData: {
            const auto& data = scalars.bool_data();
            if (data.data_size() == 0) {
                break;
            }
            int true_count = 0;
            for (int i = 0; i < data.data_size(); ++i) {
                true_count += data.data(i) ? 1 : 0;
            }
            stats.min_value = 0;
            stats.max_value = 1;
            stats.avg_value = static_cast<double>(true_count) / data.data_size();
            stats.unique_count =
                (true_count > 0 && true_count < data.data_size()) ? 2 : 1;
            break;
        }
        case proto::schema::ScalarField::kIntData: {
            const auto& data = scalars.int_data();
            if (data.data_size() == 0) {
                break;
            }
            int32_t min_val = data.data(0);
            int32_t max_val = data.data(0);
            int64_t sum = 0;
            std::set<int32_t> unique_values;
            for (int i = 0; i < data.data_size(); ++i) {
                auto value = data.data(i);
                min_val = std::min(min_val, value);
                max_val = std::max(max_val, value);
                sum += value;
                unique_values.insert(value);
            }
            stats.min_value = static_cast<double>(min_val);
            stats.max_value = static_cast<double>(max_val);
            stats.avg_value = static_cast<double>(sum) / data.data_size();
            stats.unique_count = unique_values.size();
            break;
        }
        case proto::schema::ScalarField::kLongData: {
            const auto& data = scalars.long_data();
            if (data.data_size() == 0) {
                break;
            }
            int64_t min_val = data.data(0);
            int64_t max_val = data.data(0);
            long double sum = 0;
            std::set<int64_t> unique_values;
            for (int i = 0; i < data.data_size(); ++i) {
                auto value = data.data(i);
                min_val = std::min(min_val, value);
                max_val = std::max(max_val, value);
                sum += value;
                unique_values.insert(value);
            }
            stats.min_value = static_cast<double>(min_val);
            stats.max_value = static_cast<double>(max_val);
            stats.avg_value = static_cast<double>(sum / data.data_size());
            stats.unique_count = unique_values.size();
            break;
        }
        case proto::schema::ScalarField::kFloatData: {
            const auto& data = scalars.float_data().data();
            if (data.empty()) {
                break;
            }
            auto [min_it, max_it] = std::minmax_element(data.begin(), data.end());
            stats.min_value = static_cast<double>(*min_it);
            stats.max_value = static_cast<double>(*max_it);
            double sum = std::accumulate(data.begin(), data.end(), 0.0);
            stats.avg_value = sum / data.size();
            std::set<float> unique_values(data.begin(), data.end());
            stats.unique_count = unique_values.size();
            break;
        }
        case proto::schema::ScalarField::kDoubleData: {
            const auto& data = scalars.double_data().data();
            if (data.empty()) {
                break;
            }
            auto [min_it, max_it] = std::minmax_element(data.begin(), data.end());
            stats.min_value = *min_it;
            stats.max_value = *max_it;
            double sum = std::accumulate(data.begin(), data.end(), 0.0);
            stats.avg_value = sum / data.size();
            std::set<double> unique_values(data.begin(), data.end());
            stats.unique_count = unique_values.size();
            break;
        }
        case proto::schema::ScalarField::kStringData: {
            const auto& data = scalars.string_data();
            if (data.data_size() == 0) {
                break;
            }
            auto begin = data.data().begin();
            auto end = data.data().end();
            auto [min_it, max_it] = std::minmax_element(begin, end);
            stats.min_string = *min_it;
            stats.max_string = *max_it;
            std::set<std::string> unique_values(begin, end);
            stats.unique_count = unique_values.size();
            break;
        }
        case proto::schema::ScalarField::kArrayData: {
            stats.unique_count = scalars.array_data().data_size();
            break;
        }
        case proto::schema::ScalarField::kJsonData:
        case proto::schema::ScalarField::kBytesData:
        case proto::schema::ScalarField::DATA_NOT_SET:
        default:
            break;
    }

    return stats;
}

bool
SegmentData::ValidateData() const {
    // 检查所有字段的行数是否一致
    for (const auto& [name, data] : field_data_) {
        int64_t field_size = GetFieldDataRowCount(data);

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

        auto type_str = [&]() -> std::string {
            switch (field_data.type()) {
                case proto::schema::DataType::Bool:
                    return "BOOL";
                case proto::schema::DataType::Int8:
                    return "INT8";
                case proto::schema::DataType::Int16:
                    return "INT16";
                case proto::schema::DataType::Int32:
                    return "INT32";
                case proto::schema::DataType::Int64:
                    return "INT64";
                case proto::schema::DataType::Float:
                    return "FLOAT";
                case proto::schema::DataType::Double:
                    return "DOUBLE";
                case proto::schema::DataType::VarChar:
                    return "VARCHAR";
                case proto::schema::DataType::Array:
                    return "ARRAY";
                case proto::schema::DataType::Json:
                    return "JSON";
                default:
                    return "UNKNOWN";
            }
        }();
        std::cout << std::setw(15) << type_str;

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
        proto::schema::FieldData pk_field;
        pk_field.set_field_name("pk");
        pk_field.set_type(proto::schema::DataType::Int64);
        auto long_data = pk_field.mutable_scalars()->mutable_long_data();
        for (int64_t i = 0; i < config.segment_size; ++i) {
            long_data->add_data(i);
        }
        segment_data->AddFieldData("pk", std::move(pk_field));
    }

    // Generate data for each configured field
    for (const auto& field_config : config.fields) {
        try {
            // Create generator for this field
            auto generator = FieldGeneratorFactory::CreateGenerator(field_config);

            // Generate data
            auto field_data = generator->Generate(config.segment_size, ctx);

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