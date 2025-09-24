#include "categorical_generator.h"
#include <stdexcept>
#include <algorithm>

namespace milvus {
namespace scalar_bench {

CategoricalGenerator::CategoricalGenerator(const FieldConfig& config)
    : config_(config) {
    if (config_.generator != FieldGeneratorType::CATEGORICAL) {
        throw std::runtime_error("Invalid generator type for CategoricalGenerator");
    }
    LoadValues();
    PrepareDuplicationRatios();
}

void CategoricalGenerator::LoadValues() {
    const auto& cat_config = config_.categorical_config;

    // Get values from dictionary or inline
    if (!cat_config.values.dictionary.empty()) {
        auto& registry = DictionaryRegistry::GetInstance();
        values_ = registry.GetDictionary(cat_config.values.dictionary, 0);
    } else if (!cat_config.values.inline_items.empty()) {
        values_ = cat_config.values.inline_items;
    } else {
        throw std::runtime_error("Categorical generator requires either dictionary or inline values");
    }

    if (values_.empty()) {
        throw std::runtime_error("Categorical generator has no values");
    }

    if (config_.categorical_config.type == DataType::INT64) {
        for (const auto& value : values_) {
            try {
                std::stoll(value);
            } catch (const std::exception&) {
                throw std::runtime_error("Categorical generator field '" + config_.field_name +
                                         "' expects numeric dictionary values but found '" + value + "'");
            }
        }
    }
}

void CategoricalGenerator::PrepareDuplicationRatios() {
    const auto& ratios = config_.categorical_config.duplication_ratios;
    const size_t value_count = values_.size();
    constexpr double kTolerance = 1e-6;

    cumulative_ratios_.clear();
    cumulative_ratios_.reserve(value_count);

    if (ratios.empty()) {
        double uniform_prob = 1.0 / static_cast<double>(value_count);
        double cumulative = 0.0;
        for (size_t i = 0; i < value_count; ++i) {
            cumulative += uniform_prob;
            if (i == value_count - 1) {
                cumulative = 1.0;
            }
            cumulative_ratios_.push_back(cumulative);
        }
        return;
    }

    if (ratios.size() > value_count) {
        throw std::runtime_error("Too many duplication ratios for available values");
    }

    double cumulative = 0.0;
    for (size_t i = 0; i < ratios.size(); ++i) {
        const double ratio = ratios[i];
        if (ratio < 0.0) {
            throw std::runtime_error("Duplication ratios must be non-negative");
        }
        cumulative += ratio;
        cumulative_ratios_.push_back(cumulative);
    }

    double remainder = 1.0 - cumulative;
    if (remainder < -kTolerance) {
        throw std::runtime_error("Duplication ratios must not sum to more than 1.0");
    }

    const size_t remaining_values = value_count - ratios.size();
    if (remaining_values > 0) {
        if (remainder <= kTolerance) {
            throw std::runtime_error(
                "Duplication ratios consume the full probability mass but do not cover all values");
        }

        double uniform_remainder = remainder / static_cast<double>(remaining_values);
        for (size_t i = 0; i < remaining_values; ++i) {
            cumulative += uniform_remainder;
            if (i == remaining_values - 1) {
                cumulative = 1.0;
            }
            cumulative_ratios_.push_back(cumulative);
        }
    } else {
        if (remainder > kTolerance) {
            throw std::runtime_error("Duplication ratios sum to less than 1.0");
        }
    }

    if (!cumulative_ratios_.empty()) {
        cumulative_ratios_.back() = 1.0;
    }
}

size_t CategoricalGenerator::SelectValueIndex(RandomContext& ctx) {
    double r = ctx.UniformReal(0, 1);

    // Binary search for the index
    auto it = std::lower_bound(cumulative_ratios_.begin(), cumulative_ratios_.end(), r);
    if (it == cumulative_ratios_.end()) {
        return cumulative_ratios_.size() - 1;
    }
    return std::distance(cumulative_ratios_.begin(), it);
}

DataArray CategoricalGenerator::Generate(size_t num_rows, RandomContext& ctx) {
    const auto& cat_config = config_.categorical_config;

    // Generate based on type
    if (cat_config.type == DataType::VARCHAR) {
        // VARCHAR is default
        std::vector<std::string> result;
        result.reserve(num_rows);

        for (size_t i = 0; i < num_rows; i++) {
            size_t idx = SelectValueIndex(ctx);
            if (idx >= values_.size()) {
                throw std::runtime_error("Selected value index out of range for field '" +
                                         config_.field_name + "'");
            }

            std::string value = values_[idx];

            // Apply max_length truncation if specified
            if (cat_config.max_length > 0 && value.length() > cat_config.max_length) {
                value = value.substr(0, cat_config.max_length);
            }

            result.push_back(value);
        }

        DataArray data_array;
        data_array.set_type(milvus::proto::schema::DataType::VarChar);
        auto* string_array = data_array.mutable_scalars()->mutable_string_data();
        string_array->mutable_data()->Reserve(result.size());
        for (auto& s : result) {
            string_array->add_data(std::move(s));
        }
        return data_array;
    } else if (cat_config.type == DataType::INT64) {
        auto values = GenerateTyped<int64_t>(num_rows, ctx);
        DataArray data_array;
        data_array.set_type(milvus::proto::schema::DataType::Int64);
        auto* long_array = data_array.mutable_scalars()->mutable_long_data();
        long_array->mutable_data()->Reserve(values.size());
        for (auto v : values) {
            long_array->add_data(v);
        }
        return data_array;
    } else {
        throw std::runtime_error("Unsupported categorical type.");
    }
}

template<typename T>
std::vector<T> CategoricalGenerator::GenerateTyped(size_t num_rows, RandomContext& ctx) {
    std::vector<T> result;
    result.reserve(num_rows);

    for (size_t i = 0; i < num_rows; i++) {
        size_t idx = SelectValueIndex(ctx);
        if (idx >= values_.size()) {
            throw std::runtime_error("Selected value index out of range for field '" +
                                     config_.field_name + "'");
        }

        const std::string& raw_value = values_[idx];

        // Try to convert string value to numeric type
        try {
            if constexpr (std::is_same_v<T, int64_t>) {
                result.push_back(std::stoll(raw_value));
            } else if constexpr (std::is_same_v<T, int32_t>) {
                result.push_back(std::stoi(raw_value));
            }
        } catch (const std::exception&) {
            throw std::runtime_error("Categorical generator field '" + config_.field_name +
                                     "' cannot parse value '" + raw_value + "' as numeric");
        }
    }

    return result;
}

} // namespace scalar_bench
} // namespace milvus