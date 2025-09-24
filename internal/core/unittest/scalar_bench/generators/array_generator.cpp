#include "array_generator.h"
#include <algorithm>
#include <stdexcept>
#include <unordered_set>

namespace milvus {
namespace scalar_bench {

ArrayGenerator::ArrayGenerator(const FieldConfig& config)
    : config_(config) {
    if (config_.generator != FieldGeneratorType::ARRAY) {
        throw std::runtime_error("Invalid generator type for ArrayGenerator");
    }
    InitializeElementGenerator();
}

void ArrayGenerator::InitializeElementGenerator() {
    const auto& array_config = config_.array_config;

    // Create element generator based on element configuration
    if (!array_config.element) {
        throw std::runtime_error("Array generator requires element configuration");
    }

    ValidateElementGenerator(*array_config.element);
    element_generator_ = FieldGeneratorFactory::CreateGenerator(*array_config.element);
    element_type_ = array_config.element->field_type;
}

FieldColumn ArrayGenerator::Generate(size_t num_rows, RandomContext& ctx) {
    switch (element_type_) {
        case DataType::BOOL:
            return GenerateBooleanArrays(num_rows, ctx);
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
            return GenerateNumericArrays(num_rows, ctx, element_type_);
        case DataType::FLOAT:
        case DataType::DOUBLE:
            return GenerateFloatArrays(num_rows, ctx, element_type_);
        case DataType::VARCHAR:
            return GenerateStringArrays(num_rows, ctx);
        default:
            throw std::runtime_error("Unsupported element type for array generator");
    }
}

size_t ArrayGenerator::DetermineArrayLength(RandomContext& ctx) {
    const auto& array_config = config_.array_config;
    const auto& length = array_config.length;

    size_t array_length;

    if (length.min == length.max) {
        array_length = length.min;
    } else {
        // LengthConfig.distribution is string in current header; interpret basic cases
        if (length.distribution == Distribution::UNIFORM) {
            array_length = ctx.UniformInt(length.min, length.max);
        } else if (length.distribution == Distribution::ZIPF) {
            array_length = length.min + ctx.Zipf(length.max - length.min + 1);
        } else if (length.distribution == Distribution::NORMAL) {
            double avg = length.avg > 0 ? length.avg : (length.min + length.max) / 2.0;
            double stddev = (length.max - length.min) / 6.0;
            double val = ctx.Normal(avg, stddev);
            array_length = static_cast<size_t>(std::max(
                static_cast<double>(length.min),
                std::min(static_cast<double>(length.max), val)));
        } else {
            array_length = ctx.UniformInt(length.min, length.max);
        }
    }

    // Apply max_capacity limit
    if (array_config.max_capacity > 0) {
        array_length = std::min(array_length, static_cast<size_t>(array_config.max_capacity));
    }

    return array_length;
}

template <typename T>
std::vector<std::vector<T>>
ArrayGenerator::GenerateTyped(size_t num_rows, RandomContext& ctx) {
    std::vector<std::vector<T>> arrays;
    arrays.reserve(num_rows);

    for (size_t i = 0; i < num_rows; ++i) {
        const size_t length = DetermineArrayLength(ctx);
        std::vector<T> elements;
        elements.reserve(length);

        AppendGeneratedElements(elements, length, ctx);
        ApplyContainsRules(elements, ctx);

        if (config_.array_config.unique) {
            size_t previous_size = elements.size();
            int attempts = 0;
            while (elements.size() < length && attempts < 3) {
                EnsureUniqueness(elements);
                if (elements.size() >= length || elements.size() == previous_size) {
                    break;
                }
                previous_size = elements.size();
                AppendGeneratedElements(elements, length, ctx);
                attempts++;
            }
            EnsureUniqueness(elements);
        }

        if (elements.size() > length) {
            elements.resize(length);
        }
        arrays.push_back(std::move(elements));
    }

    return arrays;
}

FieldColumn ArrayGenerator::GenerateStringArrays(size_t num_rows, RandomContext& ctx) {
    std::vector<std::vector<std::string>> arrays;
    arrays.reserve(num_rows);

    for (size_t i = 0; i < num_rows; ++i) {
        const size_t length = DetermineArrayLength(ctx);
        std::vector<std::string> elements;
        elements.reserve(length);

        AppendGeneratedStringElements(elements, length, ctx);
        ApplyStringContainsRules(elements, ctx, length);

        if (config_.array_config.unique) {
            size_t previous_size = elements.size();
            int attempts = 0;
            while (elements.size() < length && attempts < 3) {
                EnsureStringUniqueness(elements);
                if (elements.size() >= length || elements.size() == previous_size) {
                    break;
                }
                previous_size = elements.size();
                AppendGeneratedStringElements(elements, length, ctx);
                attempts++;
            }
            EnsureStringUniqueness(elements);
        }

        if (elements.size() > length) {
            elements.resize(length);
        }
        arrays.push_back(std::move(elements));
    }
    return arrays;
}

template <typename T>
void ArrayGenerator::AppendGeneratedElements(std::vector<T>& values,
                                             size_t min_count,
                                             RandomContext& ctx) {
    while (values.size() < min_count) {
        FieldColumn element_column = element_generator_->Generate(min_count - values.size(), ctx);
        auto batch = ExtractValues<T>(std::move(element_column));
        values.insert(values.end(), batch.begin(), batch.end());
        if (batch.empty()) {
            break;
        }
    }
}

void ArrayGenerator::AppendGeneratedStringElements(std::vector<std::string>& values,
                                                   size_t min_count,
                                                   RandomContext& ctx) {
    while (values.size() < min_count) {
        FieldColumn element_column = element_generator_->Generate(min_count - values.size(), ctx);
        auto batch = ExtractStringValues(std::move(element_column));
        values.insert(values.end(), batch.begin(), batch.end());
        if (batch.empty()) {
            break;
        }
    }
}

FieldColumn ArrayGenerator::GenerateNumericArrays(size_t num_rows,
                                                  RandomContext& ctx,
                                                  DataType numeric_type) {
    switch (numeric_type) {
        case DataType::INT8:
            return GenerateTyped<int8_t>(num_rows, ctx);
        case DataType::INT16:
            return GenerateTyped<int16_t>(num_rows, ctx);
        case DataType::INT32:
            return GenerateTyped<int32_t>(num_rows, ctx);
        case DataType::INT64:
            return GenerateTyped<int64_t>(num_rows, ctx);
        default:
            throw std::runtime_error("Unsupported integer array element type");
    }
}

FieldColumn ArrayGenerator::GenerateFloatArrays(size_t num_rows,
                                                RandomContext& ctx,
                                                DataType numeric_type) {
    switch (numeric_type) {
        case DataType::FLOAT:
            return GenerateTyped<float>(num_rows, ctx);
        case DataType::DOUBLE:
            return GenerateTyped<double>(num_rows, ctx);
        default:
            throw std::runtime_error("Unsupported float array element type");
    }
}

FieldColumn ArrayGenerator::GenerateBooleanArrays(size_t num_rows, RandomContext& ctx) {
    return GenerateTyped<bool>(num_rows, ctx);
}

template <typename T>
std::vector<T>
ArrayGenerator::ExtractValues(FieldColumn&& column) {
    if (auto values = std::get_if<std::vector<T>>(&column)) {
        return std::move(*values);
    }
    throw std::runtime_error("Array element generator returned unexpected type");
}

std::vector<std::string>
ArrayGenerator::ExtractStringValues(FieldColumn&& column) {
    if (auto values = std::get_if<std::vector<std::string>>(&column)) {
        return std::move(*values);
    }
    throw std::runtime_error("Array element generator returned unexpected type");
}

template <typename T>
void ArrayGenerator::ApplyContainsRules(std::vector<T>&, RandomContext&) {
    // No contains rules for numeric/bool types
}

void ArrayGenerator::ApplyStringContainsRules(std::vector<std::string>& values,
                                              RandomContext& ctx,
                                              size_t target_length) {
    const auto& contains_rules = config_.array_config.contains;
    if (contains_rules.empty()) {
        return;
    }

    std::unordered_set<std::string> enforced_tokens;

    for (const auto& rule : contains_rules) {
        if (!rule.include.empty() && ctx.Bernoulli(rule.probability)) {
            for (const auto& token : rule.include) {
                if (enforced_tokens.insert(token).second) {
                    if (!values.empty()) {
                        values[ctx.UniformInt(0, values.size() - 1)] = token;
                    } else {
                        values.push_back(token);
                    }
                }
            }
        }

        if (!rule.exclude.empty() && ctx.Bernoulli(rule.probability)) {
            std::unordered_set<std::string> exclusions(rule.exclude.begin(), rule.exclude.end());
            values.erase(std::remove_if(values.begin(), values.end(), [&](const std::string& value) {
                return exclusions.find(value) != exclusions.end();
            }), values.end());
        }
    }

    const size_t max_capacity = config_.array_config.max_capacity > 0
        ? static_cast<size_t>(config_.array_config.max_capacity)
        : target_length;
    if (values.size() > max_capacity) {
        values.resize(max_capacity);
    }
}

template <typename T>
void ArrayGenerator::EnsureUniqueness(std::vector<T>& values) {
    if (!config_.array_config.unique) {
        return;
    }

    std::unordered_set<T> seen;
    auto it = values.begin();
    while (it != values.end()) {
        if (!seen.insert(*it).second) {
            it = values.erase(it);
        } else {
            ++it;
        }
    }
}

void ArrayGenerator::EnsureStringUniqueness(std::vector<std::string>& values) {
    if (!config_.array_config.unique) {
        return;
    }

    std::unordered_set<std::string> seen;
    auto it = values.begin();
    while (it != values.end()) {
        if (!seen.insert(*it).second) {
            it = values.erase(it);
        } else {
            ++it;
        }
    }
}

void ArrayGenerator::ValidateElementGenerator(const FieldConfig& element_config) {
    if (element_config.generator == FieldGeneratorType::ARRAY) {
        throw std::runtime_error("Nested arrays are not supported");
    }

    switch (element_config.field_type) {
        case DataType::BOOL:
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
        case DataType::FLOAT:
        case DataType::DOUBLE:
        case DataType::VARCHAR:
            break;
        default:
            throw std::runtime_error("Unsupported array element type");
    }
}

} // namespace scalar_bench
} // namespace milvus